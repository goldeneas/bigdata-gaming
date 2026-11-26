"""
initial_load.py  (loads the historical taxis 2011-01 to 2020-12 into HBase)

What this script does (the "Python Script Initial Load Historical Data" box)
---------------------------------------------------------------------------
1. Finds the monthly taxi Parquet files for the months we picked as
   "historical" (2011-01 to 2020-12). In Kubernetes it can download them from TLC
   itself, one month at a time.
2. Reads them in chunks, so memory stays low no matter how big a file is.
3. Converts the columns with the common part (../common/shared.py), the same
   code the streaming script uses: same fields, same types, times in real UTC,
   same row keys.
4. Turns every ride into an HBase row (row key + cells) and writes the rows
   to HBase in batches through the HBase Thrift server, using `happybase`.
5. Remembers which files are done AND how far it got in a half-done file, so
   if it crashes we can just start it again and it continues where it stopped.

IMPORTANT: the streamed data (2021-01 on) gets into HBase through the Kafka
Connect HBase sink (config/kafka2hbase-connector.yaml in our repo). The rows
this script writes have to look EXACTLY like the rows the sink writes
(same tables, same column family, same column names, same row keys, same
byte encoding). Otherwise Spark sees two different formats. Most of the
choices below come from how that sink works.

Flights are not loaded anymore: the ML part only needs the live flights.

Dependencies (requirements.txt)
-------------------------------
    happybase   (HBase client, talks to the Thrift server)
    pyarrow     (reads the Parquet files, converts whole columns at once)
    numpy       (turns whole columns into bytes)

How to run it
-------------
    python initial_load.py --dry-run --limit-files 1
    python initial_load.py --limit-files 1 --limit-rows 10000
    python initial_load.py
"""

import argparse

import json

# `logging` prints messages with a timestamp and a level (INFO/WARNING/ERROR).
# Better than print() because in Kubernetes the pod logs are just stdout,
# and this way we get timestamps for free.
import logging

import os

import signal

import sys

import time

import urllib.request

from dataclasses import dataclass

from pathlib import Path

from typing import Callable, Dict, List, Optional, Tuple

import happybase                      # HBase client over Thrift
import numpy as np                    # whole columns -> bytes
import pyarrow as pa

# happybase runs on top of `thriftpy2`. When the connection to the Thrift
# server breaks, thriftpy2 raises TTransportException. We catch it so we can
# reconnect and try again instead of crashing.
from thriftpy2.transport import TTransportException

# The other error we have to retry on: HBase's own "IOError" that comes back
# through Thrift. It happens e.g. right after HBase (re)starts: the Thrift
# server already answers, but the table isn't ready yet. Despite the name it's
# NOT Python's IOError/OSError, so it needs its own import. (happybase loads
# the HBase Thrift definitions as the module "Hbase_thrift".)
from Hbase_thrift import IOError as HBaseIOError

# The common part (mappings, conversion, row keys, file listing). It's in
# ../common locally; in Kubernetes all files are in one folder, which works too
# because Python always looks in the script's own folder first.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))
import shared  # noqa: E402  (has to come after the sys.path line)

# Everything that means "HBase / Thrift isn't reachable or ready right now".
RETRYABLE_ERRORS = (TTransportException, HBaseIOError, OSError)


# CONFIGURATION
# Every setting is read from an environment variable, with a default.
# Locally the defaults are enough. In Kubernetes the Job sets them.
# TAXI_SOURCE_TZ and READ_BATCH_SIZE are in shared.py, because the streaming
# script has to use the same values.

# Locally "localhost" (with kubectl port-forward or the Docker HBase).
# Inside Kubernetes it's the Service name of the Thrift server: "hbase-thrift".
HBASE_THRIFT_HOST = os.environ.get("HBASE_THRIFT_HOST", "localhost")

# Careful: Kubernetes (and Docker links) set HBASE_THRIFT_PORT by themselves
# to something like "tcp://10.1.2.3:9090" when there's a Service / container
# called hbase-thrift. The Job turns that off (enableServiceLinks: false);
# if it still happens we stop with a clear message instead of a weird crash.
_thrift_port = os.environ.get("HBASE_THRIFT_PORT", "9090")
if not _thrift_port.isdigit():
    sys.exit(f"HBASE_THRIFT_PORT has to be a number, got {_thrift_port!r}. Kubernetes sets it "
             f"itself for a Service called hbase-thrift: set it to 9090 in the Job's env.")
HBASE_THRIFT_PORT = int(_thrift_port)

# These two have to match how the Thrift server was started. If they don't,
# the connection opens fine but every call fails or hangs.
#   transport: "buffered" (default) or "framed"
#   protocol:  "binary"  (default) or "compact"
# The cluster's hbase-thrift runs with the defaults (checked in config/HBase/hbase.yaml).
HBASE_TRANSPORT = os.environ.get("HBASE_TRANSPORT", "buffered")
HBASE_PROTOCOL = os.environ.get("HBASE_PROTOCOL", "binary")

# Our HBase sink has `table.name.format: ${topic}` and topics `taxis,flights`.
# So the sink writes into HBase tables called "taxis" and "flights".
# This script has to use the same names, otherwise the historical and the
# streamed data end up in different tables.
TAXI_TABLE = os.environ.get("TAXI_TABLE", "taxis")

# Where the monthly Parquet files are (or get downloaded to).
TAXI_DATA_DIR = Path(os.environ.get("TAXI_DATA_DIR", "./data/taxi"))

# Every month from TAXI_FIRST_MONTH to TAXI_LAST_MONTH (both included) is
# loaded by THIS script, the rest goes through the streaming script.
# Team decision: 10 years of historical data through this script
# (2011-01 to 2020-12) and 5 years of live streaming from 2021-01 on.
# (Before that it was until 2022-10, when we only had flight data from 2022-11.)
TAXI_FIRST_MONTH = os.environ.get("TAXI_FIRST_MONTH", "2011-01")
TAXI_LAST_MONTH = os.environ.get("TAXI_LAST_MONTH", "2020-12")

# TAXI_DOWNLOAD=1: download every month of the range from TLC into
# TAXI_DATA_DIR, one at a time, and delete it again after it's loaded. So the
# pod only needs disk for one month. The Kubernetes Job uses this.
# A month that's already in TAXI_DATA_DIR isn't downloaded again.
TAXI_DOWNLOAD = os.environ.get("TAXI_DOWNLOAD", "0") == "1"
TAXI_DOWNLOAD_URL = os.environ.get(
    "TAXI_DOWNLOAD_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data/{name}")

# WRITE_BATCH_SIZE: how many rows we send to HBase in one Thrift call.
#   Too small and we wait on the network all the time (slow).
#   Too big and the requests get huge, which can cause timeouts or eat the
#   Thrift server's memory (it has 1 GB on the cluster). 1k-10k is the usual range.
WRITE_BATCH_SIZE = int(os.environ.get("WRITE_BATCH_SIZE", "5000"))

# HBASE_WRITE_WAL=0 writes without HBase's write-ahead log. It's faster and
# needs a lot less temporary disk, but if HBase crashes, the last writes are
# lost WITHOUT an error. We can only risk that because the load can be rerun
# (same keys) and we check the counts afterwards. Default: with WAL.
HBASE_WRITE_WAL = os.environ.get("HBASE_WRITE_WAL", "1") == "1"

# A small JSON file with the files we already loaded AND the row to continue
# from in a half-done file. A restarted run reads it and skips / continues.
# IMPORTANT for Kubernetes: a pod's own disk is wiped when the pod restarts,
# so this file has to be on a mounted volume (PersistentVolume). If it's not,
# the "resume" part silently does nothing.
PROGRESS_FILE = Path(os.environ.get("PROGRESS_FILE", "./initial_load_progress.json"))

# If the connection to Thrift breaks while we load a file, we reconnect and
# continue THAT file from the last batch HBase confirmed. Rewriting a few rows
# is safe because the row keys are always the same for the same ride, so HBase
# just overwrites the cells instead of making duplicates.
# MAX_RETRIES now counts failures IN A ROW without any progress in between.
# As soon as a batch goes through again, the counter starts over. So a long
# load that hits a stall every few million rows keeps going, and we only give
# up when HBase is really gone. The wait grows each time: 15 s, 30 s, 45 s...
# (6 failures in a row = about 4 minutes of waiting, plus the time-outs.)
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "6"))
RETRY_WAIT_SECONDS = int(os.environ.get("RETRY_WAIT_SECONDS", "15"))

# How long ONE call to HBase (one batch) may take before we call it failed.
# Was 60 s, which was too short: when HBase has too much to clean up
# (flushes / compactions), it blocks ALL writes, by default for up to 90 s
# (hbase.hstore.blockingWaitTime). So a busy-but-fine HBase looked dead.
# 5 minutes is well above that and still catches a really dead server.
HBASE_TIMEOUT_SECONDS = int(os.environ.get("HBASE_TIMEOUT_SECONDS", "300"))

# Print a "still working" line every this many rows (and save the position
# in the progress file). A month is up to ~15 million rows (2011-2015) and
# can take half an hour, and without this the script prints nothing until the
# file is done, so it looks stuck.
PROGRESS_EVERY_ROWS = int(os.environ.get("PROGRESS_EVERY_ROWS", "500000"))


# HBASE LAYOUT
# Flat Avro in the streaming script -> the sink puts every field into ONE
# column family named after the topic ("taxis"), column name = field name.
TAXI_FAMILY = os.environ.get("TAXI_FAMILY", TAXI_TABLE)

# Only used if THIS script has to create the table. Better: create it in the
# HBase shell first, because happybase can't set FAST_DIFF:
#   create 'taxis', {NAME => 'taxis', VERSIONS => 1, DATA_BLOCK_ENCODING => 'FAST_DIFF', COMPRESSION => 'GZ'}
# Tested: FAST_DIFF + GZ made a full month 790 MB instead of ~19 GB.
# max_versions=1: HBase only keeps the latest value of each cell.
TAXI_COLUMN_FAMILIES = {TAXI_FAMILY: dict(max_versions=1, compression="GZ")}

# field -> type, and field -> the HBase column name as bytes ("taxis:fare")
TAXI_FIELDS = shared.fields(shared.TAXI_COLUMN_MAPPING)
TAXI_QUALIFIERS = {field: f"{TAXI_FAMILY}:{field}".encode("utf-8") for field in TAXI_FIELDS}


# LOGGING SETUP

log = logging.getLogger("initial_load")


def setup_logging() -> None:
    """Log lines look like: 2026-11-21 14:10:03 INFO  message"""
    logging.basicConfig(
        level=logging.INFO,                     # show INFO and above
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,                      # Kubernetes collects stdout
    )


# VALUE ENCODING  (converted columns to bytes for HBase)
# HBase only stores bytes, so every value has to be converted.
#
# We HAVE to convert them the same way the Kafka Connect HBase sink does,
# otherwise a fare of 12.5 from 2014 and a fare of 12.5 from 2023 end up as
# different bytes and Spark can't read both with the same code.
# The sink uses Java's HBase `Bytes.toBytes(...)`, which means:
#   string    -> the text as UTF-8 bytes
#   int       -> 4 bytes, big-endian         (numpy ">i4")
#   double    -> 8 bytes IEEE double         (numpy ">f8")
#   timestamp -> milliseconds since 1970 as a long, 8 bytes (numpy ">i8")
# ">" means big-endian, which is what Java uses.
# numpy turns a whole column into bytes at once and we only cut them into one
# piece per row. Same bytes as struct.pack per value, just a lot faster.
#
# Downside: in the HBase shell the numbers show up as unreadable bytes like
# \x00\x00\x00\x8E instead of "142". That's normal.

BYTE_FORMATS = {"int": ">i4", "double": ">f8", "timestamp": ">i8", "unix_seconds": ">i8"}


def encode_column(column, value_type: str) -> List[Optional[bytes]]:
    """One converted column -> one bytes value per row, None where the value is
    missing (= no cell: HBase is sparse, and the sink deletes a cell when it
    gets a null, so leaving it out gives the same end result)."""
    if isinstance(column, pa.ChunkedArray):
        column = column.combine_chunks()
    if value_type == "string":
        return [None if value is None else value.encode("utf-8") for value in column.to_pylist()]
    dtype = np.dtype(BYTE_FORMATS[value_type])
    raw = column.fill_null(0).to_numpy(zero_copy_only=False).astype(dtype).tobytes()
    valid = column.is_valid().to_numpy(zero_copy_only=False)
    width = dtype.itemsize
    return [raw[i * width:(i + 1) * width] if valid[i] else None for i in range(len(column))]


def build_rows(batch, source_file: str, first_row_number: int) -> List[Tuple[int, bytes, Dict[bytes, bytes]]]:
    """One batch from a file -> [(row number, row key, cells), ...].
    cells is the dict happybase wants: {b"taxis:fare": b"...", ...}"""
    table = shared.convert_batch(batch, shared.TAXI_COLUMN_MAPPING)
    keys = shared.taxi_row_keys(table.column("pickup_ts"), source_file, first_row_number)
    encoded = {TAXI_QUALIFIERS[field]: encode_column(table.column(field), value_type)
               for field, value_type in TAXI_FIELDS.items()}
    rows = []
    for i, key in enumerate(keys):
        cells = {qualifier: values[i] for qualifier, values in encoded.items()
                 if values[i] is not None}
        if cells:          # every value missing = nothing to store
            rows.append((first_row_number + i, key.encode("utf-8"), cells))
    return rows


# PROGRESS TRACKING

class Progress:
    """Keeps the finished file names AND the row to continue from in a
    half-done file, and saves them to a JSON file:
        {"done": ["taxi/yellow_tripdata_2011-01.parquet", ...],
         "partial": {"taxi/yellow_tripdata_2011-02.parquet": 6395000}}
    The old format (just a list of finished files) is still read."""

    def __init__(self, path: Path):
        self.path = path
        self.done = set()
        self.partial: Dict[str, int] = {}
        if path.exists():
            data = json.loads(path.read_text())
            if isinstance(data, list):
                data = {"done": data}
            self.done = set(data.get("done", []))
            self.partial = {key: int(row) for key, row in data.get("partial", {}).items()}
            log.info("Resuming: %d files already loaded, %d half done",
                     len(self.done), len(self.partial))

    def is_done(self, key: str) -> bool:
        return key in self.done

    def partial_row(self, key: str) -> int:
        return self.partial.get(key, 0)

    def mark_partial(self, key: str, next_row: int) -> None:
        self.partial[key] = next_row
        self._save()

    def mark_done(self, key: str) -> None:
        self.done.add(key)
        self.partial.pop(key, None)
        self._save()

    def _save(self) -> None:
        # Write to a temp file first, then rename it over the real one.
        # A rename happens all at once, so if the pod gets killed while
        # writing, we still have the old complete file and not a broken one.
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"done": sorted(self.done), "partial": self.partial}))
        tmp.replace(self.path)


# HBASE CONNECTION + TABLE CREATION

def connect() -> happybase.Connection:
    """Connect to the HBase Thrift server, with a few retries.
    The retries matter in Kubernetes: our Job can start before the Thrift
    server pod is ready."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            connection = happybase.Connection(
                host=HBASE_THRIFT_HOST,
                port=HBASE_THRIFT_PORT,
                transport=HBASE_TRANSPORT,
                protocol=HBASE_PROTOCOL,
                # Timeout in milliseconds for each Thrift call. Without it a
                # dead server can make the script hang forever.
                timeout=HBASE_TIMEOUT_SECONDS * 1000,
            )
            # A cheap call to check the server really answers
            # (creating the Connection alone doesn't prove that).
            connection.tables()
            log.info("Connected to HBase Thrift at %s:%d", HBASE_THRIFT_HOST, HBASE_THRIFT_PORT)
            return connection
        except RETRYABLE_ERRORS as error:
            log.warning("Connection attempt %d/%d failed: %s", attempt, MAX_RETRIES, error)
            time.sleep(RETRY_WAIT_SECONDS)
    log.error("Could not connect to HBase. Giving up.")
    sys.exit(1)   # exit code 1 = Kubernetes marks the Job attempt as failed


def ensure_table(connection: happybase.Connection) -> None:
    """Create the table if it doesn't exist yet (only GZ, see TAXI_COLUMN_FAMILIES).
    The sink also has auto.create.tables: true, so whoever runs first creates
    it. Both create it with the same name and family, so it doesn't matter
    which one, but only the shell command gives it FAST_DIFF."""
    # connection.tables() gives the names as BYTES, so we compare with bytes.
    if TAXI_TABLE.encode("utf-8") in connection.tables():
        log.info("Table %s already exists", TAXI_TABLE)
        return
    log.warning("Table %s doesn't exist, creating it WITHOUT FAST_DIFF (only GZ). "
                "Better: create it in the HBase shell first (see the README).", TAXI_TABLE)
    connection.create_table(TAXI_TABLE, TAXI_COLUMN_FAMILIES)


# FINDING / DOWNLOADING THE FILES

def files_to_load() -> List[Path]:
    """The monthly files of the range, in order (2011-01 first).
    With TAXI_DOWNLOAD=1 these are the paths the months WILL be downloaded to."""
    if TAXI_DOWNLOAD:
        return [TAXI_DATA_DIR / shared.taxi_file_name(year, month)
                for year, month in shared.months_between(TAXI_FIRST_MONTH, TAXI_LAST_MONTH)]
    return shared.list_taxi_files(TAXI_DATA_DIR, TAXI_FIRST_MONTH, TAXI_LAST_MONTH)


def download(path: Path) -> None:
    """Download one month from TLC to `path`, unless it's already there.
    Into a temp file first and renamed when complete, so a stopped download
    never looks like a finished file."""
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    url = TAXI_DOWNLOAD_URL.format(name=path.name)
    tmp = path.with_name(path.name + ".part")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("Downloading %s", url)
            started = time.time()
            with urllib.request.urlopen(url, timeout=120) as response, tmp.open("wb") as out:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
            tmp.replace(path)
            log.info("Downloaded %s (%.0f MB in %.0f s)", path.name,
                     path.stat().st_size / 1e6, time.time() - started)
            return
        except OSError as error:   # includes HTTP errors and timeouts
            log.warning("Download attempt %d/%d failed: %s", attempt, MAX_RETRIES, error)
            time.sleep(RETRY_WAIT_SECONDS * attempt)
    log.error("Could not download %s. Giving up.", url)
    sys.exit(1)


# LOADING ONE FILE

@dataclass
class FileProgress:
    """How far we got in ONE file. It survives a failed attempt, so the retry
    can continue from here instead of starting the file over."""
    next_row: int = 0          # first row number NOT confirmed by HBase yet
    rows_written: int = 0      # rows HBase confirmed so far (all attempts)
    started: float = 0.0       # when we started this file (for rows/s)
    # called every PROGRESS_EVERY_ROWS rows to save next_row in the progress file
    save_position: Optional[Callable[[int], None]] = None


def check_file(path: Path) -> None:
    """Stop if the file has another layout; warn about columns we'd drop."""
    not_converted = shared.check_columns(path.name, shared.read_taxi_columns(path),
                                         shared.TAXI_COLUMN_MAPPING, shared.TAXI_REQUIRED_COLUMNS)
    if not_converted:
        # We decided the initial load doesn't drop anything, so if this shows
        # up, the column has to be added to the mapping in shared.py.
        log.warning("%s has columns we DON'T load: %s", path.name, not_converted)


def load_file(table: Optional[happybase.Table], path: Path, dry_run: bool, dry_run_rows: int,
              limit_rows: Optional[int], progress: FileProgress) -> int:
    """Read one file and write its rows to HBase, starting at
    progress.next_row. Returns how many rows were written in total."""
    # path.stem = file name without .parquet, e.g. "yellow_tripdata_2014-03"
    source_file = path.stem

    if dry_run:
        # DRY RUN: no HBase at all. Prints the first few rows exactly how they
        # WOULD be written. Just for me to test the script is working.
        batch = next(shared.iter_taxi_batches(path)).slice(0, dry_run_rows)
        for _, key, cells in build_rows(batch, source_file, 0):
            log.info("DRY RUN %s -> %s", key, cells)
        return 0

    # We fill our own list of rows and send it ourselves when it's full,
    # instead of letting happybase send automatically. That way we know
    # EXACTLY which rows HBase confirmed: only after send() returns without
    # an error, we move progress.next_row forward. If a send fails, nothing
    # moves, and the retry continues from the first row of that batch.
    # (Sending is one Thrift call per batch.)
    pending = []      # (row_number, key, cells) not sent yet

    def send_pending():
        batch = table.batch(wal=HBASE_WRITE_WAL)   # no batch_size: only sends on send()
        for _, key, cells in pending:
            batch.put(key, cells)
        batch.send()                   # raises if HBase doesn't confirm
        # Only here we know HBase has these rows.
        before = progress.rows_written
        progress.rows_written += len(pending)
        progress.next_row = pending[-1][0] + 1
        pending.clear()
        # "Still working" line, so a long file doesn't look stuck.
        if progress.rows_written // PROGRESS_EVERY_ROWS > before // PROGRESS_EVERY_ROWS:
            elapsed_so_far = time.time() - progress.started
            log.info("%s: %d rows so far (%.0f rows/s)", path.name, progress.rows_written,
                     progress.rows_written / max(elapsed_so_far, 1e-9))
            if progress.save_position is not None:
                progress.save_position(progress.next_row)

    if progress.next_row > 0:
        log.info("%s: continuing from row %d", path.name, progress.next_row)

    # first_row = row number (in the whole file) of the first ride in the
    # batch. It's the tie-breaker in the row key, so it has to be counted over
    # the WHOLE file, also for the batches we skip.
    first_row = 0
    for batch in shared.iter_taxi_batches(path):
        end = first_row + batch.num_rows
        # --limit-rows: stop early, only for testing.
        if limit_rows is not None:
            batch = batch.slice(0, max(0, min(batch.num_rows, limit_rows - first_row)))
        # Rows HBase already confirmed in an earlier attempt: skip them
        # without converting (reading is fast compared to writing).
        skip = max(0, progress.next_row - first_row)
        if skip < batch.num_rows:
            for row in build_rows(batch.slice(skip), source_file, first_row + skip):
                pending.append(row)
                if len(pending) >= WRITE_BATCH_SIZE:
                    send_pending()
        first_row = end
        if limit_rows is not None and first_row >= limit_rows:
            break
    # The last, half-full batch.
    if pending:
        send_pending()

    elapsed = time.time() - progress.started
    # max(..., 1e-9) so we never divide by zero on a tiny/empty file.
    log.info("%s: %d rows in %.1fs (%.0f rows/s)", path.name, progress.rows_written,
             elapsed, progress.rows_written / max(elapsed, 1e-9))
    return progress.rows_written


def load_file_with_retries(connection, path, dry_run, dry_run_rows, limit_rows,
                           start_row: int = 0,
                           save_position: Optional[Callable[[int], None]] = None) -> int:
    """Wraps load_file: if HBase / Thrift fails, wait, reconnect and CONTINUE
    the file from the last batch HBase confirmed. Rewriting a few rows of the
    failed batch is safe, because the keys never change (it just overwrites).
    We only give up after MAX_RETRIES failures in a row with no progress.
    When we stop (giving up or Ctrl-C), the position is saved, so the next
    run continues from there too."""
    progress = FileProgress(next_row=start_row, started=time.time(),
                            save_position=save_position)
    failures_in_a_row = 0
    try:
        while True:
            row_before = progress.next_row
            try:
                table = None if dry_run else connection.table(TAXI_TABLE)
                return load_file(table, path, dry_run, dry_run_rows, limit_rows, progress)
            except RETRYABLE_ERRORS as error:
                # Did this attempt get anything through before failing?
                if progress.next_row > row_before:
                    failures_in_a_row = 1      # yes: it's a new problem, count from 1
                else:
                    failures_in_a_row += 1     # no: same problem still going on
                if failures_in_a_row >= MAX_RETRIES:
                    break
                wait = RETRY_WAIT_SECONDS * failures_in_a_row
                log.warning("%s failed after %d confirmed rows (failure %d/%d in a row): %s. "
                            "Retrying in %d s", path.name, progress.rows_written,
                            failures_in_a_row, MAX_RETRIES, error, wait)
                time.sleep(wait)
                # close() + open() makes a new connection to the Thrift server.
                # If the server is still down, open() fails too; we catch that
                # and try again on the next loop.
                try:
                    connection.close()
                    connection.open()
                except RETRYABLE_ERRORS as reconnect_error:
                    log.warning("Reconnect failed: %s", reconnect_error)
    except KeyboardInterrupt:
        # Ctrl-C (or Kubernetes stopping the pod): remember where we were.
        if save_position is not None and progress.next_row > 0:
            save_position(progress.next_row)
            log.info("Stopped. Saved position: %s continues from row %d next time",
                     path.name, progress.next_row)
        raise
    if save_position is not None and progress.next_row > 0:
        save_position(progress.next_row)
    log.error("Giving up on %s after %d failures in a row (%d rows were confirmed, "
              "the next run continues from row %d)", path.name, MAX_RETRIES,
              progress.rows_written, progress.next_row)
    sys.exit(1)


# COMMAND-LINE ARGUMENTS

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initial load of the historical taxis into HBase")
    # action="store_true" = a flag without a value, True if it's there.
    parser.add_argument("--dry-run", action="store_true",
                        help="don't touch HBase, just print what would be written")
    parser.add_argument("--dry-run-rows", type=int, default=5,
                        help="rows to print per file in dry-run mode")
    parser.add_argument("--limit-files", type=int, default=None,
                        help="only process the first N files (for testing)")
    parser.add_argument("--limit-rows", type=int, default=None,
                        help="only write the first N rows of each file (for testing). "
                             "Files loaded like this are NOT marked as done.")
    return parser.parse_args()


# MAIN

def stop_on_sigterm(signum, frame):
    """Kubernetes stops a pod with SIGTERM instead of Ctrl-C. We turn it into a
    KeyboardInterrupt too, so the position gets saved before the pod dies
    (it gets 30 s by default, saving takes milliseconds)."""
    raise KeyboardInterrupt


def main() -> None:
    setup_logging()
    args = parse_args()
    signal.signal(signal.SIGTERM, stop_on_sigterm)
    # Fails right away if this machine can't convert New York time to UTC.
    shared.check_timezone_support()
    log.info("Taxi months %s to %s, times converted from %s to UTC, table %s",
             TAXI_FIRST_MONTH, TAXI_LAST_MONTH, shared.TAXI_SOURCE_TZ, TAXI_TABLE)

    # In dry-run we never connect, so we dont need HBase / Kubernetes
    connection = None if args.dry_run else connect()
    progress = Progress(PROGRESS_FILE)
    if connection is not None:
        ensure_table(connection)

    files = files_to_load()
    if args.limit_files is not None:
        files = files[:args.limit_files]
    log.info("[taxi] %d files to process", len(files))

    total_rows = 0
    total_started = time.time()
    for path in files:
        # "taxi/" in front, like before, so older progress files still work.
        progress_key = f"taxi/{path.name}"
        if not args.dry_run and progress.is_done(progress_key):
            log.info("Skipping %s (already loaded)", path.name)
            continue

        if TAXI_DOWNLOAD:
            download(path)
        check_file(path)

        # Test runs (--limit-rows, dry run) don't save positions or mark anything done.
        real_run = not args.dry_run and args.limit_rows is None
        start_row = progress.partial_row(progress_key) if real_run else 0
        save_position = ((lambda row, key=progress_key: progress.mark_partial(key, row))
                         if real_run else None)
        total_rows += load_file_with_retries(
            connection, path, args.dry_run, args.dry_run_rows, args.limit_rows,
            start_row=start_row, save_position=save_position)

        # Only mark a file as done AFTER all of it was written.
        if real_run:
            progress.mark_done(progress_key)
        # Downloaded months are deleted again, so the pod only needs disk for one.
        if TAXI_DOWNLOAD and real_run:
            path.unlink()

    if connection is not None:
        connection.close()

    log.info("Finished: %d rows in %.1f minutes",
             total_rows, (time.time() - total_started) / 60)


if __name__ == "__main__":
    main()
