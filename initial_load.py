"""
initial_load.py  (PROTOTYPE, not tested on our cluster yet)

What this script does (the "Python Script Initial Load Historical Data" box)
---------------------------------------------------------------------------
1. Finds the historical files (taxi Parquet + flights CSV) for the years we
   picked as "historical" (e.g. 2009-2015).
2. Reads them in small chunks, so memory stays low no matter how big a file is.
3. Turns every ride / flight into an HBase row:
      row key + a few cells "column_family:column" = value (bytes)
4. Writes the rows to HBase in batches through the HBase Thrift server,
   using the `happybase` library.
5. Remembers which files are done, so if it crashes we can just start it
   again and it skips what's already loaded.

IMPORTANT: the streamed data (2016+) gets into HBase through the Kafka
Connect HBase sink (config/kafka2hbase-connector.yaml in our repo). The rows
this script writes have to look EXACTLY like the rows the sink writes
(same tables, same column family, same column names, same row keys, same
byte encoding). Otherwise Spark sees two different formats. Most of the
choices below come from how that sink works.

How to read this file
---------------------
- Everything we haven't decided yet is marked with:
      # >>> PLACEHOLDER: ...
  Search for ">>> PLACEHOLDER" to see all the open decisions.
- Everything else is explained in the comment above it.

Dependencies (requirements.txt)
-------------------------------
    happybase   (HBase client, talks to the Thrift server)
    pyarrow     (reads the taxi Parquet files)
    pandas      (reads the flights CSV in chunks)

How to run it
-------------
    python initial_load.py --dataset taxi --dry-run --limit-files 1
    python initial_load.py --dataset taxi --limit-files 1 --limit-rows 10000
    python initial_load.py --dataset all
"""

import argparse

import json

# `logging` prints messages with a timestamp and a level (INFO/WARNING/ERROR).
# Better than print() because in Kubernetes the pod logs are just stdout,
# and this way we get timestamps for free.
import logging

import os

import re

# `struct` turns numbers into raw bytes (e.g. a double into 8 bytes).
# We need it because the Kafka Connect HBase sink stores numbers as raw
# bytes, not as text, and we have to do the same.
import struct

import sys

import time

# `zlib.crc32` gives us a hash of a string that is always the same.
# We use it for the optional "salt" in the row key.
# We DON'T use Python's hash(), because hash() of a string changes every
# time Python starts. Then the same ride would get a different row key on
# a rerun, and we'd get duplicates instead of overwriting.
import zlib

from dataclasses import dataclass

from pathlib import Path

from typing import Callable, Dict, Iterator, List, Optional, Tuple

import happybase                      # HBase client over Thrift
import pandas as pd                   # CSV reading in chunks + timestamps
import pyarrow.parquet as pq          # Parquet reading in batches

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

# Everything that means "HBase / Thrift isn't reachable or ready right now".
RETRYABLE_ERRORS = (TTransportException, HBaseIOError, OSError)


# CONFIGURATION
# Every setting is read from an environment variable, with a default.
# Locally the defaults are enough. In Kubernetes a ConfigMap overrides them.

# >>> PLACEHOLDER: real host. Inside Kubernetes this will be the Service name
#     of the Thrift server, e.g. "hbase-thrift". Right now there's no HBase or
#     Thrift server in the repo yet (only ZooKeeper), so we have to add it.
HBASE_THRIFT_HOST = os.environ.get("HBASE_THRIFT_HOST", "localhost")

HBASE_THRIFT_PORT = int(os.environ.get("HBASE_THRIFT_PORT", "9090"))

# These two have to match how the Thrift server was started. If they don't,
# the connection opens fine but every call fails or hangs.
#   transport: "buffered" (default) or "framed"
#   protocol:  "binary"  (default) or "compact"
# >>> PLACEHOLDER: confirm this when we start the Thrift server.
HBASE_TRANSPORT = os.environ.get("HBASE_TRANSPORT", "buffered")
HBASE_PROTOCOL = os.environ.get("HBASE_PROTOCOL", "binary")

# Our HBase sink has `table.name.format: ${topic}` and topics `taxis,flights`.
# So the sink writes into HBase tables called "taxis" and "flights".
# This script has to use the same names, otherwise the historical and the
# streamed data end up in different tables.
TAXI_TABLE = os.environ.get("TAXI_TABLE", "taxis")
FLIGHTS_TABLE = os.environ.get("FLIGHTS_TABLE", "flights")

# >>> PLACEHOLDER: depends on how the pod gets the data (PersistentVolume
#     mounted at some path, HDFS, object storage...).
TAXI_DATA_DIR = Path(os.environ.get("TAXI_DATA_DIR", "./data/taxi"))
FLIGHTS_DATA_DIR = Path(os.environ.get("FLIGHTS_DATA_DIR", "./data/flights"))

# Everything with FIRST_YEAR <= year <= LAST_YEAR is loaded by THIS script.
# Later years are left for the other script.
# >>> PLACEHOLDER: final split
HISTORICAL_FIRST_YEAR = int(os.environ.get("HISTORICAL_FIRST_YEAR", "2009"))
HISTORICAL_LAST_YEAR = int(os.environ.get("HISTORICAL_LAST_YEAR", "2015"))

# Flights get their own year range, because the flight data we have right now
# (OpenSky arrivals at JFK) is ONLY 2025. With the taxi range 2009-2015 not a
# single flight file would be loaded. By default it's the same range as taxi.
# >>> PLACEHOLDER: decide the flights split together with the team (and which
#     years of flights we download at all).
FLIGHTS_FIRST_YEAR = int(os.environ.get("FLIGHTS_FIRST_YEAR", str(HISTORICAL_FIRST_YEAR)))
FLIGHTS_LAST_YEAR = int(os.environ.get("FLIGHTS_LAST_YEAR", str(HISTORICAL_LAST_YEAR)))

# READ_BATCH_SIZE: how many rows we read from a file at once. Bigger means
#   fewer reads but more memory. 50k taxi rows is a few tens of MB.
# WRITE_BATCH_SIZE: how many rows we send to HBase in one Thrift call.
#   Too small and we wait on the network all the time (slow).
#   Too big and the requests get huge, which can cause timeouts or eat the
#   Thrift server's memory. 1k-10k is the usual range.
# >>> PLACEHOLDER: both need tuning after testing on one month of data.
READ_BATCH_SIZE = int(os.environ.get("READ_BATCH_SIZE", "50000"))
WRITE_BATCH_SIZE = int(os.environ.get("WRITE_BATCH_SIZE", "5000"))

# Row key salting: only worth it if HBase runs with more than one RegionServer.
# >>> PLACEHOLDER: depends on how many RegionServers we get.
NUM_SALT_BUCKETS = int(os.environ.get("NUM_SALT_BUCKETS", "0"))

# The taxi timestamps have no timezone. They're just New York wall clock
# time ("2014-03-01 08:15:00"). To store them as a number (milliseconds since
# 1970, which is what the HBase sink does with timestamps) we have to pick a
# timezone to interpret them in:
#   "UTC"              -> we pretend the wall clock time is UTC. The number
#                         then turns back into the exact same "08:15" when
#                         read as UTC. Simple, no daylight saving problems.
#   "America/New_York" -> the "real" moment in time. More correct, but
#                         Spark then has to show it in New York time again,
#                         and daylight saving changes make some hours weird.
# The "Simulate live streaming" script MUST use the same choice.
# >>> PLACEHOLDER: decide together with the streaming script + Spark people.
NAIVE_TIMESTAMP_TZ = os.environ.get("NAIVE_TIMESTAMP_TZ", "UTC")

# A small JSON file with the list of files we already loaded. A restarted
# run reads it and skips those files.
# IMPORTANT for Kubernetes: a pod's own disk is wiped when the pod restarts,
# so this file has to be on a mounted volume (PersistentVolume). If it's not,
# the "resume" part silently does nothing.
# >>> PLACEHOLDER: path on the mounted volume.
PROGRESS_FILE = Path(os.environ.get("PROGRESS_FILE", "./initial_load_progress.json"))

# If the connection to Thrift breaks while we load a file, we reconnect and
# load THAT file again from the start, up to MAX_RETRIES times. That's safe
# because the row keys are always the same for the same ride, so HBase just
# overwrites the cells instead of making duplicates.
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

# Print a "still working" line every this many rows. A full month is 12-15
# million rows and takes over half an hour, and without this the script
# prints nothing until the file is done, so it looks stuck.
PROGRESS_EVERY_ROWS = int(os.environ.get("PROGRESS_EVERY_ROWS", "500000"))


# SCHEMA DECISIONS: WHICH COLUMNS GO WHERE IN HBASE!!!
# HBase basics we need here:
#   - A table has a few COLUMN FAMILIES, fixed when the table is created.
#     Inside a family a row can have any columns we want.
#   - A cell is "row key + family:column" and holds bytes.
#   - HBase is SPARSE: a missing value just isn't stored (no NULLs).
#
# How the Kafka Connect HBase sink decides family and column:
#   - If the Avro value is "flat" (just fields, no nested records), every
#     field goes into ONE column family named after the topic ("taxis"),
#     and the column name is the field name.
#   - If the Avro value is nested ({"t": {...}, "f": {...}}), the outer
#     names become column families and the inner names become columns.
# I went with FLAT for now because it keeps the Avro schema of the streaming
# script simple. Downside: the family name "taxis" is stored with every cell,
# so it takes a bit more disk than a 1 letter family would.
# >>> PLACEHOLDER: flat vs nested. If we switch to nested, the family names
#     here must match the outer record names in the Avro schema.
TAXI_FAMILY = os.environ.get("TAXI_FAMILY", TAXI_TABLE)
FLIGHTS_FAMILY = os.environ.get("FLIGHTS_FAMILY", FLIGHTS_TABLE)

# max_versions=1: HBase only keeps the latest value of each cell. We never
# need the old ones and it saves space.
# Other options exist per family (e.g. compression="SNAPPY") but that's to decide.
TAXI_COLUMN_FAMILIES = {TAXI_FAMILY: dict(max_versions=1)}
FLIGHTS_COLUMN_FAMILIES = {FLIGHTS_FAMILY: dict(max_versions=1)}

# The mapping is:   column in the file -> (column name in HBase, type)
# Only the columns listed here are written. We decided the initial load
# doesn't drop anything, so EVERY column of a file has to be in its mapping.
# If one is missing, the script warns ("columns we DON'T load").
#
# The column name in HBase = the field name in the Avro schema of the
# streaming script. The type = the Avro type of that field:
#   "string", "int", "long", "float", "double", "boolean", "timestamp"
# ("timestamp" = an Avro long with logicalType timestamp-millis).
# The type decides how the value is turned into bytes (see encode_value),
# so a wrong type here = rows that don't match the streamed ones.
#
# >>> PLACEHOLDER: final list of taxi columns + types (must match the Avro schema).
#     CHECKED: 2015-01 and 2023-01 have the same column names, and 2015
#     already has PULocationID/DOLocationID. 2009-01 has a completely
#     different layout, it has its own mapping below (TAXI_COLUMN_MAPPING_2009).
#     2010-01 has a third layout (TAXI_COLUMN_MAPPING_2010).
#     ALL months 2009-2015 checked: every 2009 file has the 2009 layout,
#     every 2010 file the 2010 layout, and 2011-2015 all have this layout.
#     Types differ a bit between years (e.g. passenger_count is int64 in
#     2015 and double in 2023 because it has nulls there). encode_value()
#     handles that, the type here is what we WANT to store.
#     int vs long: the files have int64. We store "int" (4 bytes) because
#     the values are small (zones go up to 265), but then the Avro schema
#     of the streaming script must say "int" too, not "long".

TAXI_COLUMN_MAPPING: Dict[str, Tuple[str, str]] = {
    "VendorID":              ("vendor_id", "int"),
    "tpep_pickup_datetime":  ("pickup_ts", "timestamp"),
    "tpep_dropoff_datetime": ("dropoff_ts", "timestamp"),
    "passenger_count":       ("passengers", "int"),
    "trip_distance":         ("distance", "double"),
    "RatecodeID":            ("rate_code", "int"),
    "store_and_fwd_flag":    ("store_and_fwd_flag", "string"),
    "PULocationID":          ("pu_zone", "int"),
    "DOLocationID":          ("do_zone", "int"),
    "payment_type":          ("payment_type", "int"),
    "fare_amount":           ("fare", "double"),
    "extra":                 ("extra", "double"),
    "mta_tax":               ("mta_tax", "double"),
    "tip_amount":            ("tip", "double"),
    "tolls_amount":          ("tolls", "double"),
    "improvement_surcharge": ("improvement_surcharge", "double"),
    "total_amount":          ("total", "double"),
    "congestion_surcharge":  ("congestion_surcharge", "double"),
    "airport_fee":           ("airport_fee", "double"),
}

# The 2009 files have a different layout (checked on 2009-01): other column
# names, coordinates instead of zones, and the times are text.
# Same meaning = same HBase column as above, so Spark reads them the same way
# (the times are parsed into real timestamps, that's the "align types" part).
# Different meaning or different kind of value = its own column, stored as it is:
#   - Start/End Lon/Lat: coordinates, the zones get computed later in Spark
#   - vendor_name ("VTS", "CMT"...) is text, VendorID is a number code
#   - Payment_Type ("CASH", "Credit"...) is text, payment_type is a number code
#   - store_and_forward is a number here, store_and_fwd_flag is "Y"/"N"
#   - surcharge is probably the same thing as `extra`, but we're not sure,
#     so it gets its own column and Spark can merge them if it is
# Turning these into the newer codes would be preprocessing, so we don't.
TAXI_COLUMN_MAPPING_2009: Dict[str, Tuple[str, str]] = {
    "vendor_name":           ("vendor_name", "string"),
    "Trip_Pickup_DateTime":  ("pickup_ts", "timestamp"),
    "Trip_Dropoff_DateTime": ("dropoff_ts", "timestamp"),
    "Passenger_Count":       ("passengers", "int"),
    "Trip_Distance":         ("distance", "double"),
    "Start_Lon":             ("pu_lon", "double"),
    "Start_Lat":             ("pu_lat", "double"),
    "Rate_Code":             ("rate_code", "int"),
    "store_and_forward":     ("store_and_forward", "double"),
    "End_Lon":               ("do_lon", "double"),
    "End_Lat":               ("do_lat", "double"),
    "Payment_Type":          ("payment_type_text", "string"),
    "Fare_Amt":              ("fare", "double"),
    "surcharge":             ("surcharge", "double"),
    "mta_tax":               ("mta_tax", "double"),
    "Tip_Amt":               ("tip", "double"),
    "Tolls_Amt":             ("tolls", "double"),
    "Total_Amt":             ("total", "double"),
}

# The 2010 files have a third layout (checked on 2010-01): lowercase names,
# coordinates like 2009, times as text like 2009, and some columns are text
# where 2011+ has numbers. Same rules as for 2009:
#   - vendor_id here is TEXT (like 2009's vendor_name), not the number code
#     VendorID from 2011+, so it goes into vendor_name
#   - payment_type here is TEXT, so it goes into payment_type_text. It has the
#     SAME file column name as 2011+ but a different kind of value, which is
#     why each layout needs its own mapping (see get_taxi_mapping)
#   - rate_code is text here, but it only contains "1" (checked on 2010-01,
#     every single row), so we store it as int in rate_code like the other years
#   - store_and_fwd_flag is text like in 2011+, so it shares that column. BUT
#     the values are different: 2010 has "0"/"1" (plus 1 row "*" and 1 row
#     "2", and lots of nulls) where 2011+ has "Y"/"N". We store them as they
#     are (as text, so the weird ones don't crash anything) and Spark has to
#     turn both into the same thing later.
# CHECKED with check_text_columns.py on 2009-01 and 2010-01:
#   - vendor_id (2010) and vendor_name (2009) both use "VTS", "CMT", "DDS",
#     so sharing the vendor_name column is fine
#   - payment_type (2010) is cut to 3 letters ("CAS", "Cre", "No ", "Dis"),
#     2009 has the full words ("CASH", "Credit", "No Charge", "Dispute"),
#     both with mixed upper/lower case. Same meaning, so they share
#     payment_type_text, stored as they are. Cleaning them is for Spark.
TAXI_COLUMN_MAPPING_2010: Dict[str, Tuple[str, str]] = {
    "vendor_id":             ("vendor_name", "string"),
    "pickup_datetime":       ("pickup_ts", "timestamp"),
    "dropoff_datetime":      ("dropoff_ts", "timestamp"),
    "passenger_count":       ("passengers", "int"),
    "trip_distance":         ("distance", "double"),
    "pickup_longitude":      ("pu_lon", "double"),
    "pickup_latitude":       ("pu_lat", "double"),
    "rate_code":             ("rate_code", "int"),
    "store_and_fwd_flag":    ("store_and_fwd_flag", "string"),
    "dropoff_longitude":     ("do_lon", "double"),
    "dropoff_latitude":      ("do_lat", "double"),
    "payment_type":          ("payment_type_text", "string"),
    "fare_amount":           ("fare", "double"),
    "surcharge":             ("surcharge", "double"),
    "mta_tax":               ("mta_tax", "double"),
    "tip_amount":            ("tip", "double"),
    "tolls_amount":          ("tolls", "double"),
    "total_amount":          ("total", "double"),
}

# Which layout a file has is decided by its pickup column:
#   pickup column -> mapping for that layout
# If we find another layout, add its mapping here.
TAXI_LAYOUTS: Dict[str, Dict[str, Tuple[str, str]]] = {
    "tpep_pickup_datetime": TAXI_COLUMN_MAPPING,
    "Trip_Pickup_DateTime": TAXI_COLUMN_MAPPING_2009,
    "pickup_datetime":      TAXI_COLUMN_MAPPING_2010,
}

# Flights = the OpenSky arrivals at JFK our teammate downloaded
# (download_flight_data/ on the feat/download_arrivals branch). All 14 CSV
# columns are loaded, nothing dropped, same rules as taxi.
#   - firstSeen / lastSeen are Unix SECONDS (UTC). Type "unix_seconds" stores
#     them like every other timestamp in HBase (milliseconds as a long), so
#     Spark reads flight and taxi times the same way. Only the unit changes.
#   - firstSeen_utc / lastSeen_utc are the same times as text; we keep them
#     as they are (text) because the team decided not to drop columns.
#   - the distances are whole metres and the candidate counts whole numbers.
# CHECKED on all 155 files (2025-01 to 2025-11): 192,940 flights, these exact
# 14 columns everywhere, estArrivalAirport is always KJFK, estDepartureAirport
# is empty for 28,338 flights (14.7%), callsign empty for 16, firstSeen
# empty for 18.
# >>> PLACEHOLDER: the column names in HBase must match the Avro schema of
#     the flights streaming script.
FLIGHTS_COLUMN_MAPPING: Dict[str, Tuple[str, str]] = {
    "icao24":                           ("icao24", "string"),
    "callsign":                         ("callsign", "string"),
    "estDepartureAirport":              ("dep_airport", "string"),
    "estArrivalAirport":                ("arr_airport", "string"),
    "firstSeen":                        ("first_seen_ts", "unix_seconds"),
    "lastSeen":                         ("last_seen_ts", "unix_seconds"),
    "firstSeen_utc":                    ("first_seen_utc", "string"),
    "lastSeen_utc":                     ("last_seen_utc", "string"),
    "estDepartureAirportHorizDistance": ("dep_horiz_dist", "int"),
    "estDepartureAirportVertDistance":  ("dep_vert_dist", "int"),
    "estArrivalAirportHorizDistance":   ("arr_horiz_dist", "int"),
    "estArrivalAirportVertDistance":    ("arr_vert_dist", "int"),
    "departureAirportCandidatesCount":  ("dep_candidates", "int"),
    "arrivalAirportCandidatesCount":    ("arr_candidates", "int"),
}


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


# VALUE ENCODING  (Python value to bytes for HBase)
# HBase only stores bytes, so every value has to be converted.
#
# We HAVE to convert them the same way the Kafka Connect HBase sink does,
# otherwise a fare of 12.5 from 2014 and a fare of 12.5 from 2017 end up as
# different bytes and Spark can't read both with the same code.
# The sink uses Java's HBase `Bytes.toBytes(...)`, which means:
#   string    -> the text as UTF-8 bytes
#   int       -> 4 bytes, big-endian         (struct ">i")
#   long      -> 8 bytes, big-endian         (struct ">q")
#   float     -> 4 bytes IEEE float          (struct ">f")
#   double    -> 8 bytes IEEE double         (struct ">d")
#   boolean   -> 1 byte: 0xFF true, 0x00 false
#   timestamp -> milliseconds since 1970 as a long (8 bytes)
# ">" in struct means big-endian, which is what Java uses.
#
# Downside: in the HBase shell the numbers show up as unreadable bytes like
# \x00\x00\x00\x8E instead of "142". That's normal.

def to_epoch_millis(value) -> int:
    """Timestamp (datetime / pd.Timestamp / string) to milliseconds since 1970."""
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        # The value has no timezone, so we attach the one we picked in the
        # config. ambiguous/nonexistent handle the daylight saving hours:
        #   ambiguous=False         -> 01:30 on the night clocks go back
        #                              happens twice, we take the second one
        #   nonexistent="shift_forward" -> 02:30 on the night clocks go
        #                              forward doesn't exist, we move it to 03:00
        # With "UTC" none of this ever happens.
        ts = ts.tz_localize(NAIVE_TIMESTAMP_TZ, ambiguous=False,
                            nonexistent="shift_forward")
    # .value is nanoseconds since 1970 (UTC). // 1_000_000 gives milliseconds.
    return ts.value // 1_000_000


def encode_value(value, value_type: str) -> Optional[bytes]:
    """Turn one value into bytes. Returns None for missing values, which
    means 'don't write this cell' (HBase is sparse).
    The sink deletes a cell when it gets a null, so skipping it gives the
    same end result for new rows."""

    # Missing values. pandas uses NaN (numbers) and NaT (dates) instead of
    # None, and pyarrow gives None. pd.isna() catches all of them.
    if value is None or pd.isna(value):
        return None

    # The 2009/2010 files have text columns, and an empty text "" there
    # means missing too. For numbers and timestamps we treat it like a
    # missing value, otherwise int("") / pd.Timestamp("") would crash the
    # load. Real text columns keep "" as it is.
    if value_type != "string" and isinstance(value, str) and value.strip() == "":
        return None

    if value_type == "string":
        return str(value).encode("utf-8")

    # int/long go through float() first, so "5", 5, 5.0 and "5.0" all work.
    if value_type == "int":
        return struct.pack(">i", int(float(value)))
    if value_type == "long":
        return struct.pack(">q", int(float(value)))

    if value_type == "float":
        return struct.pack(">f", float(value))
    if value_type == "double":
        return struct.pack(">d", float(value))

    if value_type == "boolean":
        return b"\xff" if float(value) != 0 else b"\x00"

    if value_type == "timestamp":
        return struct.pack(">q", to_epoch_millis(value))

    # Unix seconds (the flight data): already a real UTC moment, so no
    # timezone guessing. Stored the same way as "timestamp" (milliseconds).
    if value_type == "unix_seconds":
        return struct.pack(">q", int(float(value)) * 1000)

    raise ValueError(f"Unknown type in column mapping: {value_type}")


def build_cells(row: dict, mapping: Dict[str, Tuple[str, str]],
                family: str) -> Dict[bytes, bytes]:
    """Turn one row from the file (dict: column -> value) into the dict
    happybase wants:  {b"family:column": b"value", ...}"""
    cells: Dict[bytes, bytes] = {}
    for source_column, (hbase_column, value_type) in mapping.items():
        # .get() gives None if the column isn't in this file, instead of
        # crashing. We already warned about missing columns when we opened it.
        encoded = encode_value(row.get(source_column), value_type)
        if encoded is not None:
            cells[f"{family}:{hbase_column}".encode("utf-8")] = encoded
    return cells


# ROW KEY DESIGN  (the most important decision for HBase performance)
# What we have to keep in mind:
#   1. HBase stores rows SORTED by row key and splits the table into REGIONS
#      (ranges of keys). Each region is handled by one RegionServer.
#   2. If keys start with something that keeps going up (like a timestamp),
#      ALL new writes go to the LAST region, so one server does all the work
#      and the rest do nothing ("hotspotting"). Only matters if we have
#      more than one RegionServer.
#   3. Reads are fastest when the rows we need are next to each other
#      (a "range scan" over a key prefix).
#   4. The key must be UNIQUE per ride, or rides overwrite each other.
#      The taxi data has NO trip ID, and two rides can have the same pickup
#      time and zone, so we add a tie-breaker (file name + row number).
#   5. The key must always be the SAME for the same ride, so reruns
#      overwrite instead of making duplicates.
#
# How the sink builds row keys: from the KAFKA KEY of each message. If the
# key is a plain string, that string becomes the row key as it is. So the
# streaming script has to send exactly the key this function builds as its
# Kafka key. Easiest way: both scripts import the same function.
#
# Prototype design for taxi:
#     [<salt>|]<pickup_ts>|<source_file>|<row_number>
#   - salt (optional, NUM_SALT_BUCKETS > 0): crc32(rest of the key) % N,
#     2 digits. Spreads the writes over N key ranges (fixes point 2).
#     Cost: "all rides between 08:00 and 09:00" turns into N small scans
#     instead of 1.
#   - pickup_ts: so the rides are sorted by time.
#   - source_file + row_number: tie-breaker (point 4), always the same (point 5).
#
# Other options:
#   - <pu_zone>|<pickup_ts>|... : spreads writes over ~265 zones by itself
#     and makes "all rides from zone X in period Y" fast (would be good for the
#     heatmap). Busy zones (Manhattan, airports) get more load though.
# >>> PLACEHOLDER: final row key design. Must be IDENTICAL to the Kafka key
#     the streaming script sends.

def format_key_time(value) -> str:
    """Pickup time as text for the row key, e.g. "2014-03-01T08:15:00".
    This is the wall clock time from the file, no timezone conversion."""
    if value is None or pd.isna(value):
        return "unknown"
    return pd.Timestamp(value).strftime("%Y-%m-%dT%H:%M:%S")


def build_taxi_row_key(row: dict, source_file: str, row_number: int) -> bytes:
    # The part that makes the key unique and always the same.
    # :09d pads the row number with zeros (000000042), so keys sort right
    # as text. Without padding "10" would come before "9".
    # The pickup column has a different name depending on the layout.
    pickup = next((row[c] for c in TAXI_LAYOUTS if c in row), None)
    unique_part = (f"{format_key_time(pickup)}"
                   f"|{source_file}|{row_number:09d}")
    if NUM_SALT_BUCKETS > 0:
        # crc32 gives a big positive number, % N turns it into 0..N-1.
        salt = zlib.crc32(unique_part.encode("utf-8")) % NUM_SALT_BUCKETS
        return f"{salt:02d}|{unique_part}".encode("utf-8")
    return unique_part.encode("utf-8")


# Flights row key: <lastSeen as UTC time>|<icao24>
#   e.g. "2025-01-02T23:57:59|a1a3cc"
#   - lastSeen first, so flights are sorted by (roughly) landing time and
#     "all arrivals between X and Y" is one range scan.
#   - icao24 = the plane. One plane can't land twice in the same second, so
#     the pair is unique. CHECKED: 0 duplicates of (icao24, lastSeen) in all
#     192,940 flights. (icao24, firstSeen) is NOT unique: 2 duplicates.
#   - no file name / row number needed (unlike taxi): the key only uses the
#     flight's own data, so the streaming script can build the exact same
#     key, and reruns overwrite instead of duplicating.
#   - flights are small (~635 per day), so hotspotting doesn't matter here.
# NOTE: this time is real UTC, while the taxi keys are New York wall clock
# time. Keep that in mind when joining taxis and flights by time.
# >>> PLACEHOLDER: final flights row key (must match the streaming script).
def build_flight_row_key(row: dict, source_file: str, row_number: int) -> bytes:
    landed = pd.Timestamp(int(float(row["lastSeen"])), unit="s")
    return f"{landed.strftime('%Y-%m-%dT%H:%M:%S')}|{row['icao24']}".encode("utf-8")


# FINDING THE FILES

# Matches "..._2014-03.parquet" and grabs the year and the month.
TAXI_FILENAME_PATTERN = re.compile(r"_(\d{4})-(\d{2})\.parquet$")

def list_taxi_files() -> List[Path]:
    """The taxi data is one Parquet file per month with the year in the name,
    so the year split is just looking at file names. We never have to open a
    file to know which half it belongs to."""
    selected = []

    for path in sorted(TAXI_DATA_DIR.glob("*.parquet")):
        match = TAXI_FILENAME_PATTERN.search(path.name)
        if not match:
            log.warning("Skipping %s: name doesn't match the expected pattern", path.name)
            continue
        year = int(match.group(1))
        if HISTORICAL_FIRST_YEAR <= year <= HISTORICAL_LAST_YEAR:
            selected.append(path)
    return selected


# Matches "2025-01-01_2025-01-02.csv" (first and last day in the file) and
# grabs the year of the first day.
FLIGHTS_FILENAME_PATTERN = re.compile(r"^(\d{4})-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}\.csv$")

def list_flight_files() -> List[Path]:
    """The flight files are in folders like
        airport=KJFK/year=2025/month=01/2025-01-01_2025-01-02.csv
    so we look in all subfolders (rglob) and, like taxi, take the year from
    the file name. One file never covers two months (the download script
    makes sure of that), so it never covers two years either."""
    selected = []
    for path in sorted(FLIGHTS_DATA_DIR.rglob("*.csv")):
        match = FLIGHTS_FILENAME_PATTERN.match(path.name)
        if not match:
            log.warning("Skipping %s: name doesn't match the expected pattern", path.name)
            continue
        year = int(match.group(1))
        if FLIGHTS_FIRST_YEAR <= year <= FLIGHTS_LAST_YEAR:
            selected.append(path)
    return selected


# READING THE FILES IN CHUNKS
# Both readers are GENERATORS (they use `yield`). A generator gives us rows
# one at a time when we ask for them, instead of building one giant list.
# That's what keeps the memory low: we only hold one batch at a time.

def get_taxi_mapping(path: Path) -> Dict[str, Tuple[str, str]]:
    """Look at the columns of one taxi file and return the mapping of its
    layout. Every file gets ONE mapping, because the same column name can
    mean different things in different layouts (payment_type is text in 2010
    and a number code in 2011+)."""
    parquet_file = pq.ParquetFile(path)

    # Parquet keeps its schema at the end of the file, so we can check the
    # columns without reading any data.
    available = set(parquet_file.schema_arrow.names)
    # Find the layout of this file by looking at which pickup column it has.
    pickup_column = next((c for c in TAXI_LAYOUTS if c in available), None)
    if pickup_column is None:
        # No known pickup column = we can't build proper row keys, and it
        # means the file has a layout that isn't in TAXI_LAYOUTS yet. We stop
        # instead of skipping it, so a full run never silently drops a whole month.
        raise ValueError(f"{path.name} has none of the known pickup columns "
                         f"{list(TAXI_LAYOUTS)}, probably a layout we haven't seen yet. "
                         f"Add a mapping for it to TAXI_LAYOUTS.")
    mapping = TAXI_LAYOUTS[pickup_column]
    log.info("%s: layout with pickup column %s", path.name, pickup_column)

    wanted = [c for c in mapping if c in available]
    missing = [c for c in mapping if c not in available]
    if missing:
        # A file can match a layout but still miss some of its columns
        # (e.g. fees that didn't exist yet in that year). A warning, not a
        # crash, so one dry run shows them all.
        log.warning("%s is missing columns: %s", path.name, missing)
    # The other direction: columns in the file that aren't in the mapping
    # would be silently left out. We decided the initial load doesn't drop
    # anything, so if this shows up, the column has to be added to the mapping.
    # Exception: "__index_level_0__" (in 2010-02 and 2010-03) isn't taxi data,
    # it's the row number pandas saved into the file by accident when TLC made
    # it. So we leave it out on purpose and don't warn about it.
    not_loaded = sorted(c for c in available - set(mapping)
                        if not c.startswith("__index_level_"))
    if not_loaded:
        log.warning("%s has columns we DON'T load: %s", path.name, not_loaded)
    return mapping


def get_flights_mapping(path: Path) -> Dict[str, Tuple[str, str]]:
    """Flights only have one layout (for now), so every file gets the same mapping."""
    return FLIGHTS_COLUMN_MAPPING


def iter_taxi_rows(path: Path, mapping: Dict[str, Tuple[str, str]]) -> Iterator[dict]:
    parquet_file = pq.ParquetFile(path)
    # Only ask for the columns of this file's mapping that are really there
    # (the missing ones were already reported by get_taxi_mapping).
    available = set(parquet_file.schema_arrow.names)
    wanted = [c for c in mapping if c in available]

    # iter_batches reads batch_size rows at a time and ONLY the columns we
    # ask for. Parquet stores data per column, so the other columns aren't
    # even read from disk.
    for batch in parquet_file.iter_batches(batch_size=READ_BATCH_SIZE, columns=wanted):
        # to_pylist() turns the batch into a list of dicts:
        #   [{"tpep_pickup_datetime": datetime(...), "fare_amount": 12.5, ...}, ...]
        # Good enough for now. It's also the slowest part of this function;
        # a faster version would work on whole columns instead of dicts.
        for row in batch.to_pylist():
            yield row


def iter_flight_rows(path: Path, mapping: Dict[str, Tuple[str, str]]) -> Iterator[dict]:
    # usecols: only read the columns we map (less memory, faster).
    #   With a function instead of a list, missing columns don't crash it.
    # chunksize: gives us DataFrames of this many rows one after the other,
    #   instead of loading the whole file.
    # dtype=str: read everything as text. encode_value() converts it to the
    #   right type anyway, and this stops pandas from guessing different types
    #   in different chunks. Missing values still come back as NaN.
    reader = pd.read_csv(
        path,
        usecols=lambda col: col in mapping,
        chunksize=READ_BATCH_SIZE,
        dtype=str,
    )
    for chunk in reader:
        # No year filter here anymore: list_flight_files() already picked the
        # files by year, and every row is inside its file's dates (CHECKED).
        # orient="records" gives a list of dicts, one per row (same as taxi).
        for row in chunk.to_dict(orient="records"):
            yield row


# DESCRIBING A DATASET
# Taxi and flights go through the same steps, only the details are different.
# So instead of writing the loading loop twice, we describe each dataset in
# one object and write the loop once.

@dataclass
class DatasetSpec:
    name: str                                            # "taxi" / "flights"
    table: str                                           # HBase table name
    family: str                                          # column family we write into
    column_families: dict                                # used to create the table
    # gives the mapping (file column -> (hbase column, type)) for one file
    get_mapping: Callable[[Path], Dict[str, Tuple[str, str]]]
    list_files: Callable[[], List[Path]]                 # which files to load
    # how to read one file, using that file's mapping
    iter_rows: Callable[[Path, Dict[str, Tuple[str, str]]], Iterator[dict]]
    build_row_key: Callable[[dict, str, int], bytes]     # how to build the key


DATASETS = {
    "taxi": DatasetSpec(
        name="taxi",
        table=TAXI_TABLE,
        family=TAXI_FAMILY,
        column_families=TAXI_COLUMN_FAMILIES,
        get_mapping=get_taxi_mapping,
        list_files=list_taxi_files,
        iter_rows=iter_taxi_rows,
        build_row_key=build_taxi_row_key,
    ),
    "flights": DatasetSpec(
        name="flights",
        table=FLIGHTS_TABLE,
        family=FLIGHTS_FAMILY,
        column_families=FLIGHTS_COLUMN_FAMILIES,
        get_mapping=get_flights_mapping,
        list_files=list_flight_files,
        iter_rows=iter_flight_rows,
        build_row_key=build_flight_row_key,
    ),
}


# PROGRESS TRACKING

class Progress:
    """Keeps the set of finished file names and saves it to a JSON file
    every time a file is done."""

    def __init__(self, path: Path):
        self.path = path
        self.done = set()
        if path.exists():
            self.done = set(json.loads(path.read_text()))
            log.info("Resuming: %d files already loaded", len(self.done))

    def is_done(self, key: str) -> bool:
        return key in self.done

    def mark_done(self, key: str) -> None:
        self.done.add(key)
        # Write to a temp file first, then rename it over the real one.
        # A rename happens all at once, so if the pod gets killed while
        # writing, we still have the old complete file and not a broken one.
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(sorted(self.done)))
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


def ensure_table(connection: happybase.Connection, spec: DatasetSpec) -> None:
    """Create the table if it doesn't exist yet.
    The sink also has auto.create.tables: true, so whoever runs first creates
    it. Both create it with the same name and family, so it doesn't matter."""
    # connection.tables() gives the names as BYTES, so we compare with bytes.
    existing = connection.tables()
    if spec.table.encode("utf-8") in existing:
        log.info("Table %s already exists", spec.table)
        return

    # LIMITATION: happybase can't create a PRE-SPLIT table (a table that
    # starts with several regions). A new table starts with ONE region, so at
    # first all writes go to one RegionServer until HBase splits it.
    # If we use salting with several RegionServers, it's better to create the
    # table in the HBase shell with split points, e.g.:
    #   create 'taxis', 'taxis', SPLITS => ['01','02',...,'15']
    # >>> PLACEHOLDER: decide who creates the tables (this script, or an
    #     HBase shell script).
    log.info("Creating table %s with families %s", spec.table, list(spec.column_families))
    connection.create_table(spec.table, spec.column_families)


# LOADING ONE FILE

@dataclass
class FileProgress:
    """How far we got in ONE file. It survives a failed attempt, so the retry
    can continue from here instead of starting the file over."""
    next_row: int = 0          # first row number NOT confirmed by HBase yet
    rows_written: int = 0      # rows HBase confirmed so far (all attempts)
    started: float = 0.0       # when we started this file (for rows/s)


def load_file(table: Optional[happybase.Table], spec: DatasetSpec, path: Path,
              dry_run: bool, dry_run_rows: int, limit_rows: Optional[int],
              progress: FileProgress) -> int:
    """Read one file and write its rows to HBase, starting at
    progress.next_row. Returns how many rows were written in total."""
    # Pick the mapping for THIS file once (for taxi: depends on the layout).
    mapping = spec.get_mapping(path)

    if dry_run:
        # DRY RUN: no HBase at all. Prints the first few rows exactly how they
        # WOULD be written. Just for me to test the script is working.
        for row_number, row in enumerate(spec.iter_rows(path, mapping)):
            if row_number >= dry_run_rows:
                break
            key = spec.build_row_key(row, path.stem, row_number)
            log.info("DRY RUN %s -> %s", key, build_cells(row, mapping, spec.family))
        return 0

    # We fill our own list of rows and send it ourselves when it's full,
    # instead of letting happybase send automatically. That way we know
    # EXACTLY which rows HBase confirmed: only after send() returns without
    # an error, we move progress.next_row forward. If a send fails, nothing
    # moves, and the retry continues from the first row of that batch.
    # (Sending is one Thrift call per batch, same speed as before.)
    pending = []      # (row_number, key, cells) not sent yet

    def send_pending():
        batch = table.batch()          # no batch_size: only sends on send()
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

    if progress.next_row > 0:
        log.info("%s: continuing from row %d", path.name, progress.next_row)

    # enumerate() gives us a counter: the row number inside this file,
    # which we use as the tie-breaker in the row key.
    for row_number, row in enumerate(spec.iter_rows(path, mapping)):
        # Rows HBase already confirmed in an earlier attempt: skip them.
        # (We still read them from the file to keep the row numbers right,
        # reading is fast compared to writing.)
        if row_number < progress.next_row:
            continue
        # --limit-rows: stop early, only for testing.
        if limit_rows is not None and row_number >= limit_rows:
            break
        cells = build_cells(row, mapping, spec.family)
        if not cells:
            # Every value we wanted was missing, nothing to store.
            continue
        # path.stem = file name without .parquet, e.g. "yellow_tripdata_2014-03"
        key = spec.build_row_key(row, path.stem, row_number)
        pending.append((row_number, key, cells))
        if len(pending) >= WRITE_BATCH_SIZE:
            send_pending()
    # The last, half-full batch.
    if pending:
        send_pending()

    elapsed = time.time() - progress.started
    # max(..., 1e-9) so we never divide by zero on a tiny/empty file.
    log.info("%s: %d rows in %.1fs (%.0f rows/s)", path.name, progress.rows_written,
             elapsed, progress.rows_written / max(elapsed, 1e-9))
    return progress.rows_written


def load_file_with_retries(connection, spec, path, dry_run, dry_run_rows, limit_rows) -> int:
    """Wraps load_file: if HBase / Thrift fails, wait, reconnect and CONTINUE
    the file from the last batch HBase confirmed. Rewriting a few rows of the
    failed batch is safe, because the keys never change (it just overwrites).
    We only give up after MAX_RETRIES failures in a row with no progress."""
    progress = FileProgress(started=time.time())
    failures_in_a_row = 0
    while True:
        row_before = progress.next_row
        try:
            table = None if dry_run else connection.table(spec.table)
            return load_file(table, spec, path, dry_run, dry_run_rows, limit_rows, progress)
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
    log.error("Giving up on %s after %d failures in a row (%d rows were confirmed, "
              "a rerun loads the whole file again)", path.name, MAX_RETRIES, progress.rows_written)
    sys.exit(1)


# COMMAND-LINE ARGUMENTS

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initial load of historical data into HBase")
    # choices= rejects anything else with a clear error.
    parser.add_argument("--dataset", choices=["taxi", "flights", "all"], default="all",
                        help="which dataset to load")
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

def main() -> None:
    setup_logging()
    args = parse_args()

    # Which datasets to run, in order.
    names = ["taxi", "flights"] if args.dataset == "all" else [args.dataset]
    specs = [DATASETS[n] for n in names]

    # In dry-run we never connect, so we dont need HBase / Kubernetes
    connection = None if args.dry_run else connect()
    progress = Progress(PROGRESS_FILE)

    total_rows = 0
    total_started = time.time()

    for spec in specs:
        if connection is not None:
            ensure_table(connection, spec)

        files = spec.list_files()
        if args.limit_files is not None:
            files = files[:args.limit_files]
        log.info("[%s] %d files to process", spec.name, len(files))

        for path in files:
            # We put the dataset name in front, so a taxi file and a flights
            # file with the same name can't get mixed up in the progress file.
            progress_key = f"{spec.name}/{path.name}"
            if not args.dry_run and progress.is_done(progress_key):
                log.info("Skipping %s (already loaded)", path.name)
                continue

            total_rows += load_file_with_retries(
                connection, spec, path, args.dry_run, args.dry_run_rows, args.limit_rows)

            # Only mark a file as done AFTER all of it was written. If we crash
            # in the middle, the file is loaded again on the next run.
            # Test runs with --limit-rows only wrote part of the file, so they
            # don't count as done.
            if not args.dry_run and args.limit_rows is None:
                progress.mark_done(progress_key)

    if connection is not None:
        connection.close()

    log.info("Finished: %d rows in %.1f minutes",
             total_rows, (time.time() - total_started) / 60)


if __name__ == "__main__":
    main()