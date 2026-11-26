"""
convert_to_avro.py  (PROTOTYPE, not tested on our cluster yet)

What this script does (the "Converting to AVRO" part of our Python script box)
------------------------------------------------------------------------------
1. Finds the taxi Parquet files and the flights CSV files.
2. Reads them in chunks, so memory stays low no matter how big a file is.
3. Gives the columns the names and types that initial_load.py uses in HBase,
   with the common part (../common/shared.py), the same code initial_load.py
   uses. Taxi times are converted to real UTC there.
4. Writes one .avro file per input file.

It only converts: every row is kept, in the order of the file.

How to read this file
---------------------
- Everything we haven't decided yet is marked with:
      # >>> PLACEHOLDER: ...
- Everything else is explained in the comment above it.

Dependencies (requirements.txt)
-------------------------------
    fastavro   (writes the Avro files)
    pyarrow    (reads Parquet and CSV, converts whole columns at once)
    tzdata     (timezone database, Windows and slim images don't have one)

How to run it
-------------
    python convert_to_avro.py --dataset flights
    python convert_to_avro.py --dataset taxi --limit-files 1
    python convert_to_avro.py
"""

import argparse

import logging

import os

import sys

import time

from dataclasses import dataclass

from pathlib import Path

from typing import Callable, Dict, Iterator, List, Optional, Tuple

import fastavro
import pyarrow as pa

# The common part (mappings, conversion, row keys, file listing), shared with
# initial_load.py. It's in ../common; Python looks in the script's own folder
# first, so it also works when all files are in one folder.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))
import shared  # noqa: E402  (has to come after the sys.path line)


# CONFIGURATION
# Like initial_load.py: environment variables with defaults that work locally.

HERE = Path(__file__).resolve().parent

# >>> PLACEHOLDER: where the live taxi files and the .avro output go isn't
# decided yet.
TAXI_DATA_DIR = Path(os.environ.get("TAXI_DATA_DIR", HERE / "data" / "taxi"))
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", HERE / "data" / "avro"))
# Flights: where download_flight_data/download_arrivals.py saves them.
FLIGHTS_DATA_DIR = Path(os.environ.get(
    "FLIGHTS_DATA_DIR", HERE.parent / "download_flight_data" / "data" / "opensky_arrivals"))

# The taxi timezone (TAXI_SOURCE_TZ) and READ_BATCH_SIZE are in shared.py, so
# both scripts use the same values. Taxi times are stored as real UTC.

# The mappings, the types, the Avro schema and the column conversion are in
# shared.py too (TAXI_COLUMN_MAPPING, FLIGHTS_COLUMN_MAPPING, TYPES,
# build_avro_schema(), convert_column(), convert_batch()).


# LOGGING SETUP

log = logging.getLogger("convert_to_avro")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


# VALUE CONVERSION
# shared.convert_batch() does it for whole columns, the same way as for
# initial_load.py. Avro wants one dict per row, so we turn the table into those.

def convert_batch(batch, mapping: Dict[str, Tuple[str, str]]) -> List[dict]:
    """One batch from a file -> Avro records, with every field of the schema."""
    return shared.convert_batch(batch, mapping).to_pylist()


# FINDING AND READING THE FILES
# shared.py has them: list_taxi_files(), list_flight_files(), read_taxi_columns(),
# read_flight_columns(), iter_taxi_batches(), iter_flight_batches().


# DESCRIBING A DATASET (same idea as in initial_load.py)

@dataclass
class DatasetSpec:
    name: str                                         # "taxi" / "flights", also the output folder
    input_dir: Path
    mapping: Dict[str, Tuple[str, str]]               # file column -> (field, type)
    schema: dict                                      # Avro schema
    required_columns: Tuple[str, ...]                 # without these it's another layout
    list_files: Callable[[], List[Path]]
    read_columns: Callable[[Path], List[str]]
    iter_batches: Callable[[Path], Iterator[pa.RecordBatch]]


DATASETS = {
    "taxi": DatasetSpec(
        name="taxi",
        input_dir=TAXI_DATA_DIR,
        mapping=shared.TAXI_COLUMN_MAPPING,
        schema=shared.build_avro_schema("TaxiRide", shared.TAXI_COLUMN_MAPPING),
        required_columns=shared.TAXI_REQUIRED_COLUMNS,
        list_files=lambda: shared.list_taxi_files(TAXI_DATA_DIR),
        read_columns=shared.read_taxi_columns,
        iter_batches=shared.iter_taxi_batches,
    ),
    "flights": DatasetSpec(
        name="flights",
        input_dir=FLIGHTS_DATA_DIR,
        mapping=shared.FLIGHTS_COLUMN_MAPPING,
        schema=shared.build_avro_schema("FlightArrival", shared.FLIGHTS_COLUMN_MAPPING),
        required_columns=shared.FLIGHTS_REQUIRED_COLUMNS,
        list_files=lambda: shared.list_flight_files(FLIGHTS_DATA_DIR),
        read_columns=shared.read_flight_columns,
        iter_batches=shared.iter_flight_batches,
    ),
}


def check_files(spec: DatasetSpec, files: List[Path]) -> None:
    """Look at the columns of every file BEFORE we start converting."""
    for path in files:
        unknown = shared.check_columns(path.name, spec.read_columns(path),
                                       spec.mapping, spec.required_columns)
        if unknown:
            log.warning("%s has columns we DON'T convert: %s", path.name, unknown)


# CONVERTING ONE FILE

def convert_file(spec: DatasetSpec, source: Path, target: Path) -> int:
    """Write one Avro file. Returns how many rows it has."""
    rows = 0

    def records() -> Iterator[dict]:
        nonlocal rows
        for batch in spec.iter_batches(source):
            converted = convert_batch(batch, spec.mapping)
            rows += len(converted)
            yield from converted

    target.parent.mkdir(parents=True, exist_ok=True)
    # Temp file + rename, so a stopped run never leaves half an .avro file.
    tmp = target.with_name(target.name + ".tmp")
    try:
        with tmp.open("wb") as out:
            # deflate: every Avro reader supports it. Level 1 is the fastest.
            fastavro.writer(out, spec.schema, records(),
                            codec="deflate", codec_compression_level=1)
        tmp.replace(target)
    finally:
        tmp.unlink(missing_ok=True)
    return rows


# COMMAND-LINE ARGUMENTS

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert taxi and flight files to Avro")
    parser.add_argument("--dataset", choices=["taxi", "flights", "all"], default="all",
                        help="which dataset to convert")
    parser.add_argument("--limit-files", type=int, default=None,
                        help="only convert the first N files of each dataset (for testing)")
    args = parser.parse_args(argv)
    if args.limit_files is not None and args.limit_files < 1:
        parser.error("--limit-files has to be at least 1")
    return args


# MAIN

def main(argv: Optional[List[str]] = None) -> int:
    setup_logging()
    args = parse_args(argv)
    # Fails right away if this machine can't convert New York time to UTC.
    shared.check_timezone_support()
    names = ["taxi", "flights"] if args.dataset == "all" else [args.dataset]

    selected = []
    for spec in (DATASETS[name] for name in names):
        files = spec.list_files()[:args.limit_files]
        if not files:
            log.error("[%s] no files in %s. Check the folder, or pick the other one "
                      "with --dataset.", spec.name, spec.input_dir)
            return 1
        check_files(spec, files)
        selected.append((spec, files))

    started = time.monotonic()
    for spec, files in selected:
        total = 0
        for number, source in enumerate(files, start=1):
            # Same subfolders and file name as the input, so flights keep year=/month=.
            relative = source.relative_to(spec.input_dir).with_suffix(".avro")
            rows = convert_file(spec, source, OUTPUT_DIR / spec.name / relative)
            total += rows
            log.info("[%s] %d/%d %s: %d rows", spec.name, number, len(files), source.name, rows)
        log.info("[%s] done: %d rows in %s", spec.name, total, OUTPUT_DIR / spec.name)

    log.info("Finished in %.1f minutes", (time.monotonic() - started) / 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
