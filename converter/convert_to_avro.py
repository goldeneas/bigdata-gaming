"""
convert_to_avro.py  (PROTOTYPE, not tested on our cluster yet)

What this script does (the "Converting to AVRO" part of our Python script box)
------------------------------------------------------------------------------
1. Finds the taxi Parquet files and the flights CSV files.
2. Reads them in chunks, so memory stays low no matter how big a file is.
3. Gives the columns the names and types that initial_load.py uses in HBase.
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
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq


# CONFIGURATION
# Like initial_load.py: environment variables with defaults that work locally.

HERE = Path(__file__).resolve().parent

# >>> PLACEHOLDER: depends on where we keep the data.
TAXI_DATA_DIR = Path(os.environ.get("TAXI_DATA_DIR", HERE / "data" / "taxi"))
FLIGHTS_DATA_DIR = Path(os.environ.get(
    "FLIGHTS_DATA_DIR", HERE.parent / "download_flight_data" / "data" / "opensky_arrivals"))
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", HERE / "data" / "avro"))

# MUST have the same value as in initial_load.py (see the comment there).
# "UTC" = we store the New York wall clock as if it was UTC.
NAIVE_TIMESTAMP_TZ = os.environ.get("NAIVE_TIMESTAMP_TZ", "UTC")

READ_BATCH_SIZE = int(os.environ.get("READ_BATCH_SIZE", "50000"))


# SCHEMA: WHICH COLUMNS GO INTO AVRO
# column in the file -> (Avro field = HBase column, type)
# Copied from initial_load.py, only its layout for 2011 and later. The 2009/2010
# files are rejected with an error.
#
# CHECKED the columns of all 129 months 2016-01 to 2026-09. Three things are new
# compared to 2011-2015, they are marked below.

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
    "airport_fee":           ("airport_fee", "double"),          # up to 2023-01
    "Airport_fee":           ("airport_fee", "double"),          # NEW: capital A from 2023-02
    "cbd_congestion_fee":    ("cbd_congestion_fee", "double"),   # NEW: from 2025-01
    "request_source":        ("request_source", "string"),       # NEW: from 2026-06
}

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

# our type -> (Avro type, Arrow type we convert the column to)
# Both time types are milliseconds since 1970, like in initial_load.py.
TYPES = {
    "string":       ("string", pa.string()),
    "int":          ("int", pa.int32()),
    "double":       ("double", pa.float64()),
    "timestamp":    ({"type": "long", "logicalType": "timestamp-millis"}, pa.int64()),
    "unix_seconds": ({"type": "long", "logicalType": "timestamp-millis"}, pa.int64()),
}


def build_schema(name: str, mapping: Dict[str, Tuple[str, str]]) -> dict:
    """The Avro schema of one dataset, made from its mapping so they can't differ.
    Every field may be null, that way one schema fits all the years."""
    fields = [{"name": field, "type": ["null", TYPES[value_type][0]], "default": None}
              for field, value_type in dict(mapping.values()).items()]
    return {"type": "record", "name": name, "namespace": "bigdata_gaming", "fields": fields}


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
# initial_load.py converts value by value (encode_value). Here we convert whole
# columns, which is a lot faster, with the same rules.

def convert_column(column, value_type: str):
    arrow_type = TYPES[value_type][1]
    if pa.types.is_null(column.type):       # completely empty, e.g. airport_fee in 2016
        return pa.nulls(len(column), arrow_type)
    if value_type == "string":
        return column.cast(arrow_type)

    if value_type == "timestamp":
        times = column.cast(pa.timestamp("ms"), safe=False)
        if NAIVE_TIMESTAMP_TZ != "UTC":
            # Same daylight saving rules as to_epoch_millis() in initial_load.py:
            # the hour that happens twice = the second one, the missing hour = 03:00.
            times = pc.assume_timezone(times, NAIVE_TIMESTAMP_TZ,
                                       ambiguous="latest", nonexistent="latest")
        return times.cast(arrow_type)

    # Numbers go through float first, so "5", 5 and 5.0 all work, and NaN becomes null.
    numbers = column.cast(pa.float64(), safe=False)
    numbers = pc.if_else(pc.is_nan(numbers), pa.scalar(None, pa.float64()), numbers)
    if value_type == "double":
        return numbers
    # trunc() cuts off like int() does. The cast still fails if a number is too big.
    whole = pc.trunc(numbers).cast(arrow_type)
    return pc.multiply(whole, 1000) if value_type == "unix_seconds" else whole


def convert_batch(batch, mapping: Dict[str, Tuple[str, str]]) -> List[dict]:
    """One batch from a file -> Avro records, with every field of the schema."""
    available = set(batch.schema.names)
    columns = {field: pa.nulls(batch.num_rows, TYPES[value_type][1])
               for field, value_type in dict(mapping.values()).items()}
    for source, (field, value_type) in mapping.items():
        if source in available:
            columns[field] = convert_column(batch.column(source), value_type)
    return pa.table(columns).to_pylist()


# FINDING AND READING THE FILES

def list_taxi_files() -> List[Path]:
    return sorted(TAXI_DATA_DIR.glob("*.parquet"))


def list_flight_files() -> List[Path]:
    return sorted(FLIGHTS_DATA_DIR.rglob("*.csv"))


def read_taxi_columns(path: Path) -> List[str]:
    return pq.read_schema(path).names


def read_flight_columns(path: Path) -> List[str]:
    with pa_csv.open_csv(path) as reader:
        return reader.schema.names


def iter_taxi_batches(path: Path) -> Iterator[pa.RecordBatch]:
    with pq.ParquetFile(path) as parquet_file:
        wanted = [c for c in parquet_file.schema_arrow.names if c in TAXI_COLUMN_MAPPING]
        yield from parquet_file.iter_batches(batch_size=READ_BATCH_SIZE, columns=wanted)


def iter_flight_batches(path: Path) -> Iterator[pa.RecordBatch]:
    # Everything is read as text (like dtype=str in initial_load.py), empty = null.
    options = pa_csv.ConvertOptions(
        column_types={column: pa.string() for column in FLIGHTS_COLUMN_MAPPING},
        null_values=[""], strings_can_be_null=True, quoted_strings_can_be_null=True)
    with pa_csv.open_csv(path, convert_options=options) as reader:
        yield from reader


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
        mapping=TAXI_COLUMN_MAPPING,
        schema=build_schema("TaxiRide", TAXI_COLUMN_MAPPING),
        required_columns=("tpep_pickup_datetime", "tpep_dropoff_datetime"),
        list_files=list_taxi_files,
        read_columns=read_taxi_columns,
        iter_batches=iter_taxi_batches,
    ),
    "flights": DatasetSpec(
        name="flights",
        input_dir=FLIGHTS_DATA_DIR,
        mapping=FLIGHTS_COLUMN_MAPPING,
        schema=build_schema("FlightArrival", FLIGHTS_COLUMN_MAPPING),
        required_columns=("icao24", "lastSeen"),
        list_files=list_flight_files,
        read_columns=read_flight_columns,
        iter_batches=iter_flight_batches,
    ),
}


def check_files(spec: DatasetSpec, files: List[Path]) -> None:
    """Look at the columns of every file BEFORE we start converting."""
    for path in files:
        columns = set(spec.read_columns(path))
        missing = [c for c in spec.required_columns if c not in columns]
        if missing:
            raise ValueError(f"{path.name} has no {missing}, can't convert this layout.")
        unknown = sorted(c for c in columns - set(spec.mapping)
                         if not c.startswith("__index_level_"))
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
