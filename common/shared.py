"""
shared.py  (the common part of initial_load.py and convert_to_avro.py)

Both scripts read the same taxi / flight files and have to turn them into
EXACTLY the same values:
  - initial_load.py writes the historical taxis (2011-01 to 2022-10) straight into HBase
  - convert_to_avro.py makes the Avro for the live part (2022-11 on), which goes
    Kafka -> Kafka Connect HBase sink -> the same HBase table
If they did it differently, Spark would see two formats in one table.

So everything that decides what a value looks like is in this file, once:
which file column becomes which field (field name = HBase column = Avro
field) and its type, converting whole columns to those types (taxi times
become real UTC), the row keys, and finding / reading the files.
Each script then stores the values its own way: initial_load.py as HBase
bytes, convert_to_avro.py as Avro records.

Both scripts import it like this. It works from any folder, and also in
Kubernetes, where all the files are in one folder:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))
    import shared
"""

import os

import re

from pathlib import Path

from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq


# CONFIGURATION

# The timezone the taxi times are WRITTEN in. The TLC files have New York wall
# clock times with no timezone ("2015-01-01 00:11:33"). We decided to store
# real UTC, so we read them as New York time and convert:
#   18:00 New York = 23:00 UTC in winter, 22:00 UTC in summer.
# That way taxi times line up with the flight times, which are UTC already.
# "UTC" would store the wall clock as if it was UTC (what we did before).
# This replaces NAIVE_TIMESTAMP_TZ, which was confusing: to get real UTC you
# had to set it to America/New_York.
TAXI_SOURCE_TZ = os.environ.get("TAXI_SOURCE_TZ", "America/New_York")

# How many rows we read from a file at once. 50k taxi rows is a few tens of MB.
READ_BATCH_SIZE = int(os.environ.get("READ_BATCH_SIZE", "50000"))


# SCHEMA: WHICH COLUMNS BECOME WHICH FIELDS
# column in the file -> (field = HBase column = Avro field, type)
# Only the 2011+ taxi layout (tpep_*, zone IDs). We dropped 2009/2010 (team
# decision), check_columns() rejects their files.
#
# Types:
#   "string"       text, UTF-8
#   "int"          4-byte int (the files have int64, or double where values are
#                  missing, but the values are small)
#   "double"       8-byte float
#   "timestamp"    taxi time, converted to real UTC, milliseconds since 1970
#   "unix_seconds" flight time (already UTC), stored as milliseconds too
#
# CHECKED the columns of every TLC file from 2011-01 to 2026-09:
#   2011-01 to 2023-01: these 19 columns, the last one is airport_fee
#   2023-02 to 2024-12: the same, but spelled Airport_fee (capital A)
#   2025-01 to 2026-05: + cbd_congestion_fee
#   2026-06 to 2026-09: + request_source
# So all historical months (2011-01 to 2022-10) have exactly the first 19, and
# the three marked NEW only show up in the live streaming. If a file doesn't
# have a column, its rides just get no cell / null for it.
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

# Without these a taxi file has another layout (2009/2010) and is rejected.
TAXI_REQUIRED_COLUMNS = ("tpep_pickup_datetime", "tpep_dropoff_datetime")

# OpenSky arrivals at JFK (download_flight_data/). All 14 CSV columns.
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

FLIGHTS_REQUIRED_COLUMNS = ("icao24", "lastSeen")

# our type -> (Avro type, Arrow type we convert the column to)
TYPES = {
    "string":       ("string", pa.string()),
    "int":          ("int", pa.int32()),
    "double":       ("double", pa.float64()),
    "timestamp":    ({"type": "long", "logicalType": "timestamp-millis"}, pa.int64()),
    "unix_seconds": ({"type": "long", "logicalType": "timestamp-millis"}, pa.int64()),
}


def fields(mapping: Dict[str, Tuple[str, str]]) -> Dict[str, str]:
    """field -> type, every field once (airport_fee is in the mapping twice)."""
    return dict(mapping.values())


def build_avro_schema(name: str, mapping: Dict[str, Tuple[str, str]]) -> dict:
    """The Avro schema of one dataset, made from its mapping so they can't differ.
    Every field may be null, that way one schema fits all the years."""
    avro_fields = [{"name": field, "type": ["null", TYPES[value_type][0]], "default": None}
                   for field, value_type in fields(mapping).items()]
    return {"type": "record", "name": name, "namespace": "bigdata_gaming", "fields": avro_fields}


# CONVERTING COLUMNS
# Whole columns at once with pyarrow.compute, much faster than one value at a time.

def convert_column(column, value_type: str):
    """One column from a file -> the Arrow type of its field (see TYPES)."""
    arrow_type = TYPES[value_type][1]
    if pa.types.is_null(column.type):       # completely empty, e.g. airport_fee in 2016
        return pa.nulls(len(column), arrow_type)
    if value_type == "string":
        return column.cast(arrow_type)

    if value_type == "timestamp":
        times = column.cast(pa.timestamp("ms"), safe=False)
        if TAXI_SOURCE_TZ != "UTC":
            # Read the wall clock as New York time, which gives the real moment.
            # Daylight saving:
            #   - the hour that happens twice (clocks go back, e.g. 01:30 on
            #     2015-11-01): we take the second one ("latest")
            #   - the hour that doesn't exist (clocks go forward, e.g. 02:30 on
            #     2015-03-08): moved to 03:00 ("latest")
            # Same as the pandas version we had before (ambiguous=False,
            # nonexistent="shift_forward"), the converter's tests check it.
            times = pc.assume_timezone(times, TAXI_SOURCE_TZ,
                                       ambiguous="latest", nonexistent="latest")
        # int64 of a timestamp = milliseconds since 1970 in UTC.
        return times.cast(arrow_type)

    # Numbers go through float first, so "5", 5 and 5.0 all work, and NaN becomes null.
    numbers = column.cast(pa.float64(), safe=False)
    numbers = pc.if_else(pc.is_nan(numbers), pa.scalar(None, pa.float64()), numbers)
    if value_type == "double":
        return numbers
    # trunc() cuts off like int() does. The cast still fails if a number is too big.
    whole = pc.trunc(numbers).cast(arrow_type)
    return pc.multiply(whole, 1000) if value_type == "unix_seconds" else whole


def convert_batch(batch, mapping: Dict[str, Tuple[str, str]]) -> pa.Table:
    """One batch from a file -> a table with EVERY field of the mapping
    (null where the file doesn't have the column)."""
    available = set(batch.schema.names)
    columns = {field: pa.nulls(batch.num_rows, TYPES[value_type][1])
               for field, value_type in fields(mapping).items()}
    for source, (field, value_type) in mapping.items():
        if source in available:
            columns[field] = convert_column(batch.column(source), value_type)
    return pa.table(columns)


def check_timezone_support() -> None:
    """Stop right away, and not after hours of loading, if this machine can't
    do the timezone conversion. pyarrow needs the system's timezone database
    (/usr/share/zoneinfo), and slim Docker images don't always have it."""
    if TAXI_SOURCE_TZ == "UTC":
        return
    try:
        converted = convert_column(pa.array([0], pa.timestamp("ms")), "timestamp")
    except Exception as error:   # pyarrow raises different errors per platform
        raise RuntimeError(
            f"Can't convert times from {TAXI_SOURCE_TZ}: {error}. Install the "
            f"timezone database (e.g. 'apt-get install tzdata' in Docker).") from error
    if TAXI_SOURCE_TZ == "America/New_York" and converted[0].as_py() != 5 * 3600 * 1000:
        # 1970-01-01 00:00 in New York = 05:00 UTC
        raise RuntimeError("Timezone conversion gives a wrong result, check the tz database.")


# ROW KEYS
# HBase sorts rows by key and the key is the only index, so it decides what we
# can query fast. Team decision: time first.
#
# Taxi:    <pickup time UTC>|<file name>|<row number>
#          e.g. 2015-01-01T05:11:33|yellow_tripdata_2015-01|000000042
#   - pickup time first: rides are sorted by time, "all rides between X and Y"
#     is one range scan. It's the CONVERTED time (real UTC), same as pickup_ts.
#   - file name + row number: the taxi data has no trip ID, so this makes the
#     key unique AND the same on every run (a rerun overwrites, no duplicates).
#     The row number is the position in the original monthly file, counted
#     from 0, padded to 9 digits so the keys sort right as text.
#   - no salt: we only have 1 RegionServer, so spreading the writes wouldn't help.
# The streaming script MUST send exactly this as its Kafka key (as an Avro
# string, not a record), otherwise the sink builds a different row key.
#
# Flights: <lastSeen UTC>|<icao24>   e.g. 2025-01-02T23:57:59|a1a3cc
#   (plane + landing time). CHECKED on all 702,396 flights (2022-11 to 2025-12):
#   the only repeats are 9 flights that are twice in the same file
#   (2024-03-07_2024-03-08.csv) with every column equal, so they just overwrite
#   themselves.

KEY_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


def format_key_times(millis) -> List[str]:
    """UTC milliseconds (Arrow int64 array) -> "2015-01-01T05:11:33" texts,
    "unknown" where the time is missing."""
    # A timestamp WITHOUT a timezone is printed as it is, and our numbers are
    # already UTC, so no timezone database is needed here. Cast to whole
    # seconds first, otherwise %S prints ".000" too.
    times = pa.array(millis, pa.int64()).cast(pa.timestamp("ms")).cast(pa.timestamp("s"), safe=False)
    texts = pc.strftime(times, format=KEY_TIME_FORMAT)
    return pc.fill_null(texts, "unknown").to_pylist()


def taxi_row_keys(pickup_ms, source_file: str, first_row_number: int) -> List[str]:
    """Row keys for a batch of rides.
    pickup_ms:        the converted pickup_ts column (UTC milliseconds)
    source_file:      file name without .parquet, e.g. "yellow_tripdata_2015-01"
    first_row_number: row number (in the whole file) of the first ride"""
    return [f"{time}|{source_file}|{first_row_number + i:09d}"
            for i, time in enumerate(format_key_times(pickup_ms))]


def taxi_row_key(pickup_ms: Optional[int], source_file: str, row_number: int) -> str:
    """Same as taxi_row_keys(), for one ride (e.g. one Kafka message)."""
    return taxi_row_keys([pickup_ms], source_file, row_number)[0]


def flight_row_keys(last_seen_ms, icao24) -> List[str]:
    """Row keys for a batch of flights (converted last_seen_ts + icao24 columns)."""
    planes = pa.array(icao24, pa.string()).to_pylist()
    return [f"{time}|{plane}" for time, plane in zip(format_key_times(last_seen_ms), planes)]


# FINDING THE FILES

# "yellow_tripdata_2015-01.parquet" -> year 2015, month 01
TAXI_FILENAME_PATTERN = re.compile(r"_(\d{4})-(\d{2})\.parquet$")
# "2025-01-01_2025-01-02.csv" (first and last day in the file) -> month of the first day
FLIGHTS_FILENAME_PATTERN = re.compile(r"^(\d{4})-(\d{2})-\d{2}_\d{4}-\d{2}-\d{2}\.csv$")


def taxi_file_name(year: int, month: int) -> str:
    """The TLC name of one month, e.g. yellow_tripdata_2015-01.parquet."""
    return f"yellow_tripdata_{year:04d}-{month:02d}.parquet"


def parse_month(text: str) -> Tuple[int, int]:
    """ "2022-10" -> (2022, 10). Raises on anything else, so a typo in a
    setting stops the script instead of silently loading the wrong months."""
    match = re.fullmatch(r"(\d{4})-(\d{2})", text.strip())
    if not match or not 1 <= int(match.group(2)) <= 12:
        raise ValueError(f"'{text}' is not a month like 2022-10")
    return int(match.group(1)), int(match.group(2))


def months_between(first_month: str, last_month: str) -> List[Tuple[int, int]]:
    """All months from first_month to last_month, both included."""
    year, month = parse_month(first_month)
    end = parse_month(last_month)
    months = []
    while (year, month) <= end:
        months.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


def _in_range(year: int, month: int, first_month: Optional[str], last_month: Optional[str]) -> bool:
    return ((first_month is None or (year, month) >= parse_month(first_month))
            and (last_month is None or (year, month) <= parse_month(last_month)))


def list_taxi_files(folder: Path, first_month: Optional[str] = None,
                    last_month: Optional[str] = None) -> List[Path]:
    """The monthly taxi files in a folder, sorted, optionally only the months
    first_month..last_month (e.g. "2011-01", "2022-10"; the month is taken
    from the file name, no need to open the file)."""
    selected = []
    for path in sorted(Path(folder).glob("*.parquet")):
        match = TAXI_FILENAME_PATTERN.search(path.name)
        if match and _in_range(int(match.group(1)), int(match.group(2)), first_month, last_month):
            selected.append(path)
    return selected


def list_flight_files(folder: Path, first_month: Optional[str] = None,
                      last_month: Optional[str] = None) -> List[Path]:
    """The flight CSVs in all subfolders (airport=KJFK/year=2025/month=01/...),
    optionally only first_month..last_month (by the first day in the file name)."""
    selected = []
    for path in sorted(Path(folder).rglob("*.csv")):
        match = FLIGHTS_FILENAME_PATTERN.match(path.name)
        if match and _in_range(int(match.group(1)), int(match.group(2)), first_month, last_month):
            selected.append(path)
    return selected


# READING THE FILES IN CHUNKS
# Generators: they give one batch at a time, so memory stays low.

def read_taxi_columns(path: Path) -> List[str]:
    return pq.read_schema(path).names


def read_flight_columns(path: Path) -> List[str]:
    with pa_csv.open_csv(path) as reader:
        return reader.schema.names


def iter_taxi_batches(path: Path) -> Iterator[pa.RecordBatch]:
    """Batches of READ_BATCH_SIZE rides, only the columns we map (Parquet
    stores data per column, so the others aren't even read from disk)."""
    with pq.ParquetFile(path) as parquet_file:
        wanted = [c for c in parquet_file.schema_arrow.names if c in TAXI_COLUMN_MAPPING]
        yield from parquet_file.iter_batches(batch_size=READ_BATCH_SIZE, columns=wanted)


def iter_flight_batches(path: Path) -> Iterator[pa.RecordBatch]:
    # Everything is read as text first, empty = null; convert_column() then
    # turns it into the right type.
    options = pa_csv.ConvertOptions(
        column_types={column: pa.string() for column in FLIGHTS_COLUMN_MAPPING},
        null_values=[""], strings_can_be_null=True, quoted_strings_can_be_null=True)
    with pa_csv.open_csv(path, convert_options=options) as reader:
        yield from reader


def check_columns(file_name: str, columns: Sequence[str], mapping: Dict[str, Tuple[str, str]],
                  required: Sequence[str]) -> List[str]:
    """Raise if a file has another layout (a required column is missing).
    Returns the columns we DON'T convert, so the scripts can warn about them
    (we decided not to drop anything). __index_level_* is a pandas index TLC
    saved into some files by accident, it's not taxi data."""
    missing = [c for c in required if c not in columns]
    if missing:
        raise ValueError(f"{file_name} has no {missing}, can't convert this layout "
                         f"(2009/2010 files aren't supported anymore).")
    return sorted(c for c in set(columns) - set(mapping) if not c.startswith("__index_level_"))
