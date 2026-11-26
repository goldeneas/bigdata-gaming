# common/shared.py

The common part of `loadscripts/initial_load.py` (historical taxis 2011-01 to 2020-12 straight into HBase) and `converter/convert_to_avro.py` (Avro for the live part, which goes Kafka -> HBase sink -> the same table). Both have to produce exactly the same values, so everything that decides what a value looks like is here, once:

- `TAXI_COLUMN_MAPPING` / `FLIGHTS_COLUMN_MAPPING`: file column -> (field = HBase column = Avro field, type). Taxi is the 2011+ layout only.
- `convert_batch()` / `convert_column()`: whole columns to those types with pyarrow. Taxi times are converted from New York time to real UTC (`TAXI_SOURCE_TZ`, default `America/New_York`).
- `taxi_row_key()` / `taxi_row_keys()`: `<pickup time UTC>|<file name>|<row number>`, e.g. `2015-01-01T05:11:33|yellow_tripdata_2015-01|000000000`. The streaming script sends this as the Kafka key (Avro string), so the sink builds the same row key. `flight_row_keys()`: `<lastSeen UTC>|<icao24>`.
- `months_between()`, `build_avro_schema()`, `list_taxi_files()`, `list_flight_files()`, `iter_taxi_batches()`, `iter_flight_batches()`, `check_columns()`, `check_timezone_support()`.

How the values are stored is up to each script: `initial_load.py` encodes them as HBase bytes (like Java's `Bytes.toBytes`), `convert_to_avro.py` writes Avro.

Needs `pyarrow` and the system's timezone database (on Linux `/usr/share/zoneinfo`, in a slim Docker image `apt-get install tzdata`). Tested by `loadscripts/test_initial_load.py` and `converter/test_convert_to_avro.py`.
