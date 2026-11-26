# convert_to_avro.py

Converts the taxi Parquet files and the flight CSV files to Avro files, with the column names and types of `initial_load.py`. Both scripts get them from the common part, `../common/shared.py` (mappings, conversion, row keys, file listing), so keep the `common` folder next to `converter`.

It only converts. It doesn't send anything to Kafka and doesn't simulate streaming.

## Setup

Use Python 3.14 from python.org (MSYS2's Python can't install these packages). Run everything from this folder.

```bash
py -3.14 -m venv .venv             # Linux / macOS: python3 -m venv .venv
.venv\Scripts\Activate.ps1         # Linux / macOS: source .venv/bin/activate
pip install -r requirements.txt
```

Flights are already in the repo. For taxi, put the [TLC yellow taxi files](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) in `converter/data/taxi/`.

## How to run it

```bash
python convert_to_avro.py --dataset flights               # a few seconds
python convert_to_avro.py --dataset taxi --limit-files 1  # under a minute
python convert_to_avro.py                                 # both
```

It converts every file in the input folders and replaces earlier output. One month of taxi takes about 45 seconds, 400 MB of memory and 150 MB of disk.

## Output

One Avro file per input file, with the schema inside:

```
converter/data/avro/
  taxi/yellow_tripdata_2025-01.avro
  flights/airport=KJFK/year=2025/month=01/2025-01-01_2025-01-02.avro
```

- **Rows:** all of them, in the order of the file. Nothing is sorted, filtered or deduplicated.
- **Fields:** named like the HBase columns, all nullable. Every file of a dataset has the same schema.
- **Times:** milliseconds (`timestamp-millis`), all real UTC. Taxi times are New York wall clock in the files and get converted (18:00 New York = 23:00 UTC in winter, 22:00 in summer), same as the initial load, so they line up with the flights.
- **Row key:** not a field. The Kafka key has to be `shared.taxi_row_key(pickup_ts, file name, row number)` (row number = position in the original monthly file, from 0), sent as an Avro string, so the HBase sink builds exactly the same row key as the initial load. Flights: `shared.flight_row_keys()`.
- **Taxi years:** 2011 and later. 2009/2010 have other layouts and stop the script with an error.
- **Newer columns:** `Airport_fee` (2023-02+, stored as `airport_fee`), `cbd_congestion_fee` (2025-01+) and `request_source` (2026-06+) are included. They're in the shared mapping, so the initial load has them too (its 2011-01 to 2020-12 files just don't contain them).

## Settings

| Environment variable | Default |
|---|---|
| `TAXI_DATA_DIR` | `converter/data/taxi` |
| `FLIGHTS_DATA_DIR` | `download_flight_data/data/opensky_arrivals` |
| `OUTPUT_DIR` | `converter/data/avro` |
| `TAXI_SOURCE_TZ` | `America/New_York` (in `shared.py`): the timezone the taxi times are written in, they get converted to UTC. Replaces `NAIVE_TIMESTAMP_TZ` |
| `READ_BATCH_SIZE` | `50000` rows (in `shared.py`) |

## Tests

```bash
pip install pandas
python -m unittest -v test_convert_to_avro
```

They compare the output with the bytes `initial_load.py` writes into HBase (that test is skipped if the script isn't in `loadscripts/` or the repo root, or set `INITIAL_LOAD_PATH`), check every flight against its CSV, and check the New York -> UTC conversion around the daylight saving changes.
