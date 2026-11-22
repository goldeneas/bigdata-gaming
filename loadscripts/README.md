# initial_load.py

Prototype of the "Python Script, Initial Load Historical Data" box in our architecture diagram. It takes the historical taxi files from disk and writes them straight into HBase, so only the newer data has to go through Kafka.

Status: works locally on small amounts of taxi data. Not tested on the cluster yet, and full months of taxi data broke the local HBase (see "Known problems"). The script also has a first version of flight support, but that part I haven't tested yet.

## What it does

1. Finds the historical taxi Parquet files (one per month) and keeps only the years we count as "historical".
2. Reads them in chunks, so memory stays low no matter how big a file is.
3. Turns every ride into an HBase row (row key + cells).
4. Writes the rows in batches to HBase through the HBase Thrift server (Python library `happybase`).
5. Remembers which files are done (progress file), so a rerun skips them.

The rows have to look exactly like the rows the Kafka Connect HBase sink writes for the streamed data (`config/kafka2hbase-connector.yaml`): same tables, same column family, same column names, same row keys, same byte encoding. Otherwise Spark sees two formats. Most choices in the script come from how that sink works.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

The script needs an HBase with a Thrift server running (default `localhost:9090`). For local testing:

```bash
docker run -d --name hbase -h hbase -p 9090:9090 -p 16010:16010 harisekhon/hbase
```

Wait about a minute, then check with `python -c "import happybase; print(happybase.Connection('localhost').tables())"`. The HBase web UI is on http://localhost:16010.

## Data it expects

Taxi: the NYC TLC yellow taxi files with their original names, e.g. `data/taxi/yellow_tripdata_2015-01.parquet`. The year is taken from the file name. There are 3 file layouts in 2009-2015 and the script detects them by their pickup column:

| Years | Layout |
|---|---|
| 2009 | `Trip_Pickup_DateTime`, coordinates, times as text, CamelCase names |
| 2010 | `pickup_datetime`, coordinates, times as text, lowercase names |
| 2011+ | `tpep_pickup_datetime`, zone IDs, real timestamps (same as today's files) |

A file with an unknown layout stops the script with an error instead of being skipped.

## How to run it

```bash
# Dry run: no HBase needed, prints the first rows exactly as they would be written
python initial_load.py --dataset taxi --dry-run --dry-run-rows 3

# Small real load: first 10,000 rows of every file
python initial_load.py --dataset taxi --limit-rows 10000

# Full load: all historical taxi files
python initial_load.py --dataset taxi
```

Options: `--dataset taxi`, `--dry-run`, `--dry-run-rows N`, `--limit-files N` (first N files), `--limit-rows N` (first N rows per file; those files are not marked as done).

## Configuration

Everything is an environment variable with a default, so in Kubernetes it can come from a ConfigMap.

| Variable | Default | What |
|---|---|---|
| `HBASE_THRIFT_HOST` / `HBASE_THRIFT_PORT` | `localhost` / `9090` | Thrift server |
| `HBASE_TRANSPORT` / `HBASE_PROTOCOL` | `buffered` / `binary` | must match how Thrift was started |
| `TAXI_TABLE` | `taxis` | same as the sink (`${topic}`) |
| `TAXI_DATA_DIR` | `./data/taxi` | input folder |
| `HISTORICAL_FIRST_YEAR` / `HISTORICAL_LAST_YEAR` | `2009` / `2015` | taxi years this script loads |
| `READ_BATCH_SIZE` / `WRITE_BATCH_SIZE` | `50000` / `5000` | rows per read / rows per HBase call |
| `NUM_SALT_BUCKETS` | `0` (off) | optional salt prefix for taxi row keys |
| `NAIVE_TIMESTAMP_TZ` | `UTC` | how to read the taxi times, which have no timezone |
| `PROGRESS_FILE` | `./initial_load_progress.json` | finished files (needs a persistent volume in Kubernetes) |
| `MAX_RETRIES` / `RETRY_WAIT_SECONDS` | `6` / `15` | failures in a row before giving up / base wait |
| `HBASE_TIMEOUT_SECONDS` | `300` | max time for one HBase call |
| `PROGRESS_EVERY_ROWS` | `500000` | "rows so far" log line |

## How the data is stored in HBase

- Table `taxis` with one column family, also named `taxis`. That's what the sink does with a flat Avro schema.
- All columns of every file are loaded, nothing is dropped (team decision). Same meaning across years = same HBase column, so Spark reads them the same way. Different kind of value = its own column, stored as it is (e.g. 2009/2010 coordinates `pu_lon`/`pu_lat`, text payment types in `payment_type_text`). Zone mapping and cleaning happen later in Spark.
- Values are raw bytes, encoded like Java's HBase `Bytes.toBytes` (what the sink uses): ints 4 bytes, doubles 8 bytes, text UTF-8, timestamps as milliseconds since 1970 (8 bytes). In the HBase shell numbers look like `\x00\x00\x00\x8E`, that's normal.
- Taxi row key: `<pickup time>|<file name>|<row number>`, e.g. `2015-01-01T00:11:33|yellow_tripdata_2015-01|000000000`. The taxi data has no trip ID, so file + row number make the key unique, and it's the same on every run, so a rerun overwrites instead of duplicating.
- The streaming script has to produce the same keys as Kafka message keys and use the same Avro field names and types.

## Retries and resuming

- Every batch is only counted as done after HBase confirms it. If HBase fails or times out, the script waits (15 s, 30 s, 45 s...), reconnects and continues the file from the last confirmed batch.
- It gives up after 6 failures in a row with no progress in between, and exits with code 1 (a Kubernetes Job shows as failed).
- Finished files are saved in the progress file and skipped on the next run. A file that was stopped halfway is loaded again from the start on the next run (safe, same keys).
- If you wipe the HBase table, also delete the progress file.

## What I tried so far

- Checked the taxi files of every month 2009-2015: 3 layouts, all mapped. Checked the values of the text columns in 2009/2010 (vendor codes, payment types, rate codes, store-and-forward flags).
- Dry run on 2009-01, 2010-01 and 2015-01: right layout per file, decoded values match the source files.
- Real load into a local Docker HBase: 10,000 rows from each layout, read back and decoded correctly. Running it again overwrites (still 30,000 rows), so the row keys are stable.
- Speed locally: ~4,300 rows/s for 2009/2010 (text timestamps), ~9,000 rows/s for 2015 once warmed up.
- The resume / retry logic was rebuilt after the full-month tests below (timeout above HBase's 90 s write block, continue from the last confirmed batch, retries count failures in a row).

## Known problems

- Full months break the local HBase. Loading all of 2015-01 (12.7M rows) failed twice at around 6M rows. HBase's log showed "too many store files; delaying flush up to 90000 ms" (it blocks all writes while it merges files), and in the end "No space left on device": My Docker's disk was full. HBase needs a lot more disk while writing than the final table size (write-ahead log, compactions, old files waiting to be cleaned).
- Storage: rough estimate ~1.5 KB per ride in HBase without compression, so ~20 GB per month and ~1.7 TB for 2009-2015. Not measured cleanly yet.
- The cluster HBase config (branch `feat/prepare-hbase-config-files`) has no Thrift server (only REST), 1 RegionServer with 512 MB heap and ~1.5 Gi of storage. The script can't connect to it as it is, and it's much smaller than the local HBase that already failed.
- Taxi times are New York local time stored "as if UTC". Other data in real UTC (e.g. flights) won't line up with them when joined by time.
- A file that was stopped halfway restarts from row 0 on the next run.

## Still to do

Decisions for all of us:
- Make sure the cluster HBase has enough resources for the full load: disk (HBase needs several times the final table size while writing), RegionServer memory, and maybe table compression.
- Timezone of the taxi times (`NAIVE_TIMESTAMP_TZ=America/New_York` would store real moments).
- Final row keys, column names and types, together with the Avro schemas of the streaming scripts.
- Table compression / encoding, and who creates the tables (this script or an HBase shell script).

Work:
- Add a Thrift server to the cluster HBase config.
- Flights: the script has a first version of flight support (OpenSky arrivals at JFK), not tested yet.
- Test resume and HBase-down behaviour on a fresh HBase with a smaller file (1M rows).
- Measure the real table size, with and without compression (FAST_DIFF + GZ).
- Save the position inside a half-done file in the progress file, so a rerun doesn't restart it.
- Run it as a Kubernetes Job (Docker image, ConfigMap, volume for the data and the progress file).
