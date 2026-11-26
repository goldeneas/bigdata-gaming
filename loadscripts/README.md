# initial_load.py

Prototype of the "Python Script, Initial Load Historical Data" box in our architecture diagram. It takes the historical taxi files from disk and writes them straight into HBase, so only the newer data has to go through Kafka.

Status: works locally, also on a full month of taxi data (2015-01, 12.7M rows), as long as the HBase table is created with compression first (see "Setup"). Without compression, full months broke the local HBase (see "Known problems"). Now loads only the taxis 2011-01 to 2020-12 (team decision: 10 years of historical data here, 5 years of live streaming from 2021-01), with the conversion shared with the streaming script (`../common/shared.py`) and the times stored as real UTC. Runs on the cluster as a Kubernetes Job (see "Running it on the cluster"): tested there with a test Job on two months, the full load was started on 2026-11-26. Flights aren't loaded by this script.

## What it does

1. Finds the historical taxi Parquet files (one per month) and keeps only the months we count as "historical" (2011-01 to 2020-12). On the cluster it downloads them from TLC itself, one month at a time.
2. Reads them in chunks, so memory stays low no matter how big a file is.
3. Converts the columns with the common part (`../common/shared.py`), the same code the streaming script uses: same column names and types, taxi times converted from New York time to real UTC, same row keys.
4. Turns every ride into an HBase row (row key + cells) and writes the rows in batches to HBase through the HBase Thrift server (Python library `happybase`).
5. Remembers which files are done and how far it got in a half-done file (progress file), so a rerun skips / continues.

The rows have to look exactly like the rows the Kafka Connect HBase sink writes for the streamed data (`config/kafka2hbase-connector.yaml`): same tables, same column family, same column names, same row keys, same byte encoding. Otherwise Spark sees two formats. Most choices in the script come from how that sink works.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Python 3.9 or newer works (the tests pass on 3.9 and 3.14; the converter's tests need 3.10+, the live streaming side uses 3.14). The script imports `../common/shared.py`, so keep the `common` folder next to `loadscripts`.

The script needs an HBase with a Thrift server running (default `localhost:9090`). For local testing:

```bash
docker run -d --name hbase -h hbase -p 9090:9090 -p 16010:16010 harisekhon/hbase
```

Wait about a minute (longer on Apple Silicon Macs, the image runs emulated there), then check with `python -c "import happybase; print(happybase.Connection('localhost').tables())"`. The HBase web UI is on http://localhost:16010.

Then create the tables with compression BEFORE loading anything:

```bash
docker exec -it hbase hbase shell
```

```
create 'taxis', {NAME => 'taxis', VERSIONS => 1, DATA_BLOCK_ENCODING => 'FAST_DIFF', COMPRESSION => 'GZ'}
create 'flights', {NAME => 'flights', VERSIONS => 1, DATA_BLOCK_ENCODING => 'FAST_DIFF', COMPRESSION => 'GZ'}
```

If a table doesn't exist, the script creates it itself, but through Thrift it can't set FAST_DIFF, so create them in the shell first. (The script only loads `taxis`; `flights` is for the streamed flights, so the Kafka connector doesn't create a plain one.) This is what makes a full month fit: 2015-01 takes ~790 MB like this instead of ~19 GB (see "What I tried so far").

## Data it expects

Taxi: the NYC TLC yellow taxi files with their original names, e.g. `data/taxi/yellow_tripdata_2015-01.parquet`. The month is taken from the file name. Only the 2011+ layout is loaded (`tpep_pickup_datetime`, zone IDs, real timestamps, same as today's files). The 2009/2010 layouts (coordinates, times as text) were dropped by the team: a file with another layout stops the script with an error instead of being skipped.

With `TAXI_DOWNLOAD=1` the script doesn't need the files: it downloads every month of the range from TLC into `TAXI_DATA_DIR`, loads it and deletes it again.

## How to run it

```bash
# Dry run: no HBase needed, prints the first rows exactly as they would be written
python initial_load.py --dry-run --dry-run-rows 3

# Small real load: first 10,000 rows of every file
python initial_load.py --limit-rows 10000

# Full load: all historical taxi files (2011-01 to 2020-12)
python initial_load.py

# Only some months, e.g. 2015
TAXI_FIRST_MONTH=2015-01 TAXI_LAST_MONTH=2015-12 python initial_load.py

# Download the months from TLC instead of reading them from disk
# (with the python.org Python on a Mac, run "Install Certificates.command" in its
#  folder once, otherwise the download fails with CERTIFICATE_VERIFY_FAILED)
TAXI_DOWNLOAD=1 TAXI_DATA_DIR=/tmp/taxi TAXI_FIRST_MONTH=2015-01 TAXI_LAST_MONTH=2015-01 python initial_load.py

# Tests (fake HBase, no Docker needed)
python -m unittest -v test_initial_load
```

Options: `--dry-run`, `--dry-run-rows N`, `--limit-files N` (first N files), `--limit-rows N` (first N rows per file; those files are not marked as done). Files that are already marked as done are skipped even with `--limit-rows`, so for a test on another file use its own `PROGRESS_FILE`. (`--dataset` is gone, the script only loads taxis now.)

## Configuration

Everything is an environment variable with a default. In Kubernetes the Job's YAML sets them.

| Variable | Default | What |
|---|---|---|
| `HBASE_THRIFT_HOST` / `HBASE_THRIFT_PORT` | `localhost` / `9090` | Thrift server |
| `HBASE_TRANSPORT` / `HBASE_PROTOCOL` | `buffered` / `binary` | must match how Thrift was started |
| `TAXI_TABLE` | `taxis` | same as the sink (`${topic}`) |
| `TAXI_DATA_DIR` | `./data/taxi` | input folder (or where the downloads go) |
| `TAXI_FIRST_MONTH` / `TAXI_LAST_MONTH` | `2011-01` / `2020-12` | taxi months this script loads, both included (replaces `TAXI_FIRST_YEAR` / `TAXI_LAST_YEAR`) |
| `TAXI_DOWNLOAD` | `0` | `1` = download the months from TLC (`TAXI_DOWNLOAD_URL`) |
| `TAXI_SOURCE_TZ` | `America/New_York` | timezone the taxi times are written in; they get converted to real UTC. In `shared.py`, the streaming script uses the same value. Replaces `NAIVE_TIMESTAMP_TZ` |
| `READ_BATCH_SIZE` / `WRITE_BATCH_SIZE` | `50000` / `5000` | rows per read (in `shared.py`) / rows per HBase call |
| `HBASE_WRITE_WAL` | `1` | `0` = write without HBase's write-ahead log (faster, less disk, but a crash loses the last writes silently) |
| `PROGRESS_FILE` | `./initial_load_progress.json` | finished files + position in a half-done file (needs a persistent volume in Kubernetes) |
| `MAX_RETRIES` / `RETRY_WAIT_SECONDS` | `6` / `15` | failures in a row before giving up / base wait |
| `HBASE_TIMEOUT_SECONDS` | `300` | max time for one HBase call |
| `PROGRESS_EVERY_ROWS` | `500000` | "rows so far" log line |

## How the data is stored in HBase

- Table `taxis` with one column family, also named `taxis`. That's what the sink does with a flat Avro schema.
- All columns of every file are loaded, nothing is dropped (team decision). All historical months have the same 19 columns; `Airport_fee` (2023-02+) goes into the same `airport_fee` column as `airport_fee`. Cleaning happens later in Spark.
- Values are raw bytes, encoded like Java's HBase `Bytes.toBytes` (what the sink uses): ints 4 bytes, doubles 8 bytes, text UTF-8, timestamps as milliseconds since 1970 (8 bytes). In the HBase shell numbers look like `\x00\x00\x00\x8E`, that's normal.
- Times: the taxi files have New York wall clock times. They're converted to real UTC (18:00 New York = 23:00 UTC in winter, 22:00 in summer), so they line up with the flight times (team decision).
- Taxi row key (team decision: time first): `<pickup time UTC>|<file name>|<row number>`, e.g. `2015-01-01T05:11:33|yellow_tripdata_2015-01|000000000` (that ride is 00:11:33 New York time). The taxi data has no trip ID, so file + row number make the key unique, and it's the same on every run, so a rerun overwrites instead of duplicating. No salt: with 1 RegionServer it gains nothing.
- Compression: FAST_DIFF stops HBase from storing the whole row key again in every cell, GZ compresses the rest. Without it a ride takes ~1.5 KB on disk, with it ~62 bytes.
- The streaming script has to produce the same keys as Kafka message keys (as an Avro string) and use the same Avro field names and types. Both come from `shared.py` (`taxi_row_key()`, `TAXI_COLUMN_MAPPING`), so the two scripts can't end up different.

## Retries and resuming

- Every batch is only counted as done after HBase confirms it. If HBase fails or times out, the script waits (15 s, 30 s, 45 s...), reconnects and continues the file from the last confirmed batch.
- It gives up after 6 failures in a row with no progress in between, and exits with code 1 (a Kubernetes Job shows as failed).
- Finished files are saved in the progress file and skipped on the next run. The position in a half-done file is saved too (every 500,000 rows, on Ctrl-C, when Kubernetes stops the pod and when the script gives up), so the next run continues from there.
- If you wipe the HBase table, also delete the progress file.

## What I tried so far

- Checked the taxi files of every month 2009-2015: 3 layouts, all mapped. Checked the values of the text columns in 2009/2010 (vendor codes, payment types, rate codes, store-and-forward flags).
- Dry run on 2009-01, 2010-01 and 2015-01: right layout per file, decoded values match the source files.
- Real load into a local Docker HBase: 10,000 rows from each layout, read back and decoded correctly. Running it again overwrites (still 30,000 rows), so the row keys are stable.
- Speed locally: ~4,300 rows/s for 2009/2010 (text timestamps), ~8,000 rows/s for 2015 on the first millions of rows, ~6,000 rows/s on average over a full month (HBase falls behind on merging files now and then).
- The resume / retry logic was rebuilt after the full-month tests below (timeout above HBase's 90 s write block, continue from the last confirmed batch, retries count failures in a row).
- HBase-down test on a fresh HBase with a 1M-row file: stopped HBase in the middle of the load, the script failed at 40,000 confirmed rows, waited, reconnected and continued from row 40,000 until all 1,000,000 rows were in. Count afterwards exactly 1,000,000 (no duplicates).
- Table size measured on 1M rows / a full month: plain table ~1.5 KB per ride (1.5 GB for 1M rides), FAST_DIFF + GZ ~62 bytes per ride (790 MB for all of 2015-01). Compression didn't slow the script down (124 s vs 126 s for 1M rows), HBase does it in the background.
- Full month 2015-01 into the compressed table: all 12,741,035 rows in 35 minutes, no retries, `count 'taxis'` matches the file exactly. HBase still needed up to ~11 GB of extra disk while writing (mostly the write-ahead log, which isn't compressed), and it cleaned that up by itself a few minutes after the load.
- Flights: January 2025 (16 files, 17,976 flights) loaded into the local HBase. `count 'flights'` matches, the stored times decode to the same times as the text columns, and a rerun skips all 16 files. (Flights were removed from the script afterwards.)
- Common part: compared the new column-wise conversion with the old value-by-value one on 200,000 real rides of 2015-01: every cell is byte for byte the same, only the time in the row key is now UTC. Building the rows now runs at ~175,000 rows/s, so HBase is the only thing that limits the speed.
- Fake-HBase tests (`test_initial_load.py`): whole file, HBase failing in the middle (same result as a clean run), giving up + continuing from the saved row, and the bytes are exactly what the Kafka sink writes for the streaming script's Avro.
- Cluster: happybase connects to the cluster's Thrift server through `kubectl port-forward`. Compression there: GZ and SNAPPY work, ZSTD doesn't (their image has no zstd).
- Checked the new version against the old value-by-value code on real months from all over the range (2011-01, 2016-07, 2020-04, 2022-10, plus 2015-01 and 2023-01): 250,000 rides per month, every cell byte for byte the same.
- Local Docker HBase with the new version (2026-11-26): 1M rides of 2015-01 into a FAST_DIFF + GZ table, stopped with SIGTERM at 550,000 rows (position saved), started again (continued from 550,000), HBase restarted in the middle (retried and continued), exactly 1,000,000 rows at the end. Read 3,000 random rows back from HBase and compared every cell with the Parquet file (times as real UTC): all equal. Download mode with `HBASE_WRITE_WAL=0` on 2020-04 (238,073 rows): count matches, the downloaded file was deleted. Speed ~9,000 rows/s.
- The Job's container (same image, script and env as the YAML, with Docker instead of Kubernetes, against the local HBase): installs tzdata + packages, downloads 2020-03 and 2020-04 from TLC, stopped with SIGTERM at 500,000 rows, restarted, continued, 3,245,760 rows in HBase = exactly both months. This test found one bug: Kubernetes/Docker put a variable `HBASE_THRIFT_PORT=tcp://...` into the container for a Service called `hbase-thrift`, which crashed the script. Fixed in the YAML (`enableServiceLinks: false` + `HBASE_THRIFT_PORT=9090`), and the script now stops with a clear message if it happens anyway.
- On the cluster (2026-11-26, test names, everything deleted afterwards): permissions OK, no quotas, pods can reach TLC, Debian and PyPI, server dry run OK. From the laptop through `kubectl port-forward`: 1M rides into a FAST_DIFF + GZ test table at ~5,200 rows/s, read 1,000 rows back, all equal. As a test Job (the real YAML with other names, 2020-03 and 2020-04): ~8,300 rows/s inside the cluster, pod deleted at 740,000 rows (position saved), the new pod continued from there without downloading again, 3,245,760 rows at the end = exactly both months, 1,000 rows read back, all equal. The new pod waited ~8 minutes before it could start because of the node's disk pressure (see "Known problems").

## Known problems

- Full months broke the local HBase with a plain (uncompressed) table. Loading all of 2015-01 (12.7M rows) failed twice at around 6M rows. HBase's log showed "too many store files; delaying flush up to 90000 ms" (it blocks all writes while it merges files), and in the end "No space left on device": My Docker's disk was full. HBase needs a lot more disk while writing than the final table size (write-ahead log, compactions, old files waiting to be cleaned). Fixed by creating the table with FAST_DIFF + GZ (see "Setup").
- Storage: measured ~1.5 KB per ride without compression (~19 GB for a 12.7M-ride month) and ~62 bytes per ride with FAST_DIFF + GZ (~0.8 GB for the same month).
- The cluster HBase config (branch `feat/prepare-hbase-config-files`) had no Thrift server and ~1.5 Gi of storage. Fixed since: Thrift server `hbase-thrift` on port 9090, 200Gi for HDFS. 2011-01 to 2020-12 is 1,288,300,471 rides (counted from the TLC files, 17.2 GB of Parquet), so ~80 GB with FAST_DIFF + GZ, plus temporary space while loading.
- The taxi row key starts with the pickup time, and the files are roughly in time order, so all writes go to the newest region. Locally the table split into a 35 MB and a 753 MB region and the second one got almost all the writes. Doesn't matter with one RegionServer, but with several we'd need a salt prefix in the key (the old `NUM_SALT_BUCKETS` setting was removed, the team decided on time first without salt) or a pre-split table.
- The cluster's node has its main disk 99-100% full (checked 2026-11-26, "disk pressure" switching on and off; while it's on, no new pods can start). That disk holds every pod's own files, so the Job keeps its downloads and Python packages on its own volume (on the node's 6.8 TB data disk, 5.4 TB free), where HDFS/HBase also keep their data. The full main disk is still a problem for the whole cluster and has to go to whoever runs our HBase setup / the course staff.
- The Job needs internet from inside the pod (TLC, Debian packages, PyPI) and has to run as root to install `tzdata` (pyarrow needs the system's timezone database for New York -> UTC, and the `TZDIR` variable doesn't help, tested). Checked on the cluster 2026-11-26: both work.
- Speed: ~8,300 rows/s in the test Job, ~9,300 rows/s in the real one, so roughly 1.5-2 days for 2011-01 to 2020-12 with one Job. Two Jobs on different month ranges could run in parallel (each with its own name, volume and progress file).

## Running it on the cluster (Kubernetes Job)

Everything from the repo root, with `KUBECONFIG` set to our group's kubeconfig (namespace `bd-gr-07`), on the SDU network or VPN.

0. Quick checks that change nothing on the cluster:
   ```bash
   kubectl auth can-i create jobs.batch && kubectl auth can-i create configmaps && kubectl auth can-i create persistentvolumeclaims
   kubectl get resourcequota,limitrange
   kubectl exec deploy/hbase-rest -- curl -sI https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2011-01.parquet | head -1
   kubectl apply --dry-run=server -f loadscripts/k8s/initial-load-job.yaml
   ```
   The three `can-i` should say `yes`. Also check `kubectl describe node leftover | grep Taints`: if it says `disk-pressure`, the Job's pod waits (Pending) until it's gone. The `curl` from inside the `hbase-rest` pod should print `HTTP/... 200`; if it hangs, pods can't reach the internet and the Job can't download the months (or install packages). The server dry run should say `created (server dry run)` twice.
1. Create the table with compression (once):
   ```bash
   kubectl exec -it hbase-master-0 -- hbase shell
   ```
   ```
   create 'taxis', {NAME => 'taxis', VERSIONS => 1, DATA_BLOCK_ENCODING => 'FAST_DIFF', COMPRESSION => 'GZ'}
   create 'flights', {NAME => 'flights', VERSIONS => 1, DATA_BLOCK_ENCODING => 'FAST_DIFF', COMPRESSION => 'GZ'}
   ```
   (`flights` is only for the live streaming, but this way the Kafka connector doesn't create a plain one.)
2. Put the code in a ConfigMap (again after every code change):
   ```bash
   kubectl create configmap initial-load-code --from-file=loadscripts/initial_load.py --from-file=common/shared.py --dry-run=client -o yaml | kubectl apply -f -
   ```
3. Start the Job (it downloads every month from TLC, loads it, deletes it):
   ```bash
   kubectl apply -f loadscripts/k8s/initial-load-job.yaml
   kubectl logs -f job/taxi-initial-load
   ```
4. Progress: the log prints a line every 500,000 rows (the first minute is the pod installing `tzdata` and the Python packages). If the pod dies, Kubernetes starts it again and it continues from the progress file on its volume. To start completely over: `kubectl delete job taxi-initial-load` and `kubectl delete pvc taxi-initial-load-work` (and empty the table).

## Still to do

Decisions for all of us:
- Who creates the tables with compression (FAST_DIFF + GZ). This script can't set FAST_DIFF, so probably an HBase shell script (or the command above).
- Maybe write without the write-ahead log for the initial load (`HBASE_WRITE_WAL=0`): faster and much less temporary disk, but if HBase crashes the last writes are lost without an error, so we'd have to check the counts afterwards.

Work:
- Wait for the Job (2011-01 to 2020-12) to finish and check the count: `count 'taxis'` should be 1,288,300,471.
- Live streaming script: send the Avro from `convert_to_avro.py` to Kafka with `shared.taxi_row_key()` as the key.
