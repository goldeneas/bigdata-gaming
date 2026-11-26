"""
Tests for initial_load.py, with a fake HBase (no Docker / cluster needed).

    pip install -r requirements.txt
    python -m unittest -v test_initial_load

What they check:
  - a whole file ends up in "HBase" with the right number of rows and unique keys
  - if HBase fails in the middle, the script continues and the result is the
    same as a clean run (no lost and no extra rows)
  - if it gives up, the position is saved and the next run continues from there
  - the bytes are exactly what the Kafka sink would write for the same Avro
    values (so historical and streamed rows look the same)
"""

import json
import os
import struct
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

import initial_load as il

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))
import shared  # noqa: E402

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def make_taxi_file(folder: Path, rows: int, name: str = "yellow_tripdata_2015-03.parquet") -> Path:
    """A small fake TLC file with the 2011+ layout, some missing values and the
    daylight saving night of 2015-03-08 in it."""
    start = datetime(2015, 3, 8, 0, 0, 0)
    pickups = [start + timedelta(seconds=7 * i) for i in range(rows)]
    table = pa.table({
        "VendorID": pa.array([1 + i % 2 for i in range(rows)], pa.int64()),
        "tpep_pickup_datetime": pa.array(pickups, pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array([p + timedelta(minutes=9) for p in pickups], pa.timestamp("us")),
        "passenger_count": pa.array([None if i % 50 == 0 else 1 + i % 4 for i in range(rows)], pa.float64()),
        "trip_distance": pa.array([0.5 + i % 30 for i in range(rows)], pa.float64()),
        "RatecodeID": pa.array([1] * rows, pa.int64()),
        "store_and_fwd_flag": pa.array(["N" if i % 7 else "Y" for i in range(rows)], pa.string()),
        "PULocationID": pa.array([1 + i % 265 for i in range(rows)], pa.int64()),
        "DOLocationID": pa.array([1 + (i * 7) % 265 for i in range(rows)], pa.int64()),
        "payment_type": pa.array([1 + i % 4 for i in range(rows)], pa.int64()),
        "fare_amount": pa.array([float("nan") if i % 99 == 0 else 2.5 + i % 40 for i in range(rows)]),
        "extra": pa.array([0.5] * rows),
        "mta_tax": pa.array([0.5] * rows),
        "tip_amount": pa.array([1.25] * rows),
        "tolls_amount": pa.array([0.0] * rows),
        "improvement_surcharge": pa.array([0.3] * rows),
        "total_amount": pa.array([10.05] * rows),
        "congestion_surcharge": pa.nulls(rows),
        "airport_fee": pa.nulls(rows),
    })
    path = folder / name
    pq.write_table(table, path)
    return path


class FakeTable:
    """Stores puts in a dict like HBase would (same key = overwrite).
    fail_on: which send() calls (1, 2, 3...) raise like a dead Thrift server."""

    def __init__(self, fail_on=()):
        self.rows = {}
        self.sends = 0
        self.fail_on = set(fail_on)

    def batch(self, wal=True):
        table = self

        class Batch:
            def __init__(self):
                self.puts = []

            def put(self, key, cells):
                self.puts.append((key, cells))

            def send(self):
                table.sends += 1
                if table.sends in table.fail_on:
                    raise il.TTransportException(message="fake: HBase is down")
                for key, cells in self.puts:
                    table.rows.setdefault(key, {}).update(cells)

        return Batch()


class FakeConnection:
    def __init__(self, table):
        self._table = table

    def table(self, name):
        return self._table

    def close(self):
        pass

    def open(self):
        pass


class LoadTests(unittest.TestCase):
    def setUp(self):
        self.folder = Path(tempfile.mkdtemp())
        self.path = make_taxi_file(self.folder, 23_456)
        # small batches so a file has many of them; no waiting between retries
        self.patches = [mock.patch.object(il, "WRITE_BATCH_SIZE", 1000),
                        mock.patch.object(il, "PROGRESS_EVERY_ROWS", 5000),
                        mock.patch.object(il, "RETRY_WAIT_SECONDS", 0),
                        mock.patch.object(shared, "READ_BATCH_SIZE", 3000)]
        for patch in self.patches:
            patch.start()

    def tearDown(self):
        for patch in self.patches:
            patch.stop()

    def load(self, table, start_row=0, save_position=None):
        return il.load_file_with_retries(FakeConnection(table), self.path, False, 0, None,
                                         start_row=start_row, save_position=save_position)

    def test_whole_file(self):
        table = FakeTable()
        self.assertEqual(self.load(table), 23_456)
        self.assertEqual(len(table.rows), 23_456)              # unique keys
        keys = sorted(table.rows)
        self.assertEqual(keys[0].decode(), "2015-03-08T05:00:00|yellow_tripdata_2015-03|000000000")

    def test_hbase_fails_in_the_middle(self):
        clean, broken = FakeTable(), FakeTable(fail_on={4, 5, 9})
        self.load(clean)
        self.load(broken)
        self.assertEqual(broken.rows, clean.rows)

    def test_gives_up_saves_position_and_next_run_continues(self):
        clean = FakeTable()
        self.load(clean)
        saved = []
        # sends 1-7 work, then 6 failures in a row -> give up
        first = FakeTable(fail_on=set(range(8, 20)))
        with self.assertRaises(SystemExit):
            self.load(first, save_position=saved.append)
        self.assertEqual(saved[-1], 7000)                      # 7 batches of 1000 confirmed
        second = FakeTable()
        second.rows = dict(first.rows)                          # same HBase, next run
        self.load(second, start_row=saved[-1])
        self.assertEqual(second.rows, clean.rows)
        self.assertEqual(second.sends, 17)                      # only the rest was written

    def test_month_range(self):
        # historical = 2011-01 to 2022-10, the split is in the middle of 2022
        months = shared.months_between("2011-01", "2022-10")
        self.assertEqual((len(months), months[0], months[-1]), (142, (2011, 1), (2022, 10)))
        for name in ("yellow_tripdata_2022-10.parquet", "yellow_tripdata_2022-11.parquet"):
            (self.folder / name).touch()
        self.assertEqual([p.name for p in shared.list_taxi_files(self.folder, "2022-01", "2022-10")],
                         ["yellow_tripdata_2022-10.parquet"])

    def test_progress_file(self):
        progress_path = self.folder / "progress.json"
        progress_path.write_text(json.dumps(["taxi/old.parquet"]))   # old format still works
        progress = il.Progress(progress_path)
        progress.mark_partial("taxi/a.parquet", 500)
        self.assertEqual(il.Progress(progress_path).partial_row("taxi/a.parquet"), 500)
        progress.mark_done("taxi/a.parquet")
        reread = il.Progress(progress_path)
        self.assertTrue(reread.is_done("taxi/a.parquet") and reread.is_done("taxi/old.parquet"))
        self.assertEqual(reread.partial_row("taxi/a.parquet"), 0)


def sink_bytes(value, value_type):
    """What the Kafka Connect HBase sink writes for one Avro value
    (Java Bytes.toBytes): text as UTF-8, numbers big-endian."""
    if value_type == "string":
        return value.encode("utf-8")
    if isinstance(value, datetime):
        value = (value - EPOCH) // timedelta(milliseconds=1)
    return struct.pack({"int": ">i", "double": ">d", "timestamp": ">q", "unix_seconds": ">q"}[value_type], value)


class SameAsTheStreamingPart(unittest.TestCase):
    """The streaming script sends Avro records made by shared.convert_batch;
    the sink turns every field into bytes. Our cells must be exactly that."""

    def check(self, batch, source_file, first_row):
        records = shared.convert_batch(batch, shared.TAXI_COLUMN_MAPPING).to_pylist()
        rows = il.build_rows(batch, source_file, first_row)
        self.assertEqual(len(rows), len(records))
        for (row_number, key, cells), record in zip(rows, records):
            expected = {il.TAXI_QUALIFIERS[field]: sink_bytes(record[field], value_type)
                        for field, value_type in il.TAXI_FIELDS.items() if record[field] is not None}
            self.assertEqual(cells, expected)
            self.assertEqual(key.decode(), shared.taxi_row_key(record["pickup_ts"], source_file, row_number))

    def test_fake_file(self):
        with tempfile.TemporaryDirectory() as folder:
            path = make_taxi_file(Path(folder), 5000)
            self.check(pq.read_table(path).to_batches()[0], path.stem, 0)

    def test_real_files(self):
        folder = Path(os.environ.get("TAXI_DATA_DIR", "./data/taxi"))
        files = shared.list_taxi_files(folder)
        if not files:
            self.skipTest(f"no taxi files in {folder}")
        for path in files:
            with self.subTest(path.name):
                table = pq.read_table(path)
                for start in (0, max(0, table.num_rows - 3000)):
                    batch = table.slice(start, 3000).combine_chunks().to_batches()[0]
                    self.check(batch, path.stem, start)


if __name__ == "__main__":
    unittest.main()
