"""
Tests for convert_to_avro.py

    pip install -r requirements.txt pandas
    python -m unittest -v test_convert_to_avro

They convert real rows, read the Avro back and compare it to the bytes
initial_load.py writes into HBase (set INITIAL_LOAD_PATH if it's somewhere
else). No HBase needed. The taxi test uses the files in TAXI_DATA_DIR and is
skipped if there are none; the flight test uses FLIGHTS_DATA_DIR.
"""

import importlib.util
import os
import struct
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import fastavro
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import convert_to_avro as converter
import shared   # convert_to_avro already put ../common on the path

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def load_initial_load():
    """Import initial_load.py without needing happybase or an HBase."""
    root = Path(__file__).resolve().parent.parent
    places = [root / "loadscripts" / "initial_load.py", root / "initial_load.py"]
    if os.environ.get("INITIAL_LOAD_PATH"):
        places.insert(0, Path(os.environ["INITIAL_LOAD_PATH"]))
    path = next((p for p in places if p.is_file()), None)
    if path is None:
        return None

    fakes = {name: types.ModuleType(name)
             for name in ("happybase", "thriftpy2", "thriftpy2.transport", "Hbase_thrift")}
    # initial_load.py only needs these to talk to HBase, the test doesn't
    fakes["happybase"].Connection = fakes["happybase"].Table = object
    fakes["thriftpy2.transport"].TTransportException = type("TTransportException", (Exception,), {})
    fakes["Hbase_thrift"].IOError = type("IOError", (Exception,), {})

    spec = importlib.util.spec_from_file_location("initial_load_reference", path)
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, fakes):
        sys.modules[spec.name] = module     # dataclasses need this while importing
        spec.loader.exec_module(module)
    return module


reference = load_initial_load()


def to_bytes(value, value_type):
    """An Avro value as the bytes initial_load.py stores."""
    if value_type == "string":
        return value.encode("utf-8")
    if isinstance(value, datetime):
        value = (value - EPOCH) // timedelta(milliseconds=1)
    code = {"int": ">i", "double": ">d", "timestamp": ">q", "unix_seconds": ">q"}[value_type]
    return struct.pack(code, value)


@unittest.skipIf(reference is None, "initial_load.py not found")
class SameAsInitialLoad(unittest.TestCase):
    """The Avro we write goes Kafka -> HBase sink, which turns every field into
    bytes. That has to be byte for byte what initial_load.py writes for the
    same ride, and the Kafka key has to be its row key."""

    def test_real_taxi_rows(self):
        spec = converter.DATASETS["taxi"]
        files = spec.list_files()
        if not files:
            self.skipTest("no taxi files in TAXI_DATA_DIR")
        field_types = shared.fields(spec.mapping)
        for path in files:
            with self.subTest(path.name), tempfile.TemporaryDirectory() as folder:
                # Start and end of the file: the rows with nulls are at the end.
                table = pq.read_table(path)
                start = max(0, table.num_rows - 3000)
                sample = pa.concat_tables([table.slice(0, 3000), table.slice(start)])
                copy = Path(folder) / path.name
                pq.write_table(sample, copy)
                target = Path(folder) / "out.avro"
                converter.convert_file(spec, copy, target)
                # initial_load numbers the rows in the ORIGINAL file
                batch = sample.combine_chunks().to_batches()[0]
                hbase_rows = (reference.build_rows(batch.slice(0, 3000), path.stem, 0)
                              + reference.build_rows(batch.slice(3000), path.stem, start))
                with target.open("rb") as avro_file:
                    for record, (row_number, key, cells) in zip(
                            fastavro.reader(avro_file), hbase_rows, strict=True):
                        actual = {reference.TAXI_QUALIFIERS[name]: to_bytes(record[name], value_type)
                                  for name, value_type in field_types.items()
                                  if record[name] is not None}
                        self.assertEqual(actual, cells)
                        pickup = record["pickup_ts"]
                        pickup_ms = None if pickup is None else (pickup - EPOCH) // timedelta(milliseconds=1)
                        self.assertEqual(shared.taxi_row_key(pickup_ms, path.stem, row_number).encode(), key)


class Flights(unittest.TestCase):
    def test_every_real_flight(self):
        """Every flight, compared with the CSV read as plain text."""
        spec = converter.DATASETS["flights"]
        files = spec.list_files()
        if not files:
            self.skipTest("no flight files in FLIGHTS_DATA_DIR")
        field_types = shared.fields(spec.mapping)
        for path in files:
            with tempfile.TemporaryDirectory() as folder:
                target = Path(folder) / "out.avro"
                converter.convert_file(spec, path, target)
                raw = pd.read_csv(path, dtype=str, keep_default_na=False).to_dict(orient="records")
                with target.open("rb") as avro_file:
                    for record, row in zip(fastavro.reader(avro_file), raw, strict=True):
                        for column, (field, value_type) in spec.mapping.items():
                            text = row[column]
                            if text == "":
                                expected = None
                            elif value_type == "string":
                                expected = text
                            elif value_type == "int":
                                expected = int(float(text))
                            else:   # unix_seconds -> datetime in UTC
                                expected = EPOCH + timedelta(seconds=int(float(text)))
                            self.assertEqual(record[field], expected, (path.name, column))

    def test_flight_key(self):
        keys = shared.flight_row_keys([1735689775000], ["7380c6"])
        self.assertEqual(keys, ["2025-01-01T00:02:55|7380c6"])


class Timezone(unittest.TestCase):
    def test_new_york_becomes_real_utc(self):
        wall = pa.array([datetime(2015, 1, 1, 18, 0), datetime(2015, 7, 1, 18, 0)], pa.timestamp("us"))
        with mock.patch.object(shared, "TAXI_SOURCE_TZ", "America/New_York"):
            ours = shared.convert_column(wall, "timestamp").to_pylist()
        # 18:00 New York = 23:00 UTC in winter, 22:00 UTC in summer
        self.assertEqual(shared.format_key_times(ours), ["2015-01-01T23:00:00", "2015-07-01T22:00:00"])

    def test_every_second_around_the_clock_changes(self):
        tz = "America/New_York"
        with mock.patch.object(shared, "TAXI_SOURCE_TZ", tz):
            for day in ("2024-03-10", "2024-11-03"):
                wall = pd.date_range(f"{day} 00:00:00", f"{day} 04:00:00", freq="s")
                ours = shared.convert_column(pa.array(wall), "timestamp").to_pylist()
                expected = wall.tz_localize(tz, ambiguous=[False] * len(wall),
                                            nonexistent="shift_forward")
                self.assertEqual(ours, [ts.value // 1_000_000 for ts in expected], day)


class Failures(unittest.TestCase):
    def test_a_failed_file_keeps_the_old_output_and_leaves_no_temp_file(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "out.avro"
            target.write_bytes(b"old")
            broken = Path(folder) / "broken.parquet"
            broken.write_bytes(b"not parquet")
            with self.assertRaises(pa.ArrowInvalid):
                converter.convert_file(converter.DATASETS["taxi"], broken, target)
            self.assertEqual(target.read_bytes(), b"old")
            self.assertFalse(target.with_name("out.avro.tmp").exists())

    def test_a_number_that_does_not_fit_is_an_error(self):
        with self.assertRaises(pa.ArrowInvalid):
            shared.convert_column(pa.array([2 ** 40]), "int")

    def test_another_layout_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "old.parquet"
            pq.write_table(pa.table({"Trip_Pickup_DateTime": ["2009-01-01"]}), path)
            with self.assertRaises(ValueError):
                converter.check_files(converter.DATASETS["taxi"], [path])


if __name__ == "__main__":
    unittest.main()
