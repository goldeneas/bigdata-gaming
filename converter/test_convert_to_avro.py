"""
Tests for convert_to_avro.py

    pip install -r requirements.txt pandas
    python -m unittest -v test_convert_to_avro

They convert real rows, read the Avro back and compare it to initial_load.py
(set INITIAL_LOAD_PATH if it's somewhere else). No HBase needed. The taxi test
uses the files in TAXI_DATA_DIR and is skipped if there are none.
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

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def load_initial_load():
    """Import initial_load.py without needing happybase or an HBase."""
    root = Path(__file__).resolve().parent.parent
    places = [root / "initial_load.py", root / "loadscripts" / "initial_load.py"]
    if os.environ.get("INITIAL_LOAD_PATH"):
        places.insert(0, Path(os.environ["INITIAL_LOAD_PATH"]))
    path = next((p for p in places if p.is_file()), None)
    if path is None:
        return None

    fakes = {name: types.ModuleType(name)
             for name in ("happybase", "thriftpy2", "thriftpy2.transport", "Hbase_thrift")}
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
    def check(self, spec, source, rows):
        field_types = dict(spec.mapping.values())
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "out.avro"
            converter.convert_file(spec, source, target)
            with target.open("rb") as avro_file:
                for record, row in zip(fastavro.reader(avro_file), rows, strict=True):
                    actual = {f"f:{name}".encode(): to_bytes(record[name], value_type)
                              for name, value_type in field_types.items()
                              if record[name] is not None}
                    self.assertEqual(actual, reference.build_cells(row, spec.mapping, "f"))

    def test_mappings(self):
        for ours, theirs in ((converter.TAXI_COLUMN_MAPPING, reference.TAXI_COLUMN_MAPPING),
                             (converter.FLIGHTS_COLUMN_MAPPING, reference.FLIGHTS_COLUMN_MAPPING)):
            for column, target in theirs.items():
                self.assertEqual(ours[column], target)

    def test_every_real_flight(self):
        spec = converter.DATASETS["flights"]
        files = spec.list_files()
        if not files:
            self.skipTest("no flight files in FLIGHTS_DATA_DIR")
        for path in files:
            self.check(spec, path, reference.iter_flight_rows(path, spec.mapping))

    def test_real_taxi_rows(self):
        spec = converter.DATASETS["taxi"]
        files = spec.list_files()
        if not files:
            self.skipTest("no taxi files in TAXI_DATA_DIR")
        for path in files:
            with self.subTest(path.name), tempfile.TemporaryDirectory() as folder:
                # Start and end of the file: the rows with nulls are at the end.
                table = pq.read_table(path)
                sample = pa.concat_tables([table.slice(0, 3000),
                                           table.slice(max(0, table.num_rows - 3000))])
                copy = Path(folder) / path.name
                pq.write_table(sample, copy)
                self.check(spec, copy, sample.to_pylist())

    def test_every_second_around_the_clock_changes(self):
        tz = "America/New_York"
        with mock.patch.object(converter, "NAIVE_TIMESTAMP_TZ", tz):
            for day in ("2024-03-10", "2024-11-03"):
                wall = pd.date_range(f"{day} 00:00:00", f"{day} 04:00:00", freq="s")
                ours = converter.convert_column(pa.array(wall), "timestamp").to_pylist()
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
            converter.convert_column(pa.array([2 ** 40]), "int")

    def test_another_layout_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "old.parquet"
            pq.write_table(pa.table({"Trip_Pickup_DateTime": ["2009-01-01"]}), path)
            with self.assertRaises(ValueError):
                converter.check_files(converter.DATASETS["taxi"], [path])


if __name__ == "__main__":
    unittest.main()
