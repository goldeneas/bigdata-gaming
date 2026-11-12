import subprocess
import sys
import tempfile
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal


SCRIPT = Path(__file__).resolve().parents[1] / "convert.py"


class ConvertTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.input = Path(self.temp.name) / "input.parquet"
        self.output = Path(self.temp.name) / "output.avro"

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, str(SCRIPT), *map(str, args)],
            capture_output=True,
            text=True,
        )

    def test_round_trip(self):
        original = pl.DataFrame({
            "id": [1, 2],
            "name": ["player 🎮", None],
            "score": [1.25, None],
            "active": [True, False],
            "payload": [b"\x00\xff", None],
            "day": [date(2026, 1, 1), None],
            "time": [datetime(2026, 1, 1, 12, 30, 0, 123456), None],
            "amount": [Decimal("12.34"), None],
            "tags": [["a", None], []],
            "player": [{"level": 3}, {"level": None}],
        })
        original.write_parquet(self.input)
        result = self.run_cli(self.input, self.output)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Converted 2 rows", result.stdout)
        self.assertEqual(self.output.read_bytes()[:4], b"Obj\x01")
        assert_frame_equal(pl.read_avro(self.output), original)

    def test_empty_file(self):
        original = pl.DataFrame(schema={"id": pl.Int64, "name": pl.String})
        original.write_parquet(self.input)
        result = self.run_cli(self.input, self.output)
        self.assertEqual(result.returncode, 0, result.stderr)
        assert_frame_equal(pl.read_avro(self.output), original)

    def test_missing_input(self):
        result = self.run_cli(self.input, self.output)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Error:", result.stderr)
        self.assertFalse(self.output.exists())

    def test_invalid_input(self):
        self.input.write_text("not parquet")
        result = self.run_cli(self.input, self.output)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.output.exists())

    def test_existing_output_is_preserved(self):
        pl.DataFrame({"id": [1]}).write_parquet(self.input)
        self.output.write_bytes(b"keep this")
        result = self.run_cli(self.input, self.output)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.output.read_bytes(), b"keep this")

    def test_input_cannot_be_overwritten(self):
        pl.DataFrame({"id": [1]}).write_parquet(self.input)
        before = self.input.read_bytes()
        result = self.run_cli(self.input, self.input)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.input.read_bytes(), before)
