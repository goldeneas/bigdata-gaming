import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal


SCRIPT = Path(__file__).resolve().parents[1] / "convert.py"


class ConvertTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.input = Path(self.temp.name) / "input.csv"
        self.output = Path(self.temp.name) / "output.avro"

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, str(SCRIPT), *map(str, args)],
            capture_output=True,
            text=True,
        )

    def test_round_trip(self):
        self.input.write_text(
            'id,name,score,active,note\n'
            '1,"player 🎮, one",1.25,true,"line one\nline two"\n'
            '2,,,false,"quoted ""text"""\n',
            encoding="utf-8",
        )
        expected = pl.DataFrame({
            "id": [1, 2],
            "name": ["player 🎮, one", None],
            "score": [1.25, None],
            "active": [True, False],
            "note": ["line one\nline two", 'quoted "text"'],
        })
        result = self.run_cli(self.input, self.output)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Converted 2 rows", result.stdout)
        self.assertEqual(self.output.read_bytes()[:4], b"Obj\x01")
        assert_frame_equal(pl.read_avro(self.output), expected)

    def test_type_inference_includes_later_rows(self):
        values = [str(number) for number in range(150)] + ["text"]
        self.input.write_text("value\n" + "\n".join(values) + "\n")
        result = self.run_cli(self.input, self.output)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(pl.read_avro(self.output)["value"].to_list(), values)

    def test_header_only_file(self):
        self.input.write_text("id,name\n")
        result = self.run_cli(self.input, self.output)
        self.assertEqual(result.returncode, 0, result.stderr)
        expected = pl.DataFrame(schema={"id": pl.String, "name": pl.String})
        assert_frame_equal(pl.read_avro(self.output), expected)

    def test_missing_input(self):
        result = self.run_cli(self.input, self.output)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Error:", result.stderr)
        self.assertFalse(self.output.exists())

    def test_empty_input(self):
        self.input.write_text("")
        result = self.run_cli(self.input, self.output)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.output.exists())

    def test_malformed_csv(self):
        self.input.write_text("id,name\n1,player,extra\n")
        result = self.run_cli(self.input, self.output)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.output.exists())

    def test_invalid_avro_header(self):
        self.input.write_text("player name\nAlice\n")
        result = self.run_cli(self.input, self.output)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invalid Avro field name", result.stderr)
        self.assertFalse(self.output.exists())

    def test_existing_output_is_preserved(self):
        self.input.write_text("id\n1\n")
        self.output.write_bytes(b"keep this")
        result = self.run_cli(self.input, self.output)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.output.read_bytes(), b"keep this")

    def test_input_cannot_be_overwritten(self):
        self.input.write_text("id\n1\n")
        before = self.input.read_bytes()
        result = self.run_cli(self.input, self.input)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.input.read_bytes(), before)
