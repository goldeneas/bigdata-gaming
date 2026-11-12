"""Convert one CSV file to an Avro file from the command line."""

import argparse
from pathlib import Path
import re

import polars as pl


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Path to the input CSV file")
    parser.add_argument("output", type=Path, help="Path to the new Avro file")
    args = parser.parse_args()

    try:
        data = pl.read_csv(args.input, infer_schema_length=None)
        for name in data.columns:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
                parser.error(f"Invalid Avro field name: {name!r}")
        with args.output.open("xb") as output:
            data.write_avro(output, name="record")
    except (OSError, pl.exceptions.PolarsError) as error:
        parser.exit(1, f"Error: {error}\n")

    print(f"Converted {data.height} rows to {args.output}")


if __name__ == "__main__":
    main()
