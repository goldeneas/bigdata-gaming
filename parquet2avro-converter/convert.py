"""Convert one Parquet file to an Avro file from the command line."""

import argparse
from pathlib import Path

import polars as pl


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Path to the input Parquet file")
    parser.add_argument("output", type=Path, help="Path to the new Avro file")
    args = parser.parse_args()

    try:
        data = pl.read_parquet(args.input)
        with args.output.open("xb") as output:
            data.write_avro(output, name="record")
    except (OSError, pl.exceptions.PolarsError) as error:
        parser.exit(1, f"Error: {error}\n")

    print(f"Converted {data.height} rows to {args.output}")


if __name__ == "__main__":
    main()
