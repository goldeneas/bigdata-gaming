# parquet2avro-converter

A small CLI that converts one local Parquet file into an Avro object container
file. Polars infers the schema from the Parquet file. No schema file or service
is needed.

## Run

Requires Python 3.11 or newer. From this directory:

```sh
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python convert.py input.parquet output.avro
```

Run `python convert.py --help` for usage. The output must be a new file in an
existing directory; existing files are never overwritten. Failures return a
nonzero exit code.

The whole input file is loaded into memory, so allow enough RAM for its
uncompressed data. Supported data types are those handled by Polars' Avro
writer, including numbers, strings, booleans, binary, nulls, dates, timestamps,
decimals, lists, and structs. Column names must be valid Avro field names.

## Check

With dependencies installed, run from this directory:

```sh
python -m unittest discover -s tests
```
