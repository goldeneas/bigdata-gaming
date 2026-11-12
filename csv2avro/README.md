# csv2avro

A small CLI that converts one local CSV file into an Avro object container
file. Polars infers column types from the entire CSV. No schema file or service
is needed. The resulting Avro schema is embedded in the output file's header;
a separate `.avsc` file is not generated.

## Run

Requires Python 3.11 or newer. From this directory:

```sh
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python convert.py input.csv output.avro
```

Run `python convert.py --help` for usage. The output must be a new file in an
existing directory; existing files are never overwritten. Failures return a
nonzero exit code.

The input must be a UTF-8, comma-separated CSV with a header row. Quoted fields
can contain commas and newlines. Unquoted empty fields become nulls. Types are
inferred as integers, floats, booleans, or strings; dates remain strings. Numeric
text such as `00123` may become an integer, so CSV formatting is not preserved.
Headers must be valid Avro field names: `[A-Za-z_][A-Za-z0-9_]*`.

The whole input file is loaded into memory, so allow enough RAM for its data.
A header-only CSV produces an Avro file with zero records; a completely empty
file is rejected.

## Container

From the repository root:

```sh
docker build -t csv2avro ./csv2avro
docker run --rm -v "$PWD/data:/data" csv2avro \
  /data/input.csv /data/output.avro
```

If your build environment uses a private CA, add
`--secret id=proxy_ca,src=/path/to/ca.pem` to the build command.

For Kubernetes, push the image to your registry and use it in a Job with
`args: ["/data/input.csv", "/data/output.avro"]`. Mount your data volume at
`/data` so the input is available and the output persists after the Job exits.

## Check

With dependencies installed, run from this directory:

```sh
python -m unittest discover -s tests
```
