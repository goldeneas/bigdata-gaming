# OpenSky airport arrivals downloader

`download_arrivals.py` downloads every flight that arrived at one or more airports
(for example New York's JFK, LaGuardia and Newark) from the
[OpenSky Network REST API](https://openskynetwork.github.io/opensky-api/rest.html)
and saves it as CSV files.

## Setup

You need Python 3.10 or newer and a free OpenSky account.

1. **Create an account** at <https://opensky-network.org> and verify your email.
2. **Create an API client**: go to *My OpenSky → Account*, create an API client and
   download `credentials.json`. Put it in this folder. It looks like this:
   ```json
   {"clientId": "...", "clientSecret": "..."}
   ```
   Keep it private and don't commit it (it's in `.gitignore`). Your website username and
   password won't work: the API only accepts these client credentials.
3. **Install the dependency** (only `requests`):
   ```bash
   python -m venv .venv
   source .venv/bin/activate          # fish shell: source .venv/bin/activate.fish
   pip install -r requirements.txt
   ```

## Usage

```bash
python download_arrivals.py --airports KJFK KLGA KEWR --start 2025-01-01 --end 2025-03-31
```

| Option | Meaning | Default |
|---|---|---|
| `--airports` | ICAO airport codes (`KJFK`, not `JFK`) | `KJFK` |
| `--start`, `--end` | First and last day, `YYYY-MM-DD`, in UTC. Both included. | required |
| `--credentials` | Path to your `credentials.json` | `credentials.json` |
| `--out` | Output folder | `data/opensky_arrivals` |
| `--max-wait` | Seconds to wait when out of credits before giving up | `900` |

## What it does

1. Splits the date range into chunks of at most 2 days. That's the most the API returns per
   request, and the cheapest per day. Chunks never cross a month boundary.
2. Skips chunks that already have a file, so you can re-run the same command at any time.
3. Logs in with your API client and asks `/flights/arrival` for each remaining chunk.
4. Writes each answer to its own CSV:
   ```
   data/opensky_arrivals/airport=KJFK/year=2025/month=01/2025-01-01_2025-01-02.csv
   ```
   The `name=value` folder names are Hive-style partitions, so Spark adds `airport`, `year`
   and `month` as columns automatically:
   `spark.read.option("header", True).csv("data/opensky_arrivals")`.
5. Prints how many credits you have left after each request.

## Credits (the daily limit)

| Account | Credits per day |
|---|---|
| Standard (what you get after signing up) | 4,000 |
| Active feeder (you run an ADS-B receiver that sends data to OpenSky) | 8,000 |

Each request costs **30 credits**, so a standard account can download about **266 days of one
airport per day**. Three airports for a whole year take about 4–5 days. When the credits run out
the script stops cleanly. **Run the same command again the next day** and it continues where it
stopped.

## The CSV columns

| Column | Meaning |
|---|---|
| `icao24` | Aircraft transponder address (identifies the plane) |
| `callsign` | Flight callsign, e.g. `DAL9889`. The first 3 letters are the airline (DAL = Delta) |
| `estDepartureAirport` | Where the flight most likely came from. Empty if OpenSky couldn't tell (about 18% at JFK) |
| `estArrivalAirport` | The airport you asked for |
| `firstSeen`, `lastSeen` | First and last time OpenSky saw the plane, Unix seconds, UTC |
| `firstSeen_utc`, `lastSeen_utc` | The same times as readable UTC dates |
| `est...HorizDistance`, `est...VertDistance` | Distance in metres from the airport at first/last sighting |
| `...CandidatesCount` | How many airports OpenSky considered |

## Things to know

- **Only up to yesterday.** OpenSky fills the arrivals table with a nightly job, so `--end`
  is moved back to yesterday automatically.
- **Times are UTC.** A New York day spans two UTC days, so convert to `America/New_York`
  before grouping by day or joining with local data.
- **`lastSeen` is close to, but not exactly, the landing time.** It's the last time a
  receiver picked the plane up.
- **Test with one day first.** The docs don't say how far back the data goes.
- **Cloud IPs may be blocked.** OpenSky warns that requests from AWS and other cloud providers
  can fail. Run it from your laptop or the university network.
