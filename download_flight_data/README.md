# OpenSky airport arrivals downloader

`download_arrivals.py` downloads every flight that arrived at New York JFK (`KJFK`)
from the [OpenSky Network REST API](https://openskynetwork.github.io/opensky-api/rest.html)
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
python download_arrivals.py --airports KJFK --start 2025-01-01 --end 2025-03-31
```

| Option | Meaning | Default |
|---|---|---|
| `--airports` | ICAO airport code (`KJFK`, not `JFK`) | `KJFK` |
| `--start`, `--end` | First and last day, `YYYY-MM-DD`, in UTC. Both included. | required |
| `--credentials` | Path to your `credentials.json` | `credentials.json` |
| `--out` | Output folder | `data/opensky_arrivals` |
| `--max-wait` | Seconds to wait when out of credits before giving up | `900` |

The files end up in folders like
`data/opensky_arrivals/airport=KJFK/year=2025/month=01/2025-01-01_2025-01-02.csv`.

## Credits (the daily limit)

Each account gets **4,000 credits per day**. One request covers 2 days and costs 30 credits,
so one account can download about **266 days of JFK per day**, which means **one year takes
about 1.5 days**. When the credits run out the script stops. Run the same command again the
next day and it continues where it stopped (files it already saved are skipped).

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

- **Times are UTC, not New York time.** UTC is the same as London winter time. New York is
  5 hours behind UTC in winter and 4 hours behind in summer (daylight saving). So a plane
  that lands at JFK at **21:00 on 1 January, New York time**, is stored as
  **02:00 on 2 January** in `lastSeen_utc`, and it is in the file for 2 January.
  If you count "arrivals per day" straight from the UTC dates, every evening flight after
  19:00 (20:00 in summer) gets counted on the next day. Convert to New York time first,
  for example in Spark:
  ```python
  from pyspark.sql import functions as F
  df = df.withColumn("lastSeen_ny",
                     F.from_utc_timestamp(F.to_timestamp("lastSeen_utc"), "America/New_York"))
  ```
  Then group by the date of `lastSeen_ny`.
- **`lastSeen` is close to, but not exactly, the landing time.** It's the last time a
  receiver picked the plane up.
- **Test with one day first.** The docs don't say how far back the data goes.

## Download progress (KJFK)

✅ whole year downloaded · 🟨 partly downloaded · ⬜ not yet

Put your name in **Who** for the years you're downloading, so two people don't download the
same year.

| Year | Status | Who |
|---|---|---|
| 2026 | ⬜ | |
| 2025 | 🟨 Jan–Nov | |
| 2024 | ⬜ | |
| 2023 | ⬜ | |
| 2022 | ⬜ | |
| 2021 | ⬜ | |
| 2020 | ⬜ | |
| 2019 | ⬜ | |
| 2018 | ⬜ | |
| 2017 | ⬜ | |
| 2016 | ⬜ | |
| 2015 | ⬜ | |
| 2014 | ⬜ | |
| 2013 | ⬜ | |
| 2012 | ⬜ | |
| 2011 | ⬜ | |
| 2010 | ⬜ | |
| 2009 | ⬜ | |
| 2008 | ⬜ | |
| 2007 | ⬜ | |
| 2006 | ⬜ | |
| 2005 | ⬜ | |

## Questions

1. **Is it a problem that we only have `firstSeen` and `lastSeen`?** These are the first and
   last times a receiver saw the plane, not the real take-off and landing times. `lastSeen`
   is usually close to landing, but not exactly the same.
2. **What date range do we want, and how do we split the work?** There is no real
   documentation on how far back the data goes, so we should test a few older years first.
   - From which year to which year do we want the data?
   - How do we split it between us? (For example, each person downloads a few years with
     their own account. The files don't overlap, so they can simply be copied together.)
   - Where do we store it? 11 months of CSVs is only about 20 MB, so 15 years would be
     roughly 330 MB.
