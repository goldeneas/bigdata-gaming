"""Download historical airport arrivals from the OpenSky Network REST API into CSV files.

Example:
    python download_arrivals.py --airports KJFK KLGA KEWR --start 2025-01-01 --end 2025-03-31

Output (one CSV per request, in folders Spark can read as partitions):
    data/opensky_arrivals/airport=KJFK/year=2025/month=01/2025-01-01_2025-01-02.csv

How the OpenSky API shapes this script:
  * /flights/arrival returns at most 2 days per request, and only up to yesterday
    (OpenSky fills it with a nightly batch job).
  * A request covering 1-2 UTC days costs 30 credits. A standard account gets 4,000
    "flights" credits per day, so about 133 requests (= 266 days of one airport) per day.
  * When the credits run out the script stops. Run the same command again later:
    files that already exist are skipped, so it continues where it stopped.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

API_URL = "https://opensky-network.org/api"
TOKEN_URL = ("https://auth.opensky-network.org/auth/realms/opensky-network"
             "/protocol/openid-connect/token")
CREDITS_PER_REQUEST = 30

# Columns written to each CSV, in this order. All but the two *_utc columns come straight
# from the API; the *_utc columns are the same Unix timestamps as readable UTC dates.
FIELDS = [
    "icao24",                            # aircraft transponder address (hex)
    "callsign",                          # flight callsign, e.g. DAL9889
    "estDepartureAirport",               # where the flight probably came from (ICAO)
    "estArrivalAirport",                 # the airport we asked for
    "firstSeen", "lastSeen",             # Unix seconds, UTC; lastSeen ~ landing time
    "firstSeen_utc", "lastSeen_utc",     # the same times as YYYY-MM-DDTHH:MM:SSZ
    "estDepartureAirportHorizDistance",  # metres from the departure airport when first seen
    "estDepartureAirportVertDistance",
    "estArrivalAirportHorizDistance",    # metres from the arrival airport when last seen
    "estArrivalAirportVertDistance",
    "departureAirportCandidatesCount",   # how many airports OpenSky considered
    "arrivalAirportCandidatesCount",
]


def log(message: str) -> None:
    """Print a timestamped progress message."""
    print(time.strftime("%H:%M:%S"), message, file=sys.stderr, flush=True)


class CreditsExhausted(Exception):
    """Raised when OpenSky says we are out of credits for longer than we want to wait."""


# --------------------------------------------------------------------------------------
# OpenSky API client
# --------------------------------------------------------------------------------------

class OpenSkyClient:
    """Gets OAuth2 access tokens and makes API requests with retries.

    OpenSky only accepts OAuth2 "client credentials": you exchange the client id and
    secret from credentials.json for an access token, which "supposedly" expires after 30 minutes.
    """

    def __init__(self, client_id: str, client_secret: str, max_wait_s: int):
        self.client_id = client_id
        self.client_secret = client_secret
        self.max_wait_s = max_wait_s     # longest "out of credits" wait we accept
        self.session = requests.Session()
        self.token = None
        self.token_expires = 0.0
        self.credits_left = "?"          # updated from every response's headers

    def auth_header(self) -> dict[str, str]:
        """Return the Authorization header, fetching a new token when needed."""
        if self.token is None or time.time() > self.token_expires - 30:
            response = self.session.post(TOKEN_URL, data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }, timeout=60)
            response.raise_for_status()
            body = response.json()
            self.token = body["access_token"]
            self.token_expires = time.time() + body.get("expires_in", 1800)
        return {"Authorization": f"Bearer {self.token}"}

    def get(self, path: str, params: dict) -> list:
        """GET an endpoint and return its JSON list (empty list if there is no data)."""
        for attempt in range(1, 8):
            try:
                response = self.session.get(API_URL + path, params=params,
                                            headers=self.auth_header(), timeout=60)
            except requests.RequestException as error:
                wait = min(60, 2 ** attempt)
                log(f"network error ({type(error).__name__}), retrying in {wait}s")
                time.sleep(wait)
                continue

            # Every response tells us how many credits are left.
            self.credits_left = response.headers.get("X-Rate-Limit-Remaining", self.credits_left)
            status = response.status_code

            if status == 200:
                return response.json()
            if status == 404:            # OpenSky's way of saying "no flights in this interval"
                return []
            if status == 401:            # token expired early: get a new one and retry
                self.token = None
                continue
            if status == 429:            # out of credits: wait if it's short, otherwise stop
                wait = int(float(response.headers.get("X-Rate-Limit-Retry-After-Seconds", 60)))
                if wait > self.max_wait_s:
                    raise CreditsExhausted(f"out of credits, they refill in about {wait / 3600:.1f} h")
                log(f"rate limited, waiting {wait}s")
                time.sleep(wait + 1)
                continue
            if status >= 500:            # OpenSky server problem: wait and retry
                wait = 5 * attempt
                log(f"server error {status}, retrying in {wait}s")
                time.sleep(wait)
                continue
            raise RuntimeError(f"OpenSky returned {status} for {response.url}: {response.text[:300]}")
        raise RuntimeError(f"giving up on {path} after repeated failures")


# --------------------------------------------------------------------------------------
# Helpers for dates and files
# --------------------------------------------------------------------------------------

def two_day_chunks(start: date, end: date):
    """Split [start, end] into chunks of at most 2 days that never cross a month boundary.

    Keeping each chunk inside one month means every file belongs in one month folder.
    """
    day = start
    while day <= end:
        first_of_next_month = (day.replace(day=28) + timedelta(days=4)).replace(day=1)
        last = min(day + timedelta(days=1), first_of_next_month - timedelta(days=1), end)
        yield day, last
        day = last + timedelta(days=1)


def unix_seconds(day: date) -> int:
    """Midnight UTC at the start of `day`, as Unix seconds."""
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())


def utc_text(timestamp) -> str:
    """Unix seconds -> '2025-01-01T12:00:00Z' (empty string if missing)."""
    if not timestamp:
        return ""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def output_path(out_dir: str, airport: str, first: date, last: date) -> Path:
    """data/opensky_arrivals/airport=KJFK/year=2025/month=01/2025-01-01_2025-01-02.csv"""
    return (Path(out_dir) / f"airport={airport}" / f"year={first:%Y}" / f"month={first:%m}"
            / f"{first}_{last}.csv")


def write_csv(path: Path, flights: list[dict]) -> None:
    """Write one chunk. It is written to a .part file first and renamed at the end, so an
    interrupted run never leaves a half-written .csv that would be skipped on resume."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".part")
    with temp.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        for flight in flights:
            flight["callsign"] = (flight.get("callsign") or "").strip()  # API pads with spaces
            flight["firstSeen_utc"] = utc_text(flight.get("firstSeen"))
            flight["lastSeen_utc"] = utc_text(flight.get("lastSeen"))
            writer.writerow(flight)
    temp.rename(path)


# --------------------------------------------------------------------------------------
# Main program
# --------------------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--airports", nargs="+", default=["KJFK"],
                        help="ICAO airport codes, e.g. KJFK KLGA KEWR (default: KJFK)")
    parser.add_argument("--start", required=True, type=date.fromisoformat,
                        help="first day to download, YYYY-MM-DD (UTC)")
    parser.add_argument("--end", required=True, type=date.fromisoformat,
                        help="last day to download, inclusive, YYYY-MM-DD (UTC)")
    parser.add_argument("--credentials", default="credentials.json",
                        help="API client file from your OpenSky account page (default: credentials.json)")
    parser.add_argument("--out", default="data/opensky_arrivals",
                        help="output folder (default: data/opensky_arrivals)")
    parser.add_argument("--max-wait", type=int, default=900,
                        help="seconds to wait when out of credits before stopping (default: 900)")
    args = parser.parse_args()

    # Arrivals only exist up to yesterday.
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    if args.end > yesterday:
        log(f"--end moved to {yesterday}: arrivals are only available up to yesterday")
        args.end = yesterday

    credentials = json.loads(Path(args.credentials).read_text())
    client = OpenSkyClient(credentials["clientId"], credentials["clientSecret"], args.max_wait)

    # Work out which chunks still need downloading (existing files are skipped).
    todo = []
    for airport in args.airports:
        for first, last in two_day_chunks(args.start, args.end):
            path = output_path(args.out, airport, first, last)
            if not path.exists():
                todo.append((airport, first, last, path))
    log(f"{len(todo)} requests to make, about {len(todo) * CREDITS_PER_REQUEST} credits")

    for done, (airport, first, last, path) in enumerate(todo):
        # The interval runs from midnight UTC on `first` to 23:59:59 UTC on `last`.
        params = {
            "airport": airport,
            "begin": unix_seconds(first),
            "end": unix_seconds(last + timedelta(days=1)) - 1,
        }
        try:
            flights = client.get("/flights/arrival", params)
        except CreditsExhausted as error:
            log(f"stopping: {error}. {done} files saved; run the same command later to continue.")
            return
        write_csv(path, flights)
        log(f"{airport} {first}..{last}: {len(flights)} arrivals (credits left: {client.credits_left})")

    log(f"finished, files are in {args.out}")


if __name__ == "__main__":
    main()
