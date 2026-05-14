#!/usr/bin/env python3
# Ingest helicopter position CSV files into the TimescaleDB helicopter_positions table.
#
# Reads CSV files produced by fetch_opensky_helicopters.py (OpenSky Trino export) and
# loads them into a dedicated TimescaleDB hypertable. Creates the table and hypertable
# on first run if they do not exist.
#
# Table schema (helicopter_positions)
#   time            TIMESTAMPTZ  -- UTC timestamp (hypertable partitioning key)
#   icao24          TEXT         -- ICAO 24-bit hex address (lower-case)
#   registration    TEXT         -- Aircraft registration / tail number
#   aircraft_desc   TEXT         -- Free-text aircraft description (e.g. "H175 HELICOPTER")
#   latitude        DOUBLE       -- WGS84 latitude in decimal degrees
#   longitude       DOUBLE       -- WGS84 longitude in decimal degrees
#   altitude_m      DOUBLE       -- Barometric altitude in metres (converted from feet)
#   groundspeed_kts DOUBLE       -- Ground speed in knots
#   heading         DOUBLE       -- Track angle 0-360 degrees
#   n_sensors       INTEGER      -- Number of ADS-B receivers that observed this position
#   source          TEXT         -- Always 'opensky_trino'
#
# Usage:
#   python ingest_opensky_helicopters_csv.py opensky_helicopters.csv
#   python ingest_opensky_helicopters_csv.py *.csv --batch-size 2000
#   python ingest_opensky_helicopters_csv.py block_island.csv vineyard.csv --dry-run
#
# See: fetch_opensky_helicopters.py  -- Produces the CSV files consumed here.

import argparse
import csv
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras


# Constants

## Default local TimescaleDB connection parameters.
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "windfarm",
    "user":     "thesis",
    "password": "thesis2026",
}

## Conversion factor: feet to metres.
FEET_TO_METRES = 0.3048

## Source tag written to every ingested row.
SOURCE_TAG = "opensky_trino"

## Default INSERT batch size (rows per round-trip).
DEFAULT_BATCH = 5_000


# DDL

## CREATE TABLE statement for helicopter_positions.
_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS helicopter_positions (
    time            TIMESTAMPTZ      NOT NULL,
    icao24          TEXT             NOT NULL,
    registration    TEXT,
    aircraft_desc   TEXT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    altitude_m      DOUBLE PRECISION,
    groundspeed_kts DOUBLE PRECISION,
    heading         DOUBLE PRECISION,
    n_sensors       INTEGER,
    source          TEXT             NOT NULL DEFAULT 'opensky_trino',
    UNIQUE (time, icao24)
);
"""

## Convert table to a TimescaleDB hypertable partitioned by month.
_CREATE_HYPERTABLE = """
SELECT create_hypertable(
    'helicopter_positions', 'time',
    if_not_exists       => TRUE,
    chunk_time_interval => INTERVAL '1 month'
);
"""

## Index to accelerate per-aircraft time-series queries.
_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_heli_icao_time
    ON helicopter_positions (icao24, time DESC);
"""

## Bulk-insert template; duplicates are silently dropped.
_UPSERT = """
INSERT INTO helicopter_positions
    (time, icao24, registration, aircraft_desc,
     latitude, longitude, altitude_m, groundspeed_kts, heading, n_sensors, source)
VALUES %s
ON CONFLICT (time, icao24) DO NOTHING;
"""


# Helpers

def _to_float(val) -> float | None:
    """
    Parse a CSV cell to float, returning None for blank or non-numeric values.

    val: Raw string value from the CSV cell.
    Returns: Parsed float, or None if the cell is empty or unparseable.
    """
    if not val:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _to_int(val) -> int | None:
    """
    Parse a CSV cell to int, returning None for blank or non-numeric values.

    val: Raw string value from the CSV cell.
    Returns: Parsed int, or None if the cell is empty or unparseable.
    """
    if not val:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# Table management

def ensure_table(conn) -> None:
    """
    Create the helicopter_positions hypertable if it does not already exist.

    Runs CREATE TABLE, create_hypertable(), and CREATE INDEX idempotently.
    Commits after each DDL statement.

    conn: Active psycopg2 connection to the windfarm database.
    """
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE)
        conn.commit()
        cur.execute(_CREATE_HYPERTABLE)
        conn.commit()
        cur.execute(_CREATE_INDEX)
        conn.commit()


# Ingestion

def ingest_file(conn, path: Path, batch_size: int, dry_run: bool) -> tuple[int, int]:
    """
    Parse a single CSV file and upsert its rows into helicopter_positions.

    Processes the file in batches of @p batch_size rows to limit memory use
    and provide incremental progress output. Altitude is converted from feet to
    metres before insertion. Rows that would violate the UNIQUE(time, icao24)
    constraint are silently dropped (ON CONFLICT DO NOTHING).

    conn: Active psycopg2 connection.
    path: Path to the CSV file produced by fetch_opensky_helicopters.py.
    batch_size: Number of rows per INSERT round-trip.
    dry_run: If True, parse and validate rows but do not write to the database.
    Returns: Tuple (n_inserted, n_skipped_empty) where n_skipped_empty is
                       the count of rows dropped due to missing lat/lon/time.
    """
    n_inserted     = 0
    n_skipped      = 0
    batch: list[tuple] = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            time_utc = row.get("time_utc", "").strip()
            icao24   = row.get("icao24",   "").strip().lower()
            lat      = _to_float(row.get("lat"))
            lon      = _to_float(row.get("lon"))

            # Drop rows missing essential fields
            if not time_utc or not icao24 or lat is None or lon is None:
                n_skipped += 1
                continue

            alt_ft = _to_float(row.get("alt_ft"))
            alt_m  = alt_ft * FEET_TO_METRES if alt_ft is not None else None

            batch.append((
                time_utc,
                icao24,
                row.get("registration") or None,
                row.get("aircraft_desc") or None,
                lat,
                lon,
                alt_m,
                _to_float(row.get("groundspeed_kts")),
                _to_float(row.get("heading")),
                _to_int(row.get("n_sensors")),
                SOURCE_TAG,
            ))

            if len(batch) >= batch_size:
                if not dry_run:
                    psycopg2.extras.execute_values(conn.cursor(), _UPSERT, batch)
                    conn.commit()
                n_inserted += len(batch)
                batch = []
                print(f"    ... {n_inserted:,} rows inserted", end="\r", flush=True)

    if batch:
        if not dry_run:
            psycopg2.extras.execute_values(conn.cursor(), _UPSERT, batch)
            conn.commit()
        n_inserted += len(batch)

    return n_inserted, n_skipped


# Entry point

def main() -> int:
    """
    Command-line entry point.

    Returns: Exit code: 0 on success, 1 on error.
    """
    parser = argparse.ArgumentParser(
        description="Ingest OpenSky helicopter CSV files into TimescaleDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s opensky_helicopters.csv
  %(prog)s block_island.csv vineyard.csv revolution.csv
  %(prog)s *.csv --batch-size 2000
  %(prog)s opensky_helicopters.csv --dry-run
        """,
    )
    parser.add_argument("files", nargs="+", metavar="CSV",
                        help="CSV file(s) produced by fetch_opensky_helicopters.py")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH,
                        metavar="N",
                        help=f"Rows per INSERT batch (default: {DEFAULT_BATCH})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and validate rows without writing to the database")
    args = parser.parse_args()

    # Validate all paths up front
    paths = []
    for p in args.files:
        path = Path(p)
        if not path.exists():
            print(f"Error: file not found: {path}", file=sys.stderr)
            return 1
        paths.append(path)

    conn = psycopg2.connect(**DB_CONFIG)

    if args.dry_run:
        print("DRY RUN — no data will be written to the database.")
    else:
        ensure_table(conn)
        print("Table helicopter_positions ready.")

    total_inserted = 0
    total_skipped  = 0

    for path in paths:
        print(f"\nIngesting {path.name} ...")
        n_ins, n_skip = ingest_file(conn, path, args.batch_size, args.dry_run)
        label = "would insert" if args.dry_run else "inserted"
        print(f"    {n_ins:,} rows {label}, {n_skip} rows skipped (missing fields)")
        total_inserted += n_ins
        total_skipped  += n_skip

    conn.close()

    label = "would insert" if args.dry_run else "total rows ingested"
    print(f"\nDone. {total_inserted:,} {label}  |  {total_skipped} skipped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
