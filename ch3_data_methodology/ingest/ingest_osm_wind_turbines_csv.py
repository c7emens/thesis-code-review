#!/usr/bin/env python3
# Ingest OSM wind turbine CSV files into the osm_wind_turbines table.
#
# Reads CSV files produced by fetch_osm_wind_turbines.py and loads them into
# a dedicated PostgreSQL table.  Creates the table on first run if it does not
# exist.  Re-ingestion is safe: rows with duplicate osm_id are silently dropped.
#
# Table schema (osm_wind_turbines)
#   osm_id            BIGINT       PRIMARY KEY — OSM node ID
#   latitude          DOUBLE       WGS84 latitude in decimal degrees
#   longitude         DOUBLE       WGS84 longitude in decimal degrees
#   name              TEXT         Turbine or wind farm name
#   ref               TEXT         Local reference identifier within the farm
#   operator          TEXT         Operating entity
#   manufacturer      TEXT         Turbine manufacturer (e.g. Vestas, Siemens)
#   model             TEXT         Turbine model (e.g. V164-8.0)
#   output_kw         DOUBLE       Rated power in kilowatts
#   hub_height_m      DOUBLE       Hub height in metres
#   rotor_diameter_m  DOUBLE       Rotor diameter in metres
#   start_date        TEXT         Installation date (free-form OSM string)
#   location_tag      TEXT         Raw OSM location tag value
#   is_offshore       BOOLEAN      True if tagged as offshore
#   source            TEXT         Always 'openstreetmap'
#   ingested_at       TIMESTAMPTZ  Server timestamp of insertion
#
# Usage:
#   python ingest_osm_wind_turbines.py osm_wind_turbines.csv
#   python ingest_osm_wind_turbines.py osm_wind_turbines.csv --dry-run
#   python ingest_osm_wind_turbines.py osm_wind_turbines.csv --batch-size 2000
#
# See: fetch_osm_wind_turbines.py  -- Produces the CSV files consumed here.

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

## Source tag written to every ingested row.
SOURCE_TAG = "openstreetmap"

## Default INSERT batch size (rows per round-trip).
DEFAULT_BATCH = 5_000


# DDL

## CREATE TABLE statement for osm_wind_turbines.
_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS osm_wind_turbines (
    osm_id            BIGINT           PRIMARY KEY,
    latitude          DOUBLE PRECISION NOT NULL,
    longitude         DOUBLE PRECISION NOT NULL,
    name              TEXT,
    ref               TEXT,
    operator          TEXT,
    manufacturer      TEXT,
    model             TEXT,
    output_kw         DOUBLE PRECISION,
    hub_height_m      DOUBLE PRECISION,
    rotor_diameter_m  DOUBLE PRECISION,
    start_date        TEXT,
    location_tag      TEXT,
    is_offshore       BOOLEAN          NOT NULL DEFAULT FALSE,
    source            TEXT             NOT NULL DEFAULT 'openstreetmap',
    ingested_at       TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);
"""

## Index to accelerate offshore-only spatial queries.
_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_osm_turbines_offshore
    ON osm_wind_turbines (is_offshore, latitude, longitude);
"""

## Bulk-insert template; duplicate osm_ids are silently dropped.
_UPSERT = """
INSERT INTO osm_wind_turbines
    (osm_id, latitude, longitude, name, ref, operator, manufacturer, model,
     output_kw, hub_height_m, rotor_diameter_m, start_date,
     location_tag, is_offshore, source)
VALUES %s
ON CONFLICT (osm_id) DO NOTHING;
"""


# Helpers

def _to_float(val: str) -> float | None:
    """
    Parse a CSV cell to float, returning None for blank/non-numeric values.

    val: Raw string value from the CSV cell.
    Returns: Parsed float, or None.
    """
    if not val:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _to_bool(val: str) -> bool:
    """
    Parse a CSV cell to bool.

    Treats "True", "true", "1", "yes" as True; everything else as False.

    val: Raw string value from the CSV cell.
    Returns: Boolean value.
    """
    return val.strip().lower() in {"true", "1", "yes"}


# Table management

def ensure_table(conn) -> None:
    """
    Create the osm_wind_turbines table if it does not already exist.

    Runs CREATE TABLE and CREATE INDEX idempotently, committing after each.

    conn: Active psycopg2 connection to the windfarm database.
    """
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE)
        conn.commit()
        cur.execute(_CREATE_INDEX)
        conn.commit()


# Ingestion

def ingest_file(conn, path: Path, batch_size: int,
                dry_run: bool) -> tuple[int, int]:
    """
    Parse a CSV file and upsert its rows into osm_wind_turbines.

    Processes the file in batches of @p batch_size rows.  Rows missing osm_id,
    latitude, or longitude are dropped.  Duplicate osm_ids are silently ignored
    via ON CONFLICT DO NOTHING.

    conn: Active psycopg2 connection.
    path: Path to the CSV file produced by fetch_osm_wind_turbines.py.
    batch_size: Number of rows per INSERT round-trip.
    dry_run: If True, parse and validate rows without writing to the DB.
    Returns: Tuple (n_inserted, n_skipped) where n_skipped counts rows
                       dropped due to missing required fields.
    """
    n_inserted = 0
    n_skipped  = 0
    batch: list[tuple] = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            osm_id = row.get("osm_id", "").strip()
            lat    = _to_float(row.get("latitude"))
            lon    = _to_float(row.get("longitude"))

            if not osm_id or lat is None or lon is None:
                n_skipped += 1
                continue

            batch.append((
                int(osm_id),
                lat,
                lon,
                row.get("name")             or None,
                row.get("ref")              or None,
                row.get("operator")         or None,
                row.get("manufacturer")     or None,
                row.get("model")            or None,
                _to_float(row.get("output_kw")),
                _to_float(row.get("hub_height_m")),
                _to_float(row.get("rotor_diameter_m")),
                row.get("start_date")       or None,
                row.get("location_tag")     or None,
                _to_bool(row.get("is_offshore", "False")),
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
        description="Ingest OSM wind turbine CSV files into PostgreSQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s osm_wind_turbines.csv
  %(prog)s osm_wind_turbines.csv --dry-run
  %(prog)s osm_wind_turbines.csv --batch-size 2000
        """,
    )
    parser.add_argument("files", nargs="+", metavar="CSV",
                        help="CSV file(s) produced by fetch_osm_wind_turbines.py")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH,
                        metavar="N",
                        help=f"Rows per INSERT batch (default: {DEFAULT_BATCH})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and validate rows without writing to the database")
    args = parser.parse_args()

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
        print("Table osm_wind_turbines ready.")

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
