#!/usr/bin/env python3
# Ingest the NGA World Port Index (Pub. 150) CSV into the ports table.
#
# Reads UpdatedPub150.csv (downloaded from the NGA Maritime Safety site),
# extracts the columns relevant to offshore wind O&M analysis, and loads them
# into a PostgreSQL table.  Re-ingestion is safe: duplicate port numbers are
# silently ignored.
#
# Table schema (ports)
#   port_number    INTEGER  PRIMARY KEY — NGA World Port Index Number
#   port_name      TEXT     NOT NULL
#   alternate_name TEXT
#   un_locode      TEXT     UN/LOCODE (5-char code where available)
#   country        TEXT     Full country name as in the source file
#   region         TEXT     NGA region string (e.g. "United States E Coast")
#   water_body     TEXT     Named water body
#   harbor_size    TEXT     Very Small / Small / Medium / Large
#   harbor_type    TEXT     River / Coastal / Tidal Basin / etc.
#   harbor_use     TEXT     Public / Military / Private / etc.
#   shelter        TEXT     Good / Fair / Poor / None
#   latitude       DOUBLE   WGS84 latitude
#   longitude      DOUBLE   WGS84 longitude
#   source         TEXT     DEFAULT 'nga_wpi'
#
# Usage:
#   python ingest_ports_csv.py
#   python ingest_ports_csv.py --csv /mnt/e/data_lake/ports/UpdatedPub150.csv
#   python ingest_ports_csv.py --dry-run

import argparse
import csv
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras


# Constants

## Default input CSV path.
DEFAULT_CSV = Path("/mnt/e/data_lake/ports/UpdatedPub150.csv")

## Default local TimescaleDB connection parameters.
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "windfarm",
    "user":     "thesis",
    "password": "thesis2026",
}

## Source tag written to every row.
SOURCE_TAG = "nga_wpi"

## Rows per INSERT round-trip.
DEFAULT_BATCH = 500


# DDL

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS ports (
    port_number    INTEGER          PRIMARY KEY,
    port_name      TEXT             NOT NULL,
    alternate_name TEXT,
    un_locode      TEXT,
    country        TEXT,
    region         TEXT,
    water_body     TEXT,
    harbor_size    TEXT,
    harbor_type    TEXT,
    harbor_use     TEXT,
    shelter        TEXT,
    latitude       DOUBLE PRECISION NOT NULL,
    longitude      DOUBLE PRECISION NOT NULL,
    source         TEXT             NOT NULL DEFAULT 'nga_wpi'
);
"""

_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_ports_latlon
    ON ports (latitude, longitude);
"""

_UPSERT = """
INSERT INTO ports
    (port_number, port_name, alternate_name, un_locode, country, region,
     water_body, harbor_size, harbor_type, harbor_use, shelter,
     latitude, longitude, source)
VALUES %s
ON CONFLICT (port_number) DO NOTHING;
"""


# Helpers

def _str(val: str) -> str | None:
    s = val.strip()
    return s if s else None


def _float(val: str) -> float | None:
    try:
        return float(val.strip())
    except (ValueError, TypeError):
        return None


def _int(val: str) -> int | None:
    try:
        return int(val.strip())
    except (ValueError, TypeError):
        return None


# Table management

def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE)
        cur.execute(_CREATE_INDEX)
    conn.commit()


# Ingestion

def ingest_file(conn, path: Path, batch_size: int, dry_run: bool) -> tuple[int, int]:
    """
    Parse the Pub. 150 CSV and upsert rows into the ports table.

    conn: Active psycopg2 connection.
    path: CSV file path.
    batch_size: Rows per INSERT round-trip.
    dry_run: If True, parse without writing to the DB.
    Returns: (n_inserted, n_skipped) tuple.
    """
    n_inserted = 0
    n_skipped  = 0
    batch: list[tuple] = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            port_number = _int(row.get("World Port Index Number", ""))
            lat         = _float(row.get("Latitude", ""))
            lon         = _float(row.get("Longitude", ""))

            if port_number is None or lat is None or lon is None:
                n_skipped += 1
                continue

            port_name = _str(row.get("Main Port Name", ""))
            if not port_name:
                n_skipped += 1
                continue

            batch.append((
                port_number,
                port_name,
                _str(row.get("Alternate Port Name", "")),
                _str(row.get("UN/LOCODE", "")),
                _str(row.get("Country Code", "")),
                _str(row.get("Region Name", "")),
                _str(row.get("World Water Body", "")),
                _str(row.get("Harbor Size", "")),
                _str(row.get("Harbor Type", "")),
                _str(row.get("Harbor Use", "")),
                _str(row.get("Shelter Afforded", "")),
                lat,
                lon,
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
    parser = argparse.ArgumentParser(
        description="Ingest NGA World Port Index CSV into PostgreSQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s --csv /mnt/e/data_lake/ports/UpdatedPub150.csv
  %(prog)s --dry-run
        """,
    )
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, metavar="PATH",
                        help=f"Input CSV file (default: {DEFAULT_CSV.name})")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH, metavar="N",
                        help=f"Rows per INSERT batch (default: {DEFAULT_BATCH})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse without writing to the database")
    args = parser.parse_args()

    if not args.csv.exists():
        print(f"Error: file not found: {args.csv}", file=sys.stderr)
        return 1

    conn = psycopg2.connect(**DB_CONFIG)

    if args.dry_run:
        print("DRY RUN — no data will be written to the database.")
    else:
        ensure_table(conn)
        print("Table ports ready.")

    print(f"Ingesting {args.csv.name} ...")
    n_ins, n_skip = ingest_file(conn, args.csv, args.batch_size, args.dry_run)
    conn.close()

    label = "would insert" if args.dry_run else "inserted"
    print(f"\nDone. {n_ins:,} rows {label}, {n_skip} skipped (missing fields).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
