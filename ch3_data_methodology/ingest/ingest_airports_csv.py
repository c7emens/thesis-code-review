#!/usr/bin/env python3
# Ingest the OurAirports global airports CSV into the airports table.
#
# Reads airports.csv (from ourairports.com) and loads it into PostgreSQL.
# Includes heliports, which are relevant to helicopter-based offshore wind
# farm maintenance operations.  Re-ingestion is safe: duplicate IDs are
# silently ignored.
#
# Table schema (airports)
#   airport_id        INTEGER   PRIMARY KEY — OurAirports numeric ID
#   ident             TEXT      FAA/local ident code (e.g. "KJFK", "00A")
#   type              TEXT      large_airport / medium_airport / small_airport
#                               / heliport / seaplane_base / balloonport / closed
#   name              TEXT      NOT NULL
#   latitude          DOUBLE    WGS84 latitude in decimal degrees
#   longitude         DOUBLE    WGS84 longitude in decimal degrees
#   elevation_ft      INTEGER   Elevation above MSL in feet
#   continent         TEXT      Two-letter continent code (NA, EU, AS, ...)
#   iso_country       TEXT      ISO 3166-1 alpha-2 country code
#   iso_region        TEXT      ISO 3166-2 region code (e.g. "US-MA")
#   municipality      TEXT      Nearest city or town
#   scheduled_service BOOLEAN   True if regular scheduled airline service
#   icao_code         TEXT      ICAO airport identifier
#   iata_code         TEXT      IATA airport code
#   source            TEXT      DEFAULT 'ourairports'
#
# Usage:
#   python ingest_airports_csv.py
#   python ingest_airports_csv.py --csv /mnt/e/data_lake/airports/airports.csv
#   python ingest_airports_csv.py --dry-run
#   python ingest_airports_csv.py --type heliport --dry-run

import argparse
import csv
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras


# Constants

## Default input CSV path.
DEFAULT_CSV = Path("/mnt/e/data_lake/airports/airports.csv")

## Default local TimescaleDB connection parameters.
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "windfarm",
    "user":     "thesis",
    "password": "thesis2026",
}

## Source tag written to every row.
SOURCE_TAG = "ourairports"

## Rows per INSERT round-trip.
DEFAULT_BATCH = 2_000

## Known airport type values in the source data.
KNOWN_TYPES = {"large_airport", "medium_airport", "small_airport",
               "heliport", "seaplane_base", "balloonport", "closed"}


# DDL

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS airports (
    airport_id        INTEGER          PRIMARY KEY,
    ident             TEXT,
    type              TEXT,
    name              TEXT             NOT NULL,
    latitude          DOUBLE PRECISION NOT NULL,
    longitude         DOUBLE PRECISION NOT NULL,
    elevation_ft      INTEGER,
    continent         TEXT,
    iso_country       TEXT,
    iso_region        TEXT,
    municipality      TEXT,
    scheduled_service BOOLEAN          NOT NULL DEFAULT FALSE,
    icao_code         TEXT,
    iata_code         TEXT,
    source            TEXT             NOT NULL DEFAULT 'ourairports'
);
"""

_CREATE_INDEX_TYPE = """
CREATE INDEX IF NOT EXISTS idx_airports_type
    ON airports (type, latitude, longitude);
"""

_CREATE_INDEX_ICAO = """
CREATE INDEX IF NOT EXISTS idx_airports_icao
    ON airports (icao_code) WHERE icao_code IS NOT NULL;
"""

_UPSERT = """
INSERT INTO airports
    (airport_id, ident, type, name, latitude, longitude, elevation_ft,
     continent, iso_country, iso_region, municipality, scheduled_service,
     icao_code, iata_code, source)
VALUES %s
ON CONFLICT (airport_id) DO NOTHING;
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


def _bool(val: str) -> bool:
    return val.strip().lower() == "yes"


# Table management

def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE)
        cur.execute(_CREATE_INDEX_TYPE)
        cur.execute(_CREATE_INDEX_ICAO)
    conn.commit()


# Ingestion

def ingest_file(conn, path: Path, batch_size: int, dry_run: bool,
                type_filter: str | None) -> tuple[int, int]:
    """
    Parse the OurAirports CSV and upsert rows into the airports table.

    conn: Active psycopg2 connection.
    path: CSV file path.
    batch_size: Rows per INSERT round-trip.
    dry_run: If True, parse without writing to the DB.
    type_filter: If set, only ingest rows with this type value.
    Returns: (n_inserted, n_skipped) tuple.
    """
    n_inserted = 0
    n_skipped  = 0
    batch: list[tuple] = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            airport_id = _int(row.get("id", ""))
            lat        = _float(row.get("latitude_deg", ""))
            lon        = _float(row.get("longitude_deg", ""))
            name       = _str(row.get("name", ""))

            if airport_id is None or lat is None or lon is None or not name:
                n_skipped += 1
                continue

            row_type = _str(row.get("type", "")) or ""
            if type_filter and row_type != type_filter:
                n_skipped += 1
                continue

            batch.append((
                airport_id,
                _str(row.get("ident", "")),
                row_type or None,
                name,
                lat,
                lon,
                _int(row.get("elevation_ft", "")),
                _str(row.get("continent", "")),
                _str(row.get("iso_country", "")),
                _str(row.get("iso_region", "")),
                _str(row.get("municipality", "")),
                _bool(row.get("scheduled_service", "")),
                _str(row.get("icao_code", "")),
                _str(row.get("iata_code", "")),
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
        description="Ingest OurAirports CSV into PostgreSQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Airport types: {', '.join(sorted(KNOWN_TYPES))}

Examples:
  %(prog)s
  %(prog)s --type heliport
  %(prog)s --csv /mnt/e/data_lake/airports/airports.csv --dry-run
        """,
    )
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, metavar="PATH",
                        help=f"Input CSV file (default: {DEFAULT_CSV.name})")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH, metavar="N",
                        help=f"Rows per INSERT batch (default: {DEFAULT_BATCH})")
    parser.add_argument("--type", metavar="TYPE", dest="type_filter",
                        help="Only ingest airports of this type (e.g. heliport)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse without writing to the database")
    args = parser.parse_args()

    if not args.csv.exists():
        print(f"Error: file not found: {args.csv}", file=sys.stderr)
        return 1

    if args.type_filter and args.type_filter not in KNOWN_TYPES:
        print(f"Warning: unknown type '{args.type_filter}'. "
              f"Known types: {', '.join(sorted(KNOWN_TYPES))}", file=sys.stderr)

    conn = psycopg2.connect(**DB_CONFIG)

    if args.dry_run:
        print("DRY RUN — no data will be written to the database.")
    else:
        ensure_table(conn)
        print("Table airports ready.")

    label_filter = f" (type={args.type_filter})" if args.type_filter else ""
    print(f"Ingesting {args.csv.name}{label_filter} ...")
    n_ins, n_skip = ingest_file(conn, args.csv, args.batch_size,
                                args.dry_run, args.type_filter)
    conn.close()

    label = "would insert" if args.dry_run else "inserted"
    print(f"\nDone. {n_ins:,} rows {label}, {n_skip} skipped (missing fields or filtered out).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
