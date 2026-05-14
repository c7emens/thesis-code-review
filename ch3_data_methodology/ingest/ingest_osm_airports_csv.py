#!/usr/bin/env python3
# Ingest OSM airports CSV produced by fetch_osm_airports.py into PostgreSQL.
#
# Creates the osm_airports table if it does not exist.  Re-ingestion is safe:
# duplicate osm_ids are silently ignored.
#
# Table schema (osm_airports)
#   osm_id      BIGINT   PRIMARY KEY — OSM element ID
#   osm_type    TEXT     'node' or 'way'
#   latitude    DOUBLE   WGS84 latitude
#   longitude   DOUBLE   WGS84 longitude
#   name        TEXT     Airport/heliport name
#   operator    TEXT     Operating entity
#   icao        TEXT     ICAO code (e.g. EGLL)
#   iata        TEXT     IATA code (e.g. LHR)
#   aeroway     TEXT     OSM aeroway tag: aerodrome / heliport / helipad
#   ele_m       DOUBLE   Elevation in metres
#   source      TEXT     DEFAULT 'openstreetmap'
#   ingested_at TIMESTAMPTZ DEFAULT NOW()
#
# Usage:
#   python ingest_osm_airports_csv.py osm_airports.csv
#   python ingest_osm_airports_csv.py osm_airports.csv --dry-run
#   python ingest_osm_airports_csv.py osm_airports.csv --type heliport

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

SOURCE_TAG    = "openstreetmap"
DEFAULT_BATCH = 2_000


# DDL

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS osm_airports (
    osm_id      BIGINT           PRIMARY KEY,
    osm_type    TEXT,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    name        TEXT,
    operator    TEXT,
    icao        TEXT,
    iata        TEXT,
    aeroway     TEXT,
    ele_m       DOUBLE PRECISION,
    source      TEXT             NOT NULL DEFAULT 'openstreetmap',
    ingested_at TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);
"""

_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_osm_airports_aeroway_latlon
    ON osm_airports (aeroway, latitude, longitude);
"""

_CREATE_ICAO_INDEX = """
CREATE INDEX IF NOT EXISTS idx_osm_airports_icao
    ON osm_airports (icao)
    WHERE icao IS NOT NULL;
"""

_UPSERT = """
INSERT INTO osm_airports
    (osm_id, osm_type, latitude, longitude, name, operator,
     icao, iata, aeroway, ele_m, source)
VALUES %s
ON CONFLICT (osm_id) DO NOTHING;
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
        cur.execute(_CREATE_ICAO_INDEX)
    conn.commit()


# Ingestion

def ingest_file(conn, path: Path, batch_size: int, dry_run: bool,
                type_filter: str | None) -> tuple[int, int]:
    """
    Parse the OSM airports CSV and upsert into osm_airports.

    conn: Active psycopg2 connection.
    path: CSV file from fetch_osm_airports.py.
    batch_size: Rows per INSERT round-trip.
    dry_run: If True, parse without writing.
    type_filter: Only ingest rows with this aeroway value.
    Returns: (n_inserted, n_skipped) tuple.
    """
    n_inserted = 0
    n_skipped  = 0
    batch: list[tuple] = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            osm_id = _int(row.get("osm_id", ""))
            lat    = _float(row.get("latitude", ""))
            lon    = _float(row.get("longitude", ""))

            if osm_id is None or lat is None or lon is None:
                n_skipped += 1
                continue

            if type_filter and row.get("aeroway", "") != type_filter:
                n_skipped += 1
                continue

            batch.append((
                osm_id,
                _str(row.get("osm_type", "")) or "node",
                lat,
                lon,
                _str(row.get("name", "")),
                _str(row.get("operator", "")),
                _str(row.get("icao", "")),
                _str(row.get("iata", "")),
                _str(row.get("aeroway", "")),
                _float(row.get("ele_m", "")),
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
        description="Ingest OSM airports CSV into PostgreSQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s osm_airports.csv
  %(prog)s osm_airports.csv --dry-run
  %(prog)s osm_airports.csv --type heliport
        """,
    )
    parser.add_argument("file", metavar="CSV",
                        help="CSV file produced by fetch_osm_airports.py")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH, metavar="N",
                        help=f"Rows per INSERT batch (default: {DEFAULT_BATCH})")
    parser.add_argument("--type", metavar="TYPE", dest="type_filter",
                        help="Only ingest rows with this aeroway type "
                             "(aerodrome / heliport / helipad)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse without writing to the database")
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        print(f"Error: file not found: {path}", file=sys.stderr)
        return 1

    conn = psycopg2.connect(**DB_CONFIG)

    if args.dry_run:
        print("DRY RUN — no data will be written to the database.")
    else:
        ensure_table(conn)
        print("Table osm_airports ready.")

    label_filter = f" (aeroway={args.type_filter})" if args.type_filter else ""
    print(f"Ingesting {path.name}{label_filter} ...")
    n_ins, n_skip = ingest_file(conn, path, args.batch_size,
                                args.dry_run, args.type_filter)
    conn.close()

    label = "would insert" if args.dry_run else "inserted"
    print(f"\nDone. {n_ins:,} rows {label}, {n_skip} skipped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
