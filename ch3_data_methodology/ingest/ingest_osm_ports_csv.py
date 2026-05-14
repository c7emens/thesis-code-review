#!/usr/bin/env python3
# Ingest OSM port CSV produced by fetch_osm_ports.py into PostgreSQL.
#
# Creates the osm_ports table if it does not exist.  Re-ingestion is safe:
# duplicate osm_ids are silently ignored.
#
# Table schema (osm_ports)
#   osm_id       BIGINT   PRIMARY KEY — OSM element ID
#   osm_type     TEXT     'node' or 'way'
#   latitude     DOUBLE   WGS84 latitude
#   longitude    DOUBLE   WGS84 longitude
#   name         TEXT     Port/harbour name
#   operator     TEXT     Operating entity
#   harbour      TEXT     Raw OSM harbour tag value
#   seamark_type TEXT     Raw seamark:type tag value
#   amenity      TEXT     Raw amenity tag value
#   port_type    TEXT     Derived: harbour / marina / ferry_terminal / ...
#   source       TEXT     DEFAULT 'openstreetmap'
#   ingested_at  TIMESTAMPTZ DEFAULT NOW()
#
# Usage:
#   python ingest_osm_ports_csv.py osm_ports.csv
#   python ingest_osm_ports_csv.py osm_ports.csv --dry-run
#   python ingest_osm_ports_csv.py osm_ports.csv --port-type harbour

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
CREATE TABLE IF NOT EXISTS osm_ports (
    osm_id       BIGINT           PRIMARY KEY,
    osm_type     TEXT,
    latitude     DOUBLE PRECISION NOT NULL,
    longitude    DOUBLE PRECISION NOT NULL,
    name         TEXT,
    operator     TEXT,
    harbour      TEXT,
    seamark_type TEXT,
    amenity      TEXT,
    port_type    TEXT,
    source       TEXT             NOT NULL DEFAULT 'openstreetmap',
    ingested_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);
"""

_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_osm_ports_type_latlon
    ON osm_ports (port_type, latitude, longitude);
"""

_UPSERT = """
INSERT INTO osm_ports
    (osm_id, osm_type, latitude, longitude, name, operator,
     harbour, seamark_type, amenity, port_type, source)
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
    conn.commit()


# Ingestion

def ingest_file(conn, path: Path, batch_size: int, dry_run: bool,
                port_type_filter: str | None) -> tuple[int, int]:
    """
    Parse the OSM ports CSV and upsert into osm_ports.

    conn: Active psycopg2 connection.
    path: CSV file from fetch_osm_ports.py.
    batch_size: Rows per INSERT round-trip.
    dry_run: If True, parse without writing.
    port_type_filter: Only ingest rows with this port_type value.
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

            if port_type_filter and row.get("port_type", "") != port_type_filter:
                n_skipped += 1
                continue

            batch.append((
                osm_id,
                _str(row.get("osm_type", "")) or "node",
                lat,
                lon,
                _str(row.get("name", "")),
                _str(row.get("operator", "")),
                _str(row.get("harbour", "")),
                _str(row.get("seamark_type", "")),
                _str(row.get("amenity", "")),
                _str(row.get("port_type", "")),
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
        description="Ingest OSM ports CSV into PostgreSQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s osm_ports.csv
  %(prog)s osm_ports.csv --dry-run
  %(prog)s osm_ports.csv --port-type harbour
        """,
    )
    parser.add_argument("file", metavar="CSV",
                        help="CSV file produced by fetch_osm_ports.py")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH, metavar="N",
                        help=f"Rows per INSERT batch (default: {DEFAULT_BATCH})")
    parser.add_argument("--port-type", metavar="TYPE", dest="port_type_filter",
                        help="Only ingest rows with this port_type (e.g. harbour, ferry_terminal)")
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
        print("Table osm_ports ready.")

    label_filter = f" (port_type={args.port_type_filter})" if args.port_type_filter else ""
    print(f"Ingesting {path.name}{label_filter} ...")
    n_ins, n_skip = ingest_file(conn, path, args.batch_size,
                                args.dry_run, args.port_type_filter)
    conn.close()

    label = "would insert" if args.dry_run else "inserted"
    print(f"\nDone. {n_ins:,} rows {label}, {n_skip} skipped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
