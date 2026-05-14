#!/usr/bin/env python3
# Reformat and ingest the GEM Global Wind Power Tracker Excel file.
#
# Reads the "Data" sheet of the GEM GWPT xlsx file, filters to offshore
# installation types, renames columns to snake_case, writes a single CSV
# to the data lake, and loads the rows into the gem_wind_farms PostgreSQL table.
#
# Table schema (gem_wind_farms)
#   gem_phase_id          TEXT  PRIMARY KEY — unique per project phase
#   date_last_researched  DATE
#   country               TEXT
#   project_name          TEXT  NOT NULL
#   phase_name            TEXT
#   capacity_mw           DOUBLE
#   installation_type     TEXT
#   status                TEXT
#   start_year            SMALLINT
#   retired_year          SMALLINT
#   latitude              DOUBLE  NOT NULL
#   longitude             DOUBLE  NOT NULL
#   location_accuracy     TEXT
#   state_province        TEXT
#   subregion             TEXT
#   region                TEXT
#   gem_location_id       TEXT
#   wiki_url              TEXT
#   source                TEXT  DEFAULT 'gem_gwpt'
#
# Usage:
#   python ingest_gem_wind_farms.py
#   python ingest_gem_wind_farms.py --xlsx /mnt/e/data_lake/wind_turbines/Global-Wind-Power-Tracker-February-2026.xlsx
#   python ingest_gem_wind_farms.py --dry-run

import argparse
import csv
import sys
from pathlib import Path

import openpyxl
import psycopg2
import psycopg2.extras


# Constants

## Default input Excel file path.
DEFAULT_XLSX = Path(
    "/mnt/e/data_lake/wind_turbines/Global-Wind-Power-Tracker-February-2026.xlsx"
)

## Default output CSV path.
DEFAULT_CSV = Path("/mnt/e/data_lake/wind_turbines/gem_wind_farms_offshore.csv")

## Default local TimescaleDB connection parameters.
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "windfarm",
    "user":     "thesis",
    "password": "thesis2026",
}

## Source tag written to every ingested row.
SOURCE_TAG = "gem_gwpt"

## Default INSERT batch size (rows per round-trip).
DEFAULT_BATCH = 2_000

## Installation type values treated as offshore.
OFFSHORE_TYPES = {"Offshore hard mount", "Offshore mount unknown", "Offshore floating"}

## Mapping from Excel column header → snake_case DB column name.
COLUMN_MAP = {
    "Date Last Researched":  "date_last_researched",
    "Country/Area":          "country",
    "Project Name":          "project_name",
    "Phase Name":            "phase_name",
    "Capacity (MW)":         "capacity_mw",
    "Installation Type":     "installation_type",
    "Status":                "status",
    "Start year":            "start_year",
    "Retired year":          "retired_year",
    "Latitude":              "latitude",
    "Longitude":             "longitude",
    "Location accuracy":     "location_accuracy",
    "State/Province":        "state_province",
    "Subregion":             "subregion",
    "Region":                "region",
    "GEM location ID":       "gem_location_id",
    "GEM phase ID":          "gem_phase_id",
    "Wiki URL":              "wiki_url",
}

## Ordered DB column names (matches COLUMN_MAP values + source).
DB_COLUMNS = list(COLUMN_MAP.values()) + ["source"]

## CSV column order (no source column needed in the file).
CSV_COLUMNS = list(COLUMN_MAP.values())


# DDL

## CREATE TABLE statement for gem_wind_farms.
_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS gem_wind_farms (
    gem_phase_id          TEXT             PRIMARY KEY,
    date_last_researched  DATE,
    country               TEXT,
    project_name          TEXT             NOT NULL,
    phase_name            TEXT,
    capacity_mw           DOUBLE PRECISION,
    installation_type     TEXT,
    status                TEXT,
    start_year            SMALLINT,
    retired_year          SMALLINT,
    latitude              DOUBLE PRECISION NOT NULL,
    longitude             DOUBLE PRECISION NOT NULL,
    location_accuracy     TEXT,
    state_province        TEXT,
    subregion             TEXT,
    region                TEXT,
    gem_location_id       TEXT,
    wiki_url              TEXT,
    source                TEXT             NOT NULL DEFAULT 'gem_gwpt'
);
"""

## Index to accelerate offshore spatial queries.
_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_gem_farms_latlon
    ON gem_wind_farms (latitude, longitude);
"""

## Upsert — skip rows whose gem_phase_id already exists.
_UPSERT = """
INSERT INTO gem_wind_farms
    (gem_phase_id, date_last_researched, country, project_name, phase_name,
     capacity_mw, installation_type, status, start_year, retired_year,
     latitude, longitude, location_accuracy, state_province, subregion,
     region, gem_location_id, wiki_url, source)
VALUES %s
ON CONFLICT (gem_phase_id) DO NOTHING;
"""


# Helpers

def _to_int(val) -> int | None:
    """
    Convert a cell value to int, returning None for blank/non-numeric.

    val: Raw cell value from openpyxl.
    Returns: Integer, or None.
    """
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _to_float(val) -> float | None:
    """
    Convert a cell value to float, returning None for blank/non-numeric.

    val: Raw cell value from openpyxl.
    Returns: Float, or None.
    """
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _to_date(val) -> str | None:
    """
    Convert an openpyxl date string ('YYYY/MM/DD') to ISO format.

    val: Raw cell value (string or None).
    Returns: 'YYYY-MM-DD' string, or None.
    """
    if not val:
        return None
    try:
        return str(val).replace("/", "-")
    except Exception:
        return None


# Table management

def ensure_table(conn) -> None:
    """
    Create the gem_wind_farms table and index if they do not exist.

    conn: Active psycopg2 connection.
    """
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE)
        cur.execute(_CREATE_INDEX)
    conn.commit()


# Reformatting

def load_offshore_rows(xlsx_path: Path) -> list[dict]:
    """
    Read the Excel "Data" sheet and return offshore rows as dicts.

    Filters to OFFSHORE_TYPES, renames columns via COLUMN_MAP, skips rows
    missing gem_phase_id or lat/lon.

    xlsx_path: Path to the GEM GWPT xlsx file.
    Returns: List of dicts keyed by snake_case column names.
    """
    print(f"Reading {xlsx_path.name} ...")
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb["Data"]

    rows = list(ws.iter_rows(values_only=True))
    header = list(rows[0])

    # Build index map for columns we care about
    col_idx = {}
    for orig, snake in COLUMN_MAP.items():
        try:
            col_idx[snake] = header.index(orig)
        except ValueError:
            print(f"  ⚠ Column not found in Excel: '{orig}'", file=sys.stderr)

    inst_idx = col_idx.get("installation_type")
    result   = []

    for row in rows[1:]:
        if row[0] is None:
            continue
        # Filter to offshore types
        if inst_idx is None or row[inst_idx] not in OFFSHORE_TYPES:
            continue

        d = {snake: row[col_idx[snake]] for snake in col_idx}

        # Skip rows missing essentials
        if not d.get("gem_phase_id") or d.get("latitude") is None or d.get("longitude") is None:
            continue

        result.append(d)

    wb.close()
    print(f"  {len(result):,} offshore rows extracted from 'Data' sheet")
    return result


# CSV export

def write_csv(rows: list[dict], csv_path: Path) -> None:
    """
    Write reformatted offshore rows to a CSV file.

    rows: List of dicts from load_offshore_rows().
    csv_path: Output file path.
    """
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  CSV written: {csv_path}  ({len(rows):,} rows)")


# Ingestion

def ingest_rows(conn, rows: list[dict], batch_size: int, dry_run: bool) -> int:
    """
    Upsert offshore wind farm rows into gem_wind_farms.

    conn: Active psycopg2 connection.
    rows: List of dicts from load_offshore_rows().
    batch_size: Rows per INSERT round-trip.
    dry_run: If True, skip the actual INSERT.
    Returns: Number of rows processed.
    """
    tuples = []
    for d in rows:
        tuples.append((
            str(d["gem_phase_id"]),
            _to_date(d.get("date_last_researched")),
            d.get("country")          or None,
            str(d["project_name"]),
            str(d["phase_name"])      if d.get("phase_name") else None,
            _to_float(d.get("capacity_mw")),
            d.get("installation_type") or None,
            d.get("status")            or None,
            _to_int(d.get("start_year")),
            _to_int(d.get("retired_year")),
            _to_float(d["latitude"]),
            _to_float(d["longitude"]),
            d.get("location_accuracy") or None,
            d.get("state_province")    or None,
            d.get("subregion")         or None,
            d.get("region")            or None,
            d.get("gem_location_id")   or None,
            d.get("wiki_url")          or None,
            SOURCE_TAG,
        ))

    if dry_run:
        return len(tuples)

    n_inserted = 0
    cur = conn.cursor()
    for i in range(0, len(tuples), batch_size):
        batch = tuples[i: i + batch_size]
        psycopg2.extras.execute_values(cur, _UPSERT, batch)
        conn.commit()
        n_inserted += len(batch)
        print(f"    ... {n_inserted:,} rows inserted", end="\r", flush=True)
    cur.close()
    print()
    return n_inserted


# Entry point

def main() -> int:
    """
    Command-line entry point.

    Returns: Exit code: 0 on success, 1 on error.
    """
    parser = argparse.ArgumentParser(
        description="Reformat and ingest GEM Global Wind Power Tracker data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s --xlsx /mnt/e/data_lake/wind_turbines/Global-Wind-Power-Tracker-February-2026.xlsx
  %(prog)s --dry-run
        """,
    )
    parser.add_argument("--xlsx", type=Path, default=DEFAULT_XLSX, metavar="PATH",
                        help=f"Input Excel file (default: {DEFAULT_XLSX.name})")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, metavar="PATH",
                        help=f"Output CSV path (default: {DEFAULT_CSV})")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH, metavar="N",
                        help=f"Rows per INSERT batch (default: {DEFAULT_BATCH})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Reformat and write CSV, but skip database ingestion")
    args = parser.parse_args()

    if not args.xlsx.exists():
        print(f"Error: file not found: {args.xlsx}", file=sys.stderr)
        return 1

    rows = load_offshore_rows(args.xlsx)
    if not rows:
        print("No offshore rows found — nothing to do.")
        return 0

    write_csv(rows, args.csv)

    if args.dry_run:
        print(f"\nDRY RUN — skipping database ingestion.")
        return 0

    conn = psycopg2.connect(**DB_CONFIG)
    ensure_table(conn)
    print("Table gem_wind_farms ready.")

    n = ingest_rows(conn, rows, args.batch_size, dry_run=False)
    conn.close()

    print(f"\nDone. {n:,} rows ingested into gem_wind_farms.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
