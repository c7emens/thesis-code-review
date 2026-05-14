# Ingest wind turbine CSV data into the wind_turbines table.
#
# Reads CSV files (plain, gzip, zstd, zip, or tar.gz) in chunks, normalises
# column names, computes timezone information from lat/lon, generates a PostGIS
# location string, and loads rows via COPY with ON CONFLICT DO NOTHING.
#
# Usage:
#   python ingest_turbines_csv.py './data/turbines/*.csv'
#   python ingest_turbines_csv.py './data/turbines/data.csv.gz'

from __future__ import annotations

import argparse
from datetime import datetime
import os
import sys
from io import StringIO
from pathlib import Path
from typing import BinaryIO, Dict

import pandas as pd
import psycopg2
import glob as glob_module

import pytz
from timezonefinder import TimezoneFinder

from file_utils import open_csv_files, get_supported_extensions


## Default database connection parameters.
DB_CONFIG: Dict[str, object] = {
    "host": "localhost",
    "port": 5432,
    "dbname": "windfarm",
    "user": "thesis",
    "password": "thesis2026",
}

# calculate timezone info from lat/lon
def get_timezone_info(lat, lon):
    """
    Determine the timezone name and UTC offset for a geographic coordinate.

    lat: Latitude in decimal degrees.
    lon: Longitude in decimal degrees.
    Returns: Tuple of (timezone_name, offset_hours), or (None, None) if unknown.
    """
    tf = TimezoneFinder()
    tz_name = tf.timezone_at(lat=lat, lng=lon)

    if tz_name:
        tz = pytz.timezone(tz_name)
        # Use a winter reference date so the stored offset reflects the
        # timezone's standard (non-DST) offset, not whatever happens to
        # be active at ingest time.
        offset_hours = int(tz.utcoffset(datetime(2024, 1, 1)).total_seconds() / 3600)
        return tz_name, offset_hours
    return None, None

def _get_db_conn():
    """
    Return a psycopg2 connection, honouring environment variable overrides.
    """
    # allow environment variable overrides (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
    cfg = DB_CONFIG.copy()
    env_map = {
        "DB_HOST": "host",
        "DB_PORT": "port",
        "DB_NAME": "dbname",
        "DB_USER": "user",
        "DB_PASSWORD": "password",
    }
    for env, key in env_map.items():
        val = os.getenv(env)
        if val:
            cfg[key] = int(val) if key == "port" else val
    return psycopg2.connect(**cfg)


def process_stream(name: str, file_obj: BinaryIO, conn, chunksize: int = 100_000,
                   file_num: int = 1, total_files: int = 1) -> int:
    """
    Process a single CSV stream and insert its data into the database.

    name: Human-readable name of the stream (used for logging).
    file_obj: Binary file object readable by pd.read_csv.
    conn: Active psycopg2 database connection.
    chunksize: Number of rows per chunk (default 100 000).
    file_num: 1-based index of the current file (for progress display).
    total_files: Total number of files being processed (for progress display).
    Returns: Total number of rows processed from this stream.
    """
    rows_processed = 0

    try:
        for i, chunk in enumerate(pd.read_csv(file_obj, chunksize=chunksize, low_memory=False)):
            clean_data = clean(chunk)
            load_to_db(clean_data, conn)
            rows_processed += len(chunk)
            print(f"[{file_num}/{total_files}] {name} chunk {i+1}: {rows_processed:,} rows")
    except pd.errors.ParserError as e:
        print(f"⚠️ Skipping bad CSV {name}: {e}")

    return rows_processed


def process_file(filepath: Path, chunksize: int = 100_000, file_num: int = 1, total_files: int = 1) -> int:
    """
    Process a compressed or plain CSV file and load its turbine data.

    Supports .csv, .csv.zst, .csv.gz, .zip, and .tar.gz.

    filepath: Path to the file to process.
    chunksize: Rows per chunk (default 100 000).
    file_num: 1-based file index for progress display.
    total_files: Total file count for progress display.
    Returns: Total number of rows processed.
    """
    conn = _get_db_conn()
    total_rows = 0

    try:
        for csv_name, file_obj in open_csv_files(filepath):
            print(f"  Reading: {csv_name}")
            rows = process_stream(csv_name, file_obj, conn, chunksize, file_num, total_files)
            total_rows += rows
    except Exception as e:
        print(f"⚠️ Error processing {filepath.name}: {e}")
    finally:
        conn.close()

    return total_rows


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise turbine CSV columns and produce a PostGIS WKT location string.

    Renames source columns to database names, drops rows missing project_name or
    turbine_code, removes duplicates, computes timezone fields from lat/lon, and
    builds a PostGIS POINT geometry string.

    df: Raw DataFrame chunk from pd.read_csv.
    Returns: Cleaned DataFrame ready for database ingestion.
    """
    # rename columns
    df = df.rename(columns={
        "turbine_project_name": "project_name",
        "turbine_code": "turbine_code",
        "turbine_name": "turbine_name",
        "turbine_lat": "latitude",
        "turbine_lon": "longitude",
        "turbine_height": "height_meters",
    })

    # drop rows with missing essential data
    required = ["project_name", "turbine_code"]
    existing = [c for c in required if c in df.columns]

    if len(existing) < len(required):
        print(f"Warning: Missing columns {set(required) - set(existing)}, skipping chunk")
        return pd.DataFrame()   # return empty DataFrame to skip

    df = df.dropna(subset=existing)


    # drop duplicates (same station at same timestamp)
    df = df.drop_duplicates(subset=["project_name", "turbine_code"], keep="first")


    # calculate timezone for each turbine
    print("Calculating timezones...")
    tz_data = df.apply(lambda row: get_timezone_info(row["latitude"], row["longitude"]), axis=1)
    df["timezone_name"] = [t[0] for t in tz_data]
    df["timezone_offset"] = [t[1] for t in tz_data]

    # handle sentinel heading value
    # if "heading" in df.columns:
    #     df.loc[df["heading"] == 511, "heading"] = pd.NA

    # convert integer columns to nullable integer
    int_columns = [

    ]

    for col in int_columns:
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors="coerce")
            # Only convert to Int64 if all non-null values are within int range
            if numeric.dropna().empty or numeric.dropna().between(-32768, 32767).all():
                df[col] = numeric.round().astype("Int64")
            else:
                df[col] = numeric  # Keep as float


    # safe creation of location column if lat/lon available
    if "latitude" in df.columns and "longitude" in df.columns:
        df["location"] = (
            "SRID=4326;POINT(" + df["longitude"].astype(str) + " " + df["latitude"].astype(str) + ")"
        )

    return df


def load_to_db(df: pd.DataFrame, conn) -> None:
    """
    Load a cleaned turbine DataFrame into Postgres via COPY.

    Uses a temporary table with INSERT … ON CONFLICT DO NOTHING to skip
    rows that already exist in wind_turbines.

    df: Cleaned DataFrame to load.
    conn: Active psycopg2 database connection.
    """
    columns = [
        "project_name",
        "turbine_code",
        "turbine_name",
        "latitude",
        "longitude",
        "location",
        "height_meters",
        "timezone_name",
        "timezone_offset",
    ]

    # keep only available columns in the desired order
    available = [c for c in columns if c in df.columns]
    buffer = StringIO()
    df[available].to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    cursor = conn.cursor()
    # Create temp table
    cursor.execute("CREATE TEMP TABLE tmp_wind_turbines (LIKE wind_turbines INCLUDING ALL) ON COMMIT DROP")

    # COPY to temp table
    copy_sql = "COPY tmp_wind_turbines (" + ",".join(available) + ") FROM STDIN WITH CSV NULL '\\N'"
    cursor.copy_expert(copy_sql, buffer)

    # Insert from temp, skip duplicates
    cols = ",".join(available)
    cursor.execute(f"""
        INSERT INTO wind_turbines ({cols})
        SELECT {cols} FROM tmp_wind_turbines
        ON CONFLICT (project_name, turbine_code) DO NOTHING
    """)

    conn.commit()


def _main(argv: list[str] | None = None) -> int:
    """
    Command-line entry point.

    argv: Argument list (defaults to sys.argv when None).
    Returns: Exit code (0 on success, 2 if no files matched).
    """
    parser = argparse.ArgumentParser(
        description="Ingest CSV files to Postgres (wind_turbines)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Supported formats: {', '.join(get_supported_extensions())}

Examples:
  %(prog)s './data/turbines/*.csv'        # Plain CSV files
  %(prog)s './data/turbines/data.csv.gz'  # Gzip compressed
        """
    )
    parser.add_argument("pattern", help="glob pattern or file path (supports .csv, .csv.zst, .csv.gz, .zip, .tar.gz)")
    parser.add_argument("--chunksize", type=int, default=100_000, help="rows per chunk")
    parser.add_argument("--skip", type=int, default=0, help="skip first N files (to resume)")
    args = parser.parse_args(argv)

    p = Path(args.pattern)

    if "*" in args.pattern:
        matches = glob_module.glob(args.pattern, recursive=True)
        files = [Path(f) for f in matches if Path(f).is_file()]
    elif p.exists():
        files = [p]
    else:
        files = list(Path('.').glob(args.pattern))

    if not files:
        print(f"No files found for pattern: {args.pattern}")
        return 2

    # Sort for consistent ordering, then skip if requested
    files = sorted(files)
    total_files = len(files)

    if args.skip > 0:
        print(f"Skipping first {args.skip} file(s)")
        files = files[args.skip:]
        print(f"Remaining: {len(files)} file(s)\n")
    else:
        print(f"Found {total_files} file(s) to process\n")

    total_rows = 0
    for i, filepath in enumerate(files, args.skip + 1):
        print(f"Processing: {filepath}")
        rows = process_file(filepath, chunksize=args.chunksize, file_num=i, total_files=total_files)
        total_rows += rows
        print()

    print(f"✅ Done! Processed {total_rows:,} rows from {len(files)} file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
