# Ingest NOAA AIS vessel position CSV files into the vessel_data_ais table.
#
# Supports both the legacy CamelCase format (pre-2025) and the newer lowercase
# format (2025+). Files may be plain CSV, gzip, zstd, zip, or tar.gz. Rows are
# loaded via COPY into a temporary table with ON CONFLICT DO NOTHING de-duplication.
# --start-from and --end-at flags allow resuming interrupted ingestion runs.
#
# Usage:
#   python ingest_ais_csv.py '/mnt/e/data_lake/ais/AIS_*.csv'
#   python ingest_ais_csv.py '/mnt/e/data_lake/ais/ais-*.csv.zst' --skip 500

from __future__ import annotations

import argparse
import os
import sys
from io import StringIO
from pathlib import Path
from typing import BinaryIO, Dict

import pandas as pd
import psycopg2
import glob as glob_module

from file_utils import open_csv_files, get_supported_extensions


## Default database connection parameters.
DB_CONFIG: Dict[str, object] = {
    "host": "localhost",
    "port": 5432,
    "dbname": "windfarm",
    "user": "thesis",
    "password": "thesis2026",
}


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
    Process a single CSV stream and insert its AIS data into the database.

    name: Human-readable name of the stream (used for logging).
    file_obj: Binary file object readable by pd.read_csv.
    conn: Active psycopg2 database connection.
    chunksize: Rows per chunk (default 100 000).
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
    Process a compressed or plain AIS CSV file and load its vessel positions.

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
    Normalise AIS CSV columns and produce a PostGIS WKT location string.

    Handles both the legacy CamelCase column names (pre-2025 NOAA format) and the
    newer lowercase names (2025+). Drops rows with missing mms_id or time, removes
    duplicate (vessel, timestamp) pairs, nullifies the 511 sentinel heading value,
    converts smallint columns safely, and builds a PostGIS POINT geometry string.

    df: Raw DataFrame chunk from pd.read_csv.
    Returns: Cleaned DataFrame ready for database ingestion.
    """
    # Support both old format (CamelCase, pre-2025) and new format (lowercase, 2025+)
    df = df.rename(columns={
        # Old format (pre-2025)
        "MMSI": "mms_id",
        "BaseDateTime": "time",
        "LAT": "latitude",
        "LON": "longitude",
        "SOG": "speed_over_ground",
        "COG": "course_over_ground",
        "Heading": "heading",
        "VesselName": "vessel_name",
        "IMO": "imo_number",
        "CallSign": "radio_call_sign",
        "VesselType": "vessel_type",
        "Status": "navigation_status",
        "Length": "vessel_length",
        "Width": "vessel_width",
        "Draft": "vessel_draft",
        "Cargo": "cargo_type_code",
        "TransceiverClass": "ais_transceiver_class",
        # New format (2025+)
        "mmsi": "mms_id",
        "base_date_time": "time",
        # latitude/longitude already match
        "sog": "speed_over_ground",
        "cog": "course_over_ground",
        # heading already matches
        # vessel_name already matches
        "imo": "imo_number",
        "call_sign": "radio_call_sign",
        # vessel_type already matches
        "status": "navigation_status",
        "length": "vessel_length",
        "width": "vessel_width",
        "draft": "vessel_draft",
        "cargo": "cargo_type_code",
        "transceiver": "ais_transceiver_class",
    })

    # drop rows with missing essential data
    required = ["mms_id", "time"]
    existing = [c for c in required if c in df.columns]

    if len(existing) < len(required):
        print(f"Warning: Missing columns {set(required) - set(existing)}, skipping chunk")
        return pd.DataFrame()   # return empty DataFrame to skip

    df = df.dropna(subset=existing)

    # drop duplicates (same vessel at same timestamp)
    df = df.drop_duplicates(subset=["mms_id", "time"], keep="first")

    # handle sentinel heading value
    if "heading" in df.columns:
        df.loc[df["heading"] == 511, "heading"] = pd.NA

    # mms_id as text (identifier, not a number)
    if "mms_id" in df.columns:
        df["mms_id"] = df["mms_id"].astype(str).str.replace(r'\.0$', '', regex=True)

    # convert smallint columns (check range first)
    smallint_columns = [
        "vessel_type",
        "navigation_status",
        "cargo_type_code",
    ]
    for col in smallint_columns:
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors="coerce")
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
    Load a cleaned AIS DataFrame into Postgres via COPY.

    Uses a temporary table with INSERT … ON CONFLICT DO NOTHING to skip
    rows that already exist in vessel_data_ais.

    df: Cleaned DataFrame to load.
    conn: Active psycopg2 database connection.
    """
    columns = [
        "mms_id",
        "time",
        "latitude",
        "longitude",
        "location",
        "speed_over_ground",
        "course_over_ground",
        "heading",
        "vessel_name",
        "imo_number",
        "radio_call_sign",
        "vessel_type",
        "navigation_status",
        "vessel_length",
        "vessel_width",
        "vessel_draft",
        "cargo_type_code",
        "ais_transceiver_class",
    ]

    # keep only available columns in the desired order
    available = [c for c in columns if c in df.columns]
    buffer = StringIO()
    df[available].to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    cursor = conn.cursor()
      # Create temp table
    cursor.execute("CREATE TEMP TABLE tmp_vessel_data_ais (LIKE vessel_data_ais INCLUDING DEFAULTS) ON COMMIT DROP")

    # COPY to temp table
    copy_sql = "COPY tmp_vessel_data_ais (" + ",".join(available) + ") FROM STDIN WITH CSV NULL '\\N'"
    cursor.copy_expert(copy_sql, buffer)

    # Insert from temp, skip duplicates
    cols = ",".join(available)
    cursor.execute(f"""
        INSERT INTO vessel_data_ais ({cols})
        SELECT {cols} FROM tmp_vessel_data_ais
        ON CONFLICT (mms_id, time) DO NOTHING
    """)

    conn.commit()


def _main(argv: list[str] | None = None) -> int:
    """
    Command-line entry point.

    argv: Argument list (defaults to sys.argv when None).
    Returns: Exit code (0 on success, 2 if no files matched).
    """
    parser = argparse.ArgumentParser(
        description="Ingest CSV files to Postgres (vessel_data_ais)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Supported formats: {', '.join(get_supported_extensions())}

Examples:
  %(prog)s '/mnt/d/thesis/data_lake/ais/AIS_*.csv'              # Plain CSV files
  %(prog)s '/mnt/d/thesis/data_lake/ais/ais-*.csv.zst'          # Zstd compressed
  %(prog)s '/mnt/d/thesis/data_lake/ais/AIS_*.zip'              # Zip archives
  %(prog)s '/mnt/d/thesis/data_lake/ais/AIS_*.csv' --skip 500   # Skip first 500 files
  %(prog)s '/mnt/d/thesis/data_lake/ais/AIS_*.csv' --start-from AIS_2021_03_07
        """
    )
    parser.add_argument("pattern", help="glob pattern or file path (supports .csv, .csv.zst, .csv.gz, .zip, .tar.gz)")
    parser.add_argument("--chunksize", type=int, default=100_000, help="rows per chunk")
    parser.add_argument("--skip", type=int, default=0, help="skip first N files (to resume)")
    parser.add_argument("--start-from", type=str, metavar="FILENAME", help="start from file containing this string (e.g. 'AIS_2021_03_07')")
    parser.add_argument("--end-at", type=str, metavar="FILENAME", help="end at file containing this string, inclusive (e.g. 'AIS_2021_12_31')")
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

    # Sort for consistent ordering
    files = sorted(files)
    total_files = len(files)

    # Handle --start-from option
    start_offset = 0
    if args.start_from:
        start_idx = None
        for i, f in enumerate(files):
            if args.start_from in str(f):  # check full path, not just filename
                start_idx = i
                break
        if start_idx is None:
            print(f"❌ No file found matching '{args.start_from}'")
            print(f"   Available files start with: {files[0]}")
            return 2
        print(f"Starting from file {start_idx + 1}/{total_files}: {files[start_idx]}")
        start_offset = start_idx
        files = files[start_idx:]
    elif args.skip > 0:
        print(f"Skipping first {args.skip} file(s)")
        start_offset = args.skip
        files = files[args.skip:]

    # Handle --end-at option (find LAST matching file)
    if args.end_at:
        end_idx = None
        for i in range(len(files) - 1, -1, -1):  # search backwards
            if args.end_at in str(files[i]):
                end_idx = i
                break
        if end_idx is None:
            print(f"❌ No file found matching '{args.end_at}'")
            print(f"   Available files end with: {files[-1]}")
            return 2
        print(f"Ending at file: {files[end_idx]}")
        files = files[:end_idx + 1]  # inclusive

    print(f"Found {len(files)} file(s) to process\n")

    total_rows = 0
    for i, filepath in enumerate(files, start_offset + 1):
        print(f"Processing: {filepath}")
        rows = process_file(filepath, chunksize=args.chunksize, file_num=i, total_files=total_files)
        total_rows += rows
        print()

    print(f"✅ Done! Processed {total_rows:,} rows from {total_files} file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
