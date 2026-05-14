# Ingest ICOADS weather observation CSV files into the weather_observations table.
#
# Reads CSV files (plain, gzip, zstd, zip, or tar.gz) in chunks, renames columns
# to match the database schema, generates a PostGIS location string, and loads rows
# via COPY with ON CONFLICT DO NOTHING de-duplication. Supports --start-from and
# --end-at flags for resuming interrupted ingestion runs.
#
# Usage:
#   python ingest_weather_csv.py './data/weather/*.tar.gz'
#   python ingest_weather_csv.py './data/weather/*.tar.gz' --start-from 202103

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
    Process a single CSV stream and insert its data into the database.

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
    Process a compressed or plain CSV file and load its weather data.

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
    Normalise raw ICOADS columns and produce a PostGIS WKT location string.

    Renames all uppercase source columns to snake_case database names, drops rows
    missing station_id or time, deduplicates on (station_id, time), converts
    numeric columns to nullable integers, and builds a PostGIS POINT geometry.

    df: Raw DataFrame chunk from pd.read_csv.
    Returns: Cleaned DataFrame ready for database ingestion.
    """
    df = df.rename(columns={
        "STATION": "station_id",
        "DATE": "time",
        "LATITUDE": "latitude",
        "LONGITUDE": "longitude",
        "ELEVATION": "elevation",
        "NAME": "station_name",
        "IMMA_VER": "imma_version",
        "ATTM_CT": "attachment_count",
        "TIME_IND": "time_indicator",
        "LL_IND": "lat_lon_indicator",
        "SHIP_COURSE": "ship_course",
        "SHIP_SPD": "ship_speed",
        "NAT_SOURCE_IND": "national_source_indicator",
        "ID_IND": "id_indicator",
        "COUNTRY_CODE": "country_code",
        "WIND_DIR_IND": "wind_direction_indicator",
        "WIND_DIR": "wind_direction",
        "WIND_SPD_IND": "wind_speed_indicator",
        "WIND_SPEED": "wind_speed",
        "VV_IND": "visibility_indicator",
        "VISIBILITY": "visibility",
        "PRES_WX": "present_weather",
        "PAST_WX": "past_weather",
        "SEA_LVL_PRES": "sea_level_pressure",
        "CHAR_PPP": "pressure_tendency_char",
        "AMT_PRES_TEND": "pressure_tendency_amount",
        "IND_FOR_TEMP": "temperature_indicator",
        "AIR_TEMP": "air_temp",
        "IND_FOR_WBT": "wet_bulb_indicator",
        "WET_BULB_TEMP": "wet_bulb_temp",
        "DPT_IND": "dew_point_indicator",
        "DEW_PT_TEMP": "dew_point_temp",
        "SST_MM": "sst_measurement_method",
        "SEA_SURF_TEMP": "sea_surface_temp",
        "TOT_CLD_AMT": "total_cloud_amount",
        "LOW_CLD_AMT": "low_cloud_amount",
        "LOW_CLD_TYPE": "low_cloud_type",
        "HGT_IND": "height_indicator",
        "CLD_HGT": "cloud_height",
        "MID_CLD_TYPE": "mid_cloud_type",
        "HI_CLD_TYPE": "high_cloud_type",
        "WAVE_PERIOD": "wave_period",
        "WAVE_HGT": "wave_height",
        "SWELL_DIR": "swell_direction",
        "SWELL_PERIOD": "swell_period",
        "SWELL_HGT": "swell_height",
        "TEN_BOX_NUM": "marsden_square_10",
        "ONE_BOX_NUM": "marsden_square_1",
        "DECK": "deck_id",
        "SOURCE_ID": "source_id",
        "PLATFORM_ID": "platform_id",
        "DUP_STATUS": "duplicate_status",
        "DUP_CHK": "duplicate_check",
        "NIGHT_DAY_FLAG": "night_day_flag",
        "TRIM_FLAG": "trim_flag",
        "NCDC_QC_FLAGS": "ncdc_qc_flags",
        "EXTERNAL": "external_flag",
        "SOURCE_EXCLUSION_FLAG": "source_exclusion_flag",
        "OB_SOURCE": "observation_source",
        "OB_PLATFORM": "observation_platform",
        "FM_CODE_VER": "fm_code_version",
        "STA_WX_IND": "station_weather_indicator",
        "PAST_WX2": "past_weather_2",
        "DIR_OF_SWELL2": "swell_2_direction",
        "PER_OF_SWELL2": "swell_2_period",
        "HGT_OF_SWELL2": "swell_2_height",
        "IND_FOR_PRECIP": "precipitation_indicator",
        "QC_IND": "qc_indicator",
        "QC_IND_FOR_FIELDS": "qc_indicator_fields",
        "MQCS_VER": "mqcs_version",
    })

    # drop rows with missing essential data
    required = ["station_id", "time"]
    existing = [c for c in required if c in df.columns]

    if len(existing) < len(required):
        print(f"Warning: Missing columns {set(required) - set(existing)}, skipping chunk")
        return pd.DataFrame()   # return empty DataFrame to skip

    df = df.dropna(subset=existing)


    # drop duplicates (same station at same timestamp)
    df = df.drop_duplicates(subset=["station_id", "time"], keep="first")

    # handle sentinel heading value
    # if "heading" in df.columns:
    #     df.loc[df["heading"] == 511, "heading"] = pd.NA

    # convert integer columns to nullable integer
    int_columns = [
        "imma_version",
        "attachment_count",
        "time_indicator",
        "latlon_indicator",
        "ship_course",
        "ship_speed",
        "national_source_indicator",
        "id_indicator",
        "wind_direction_indicator",
        "wind_direction",
        "wind_speed_indicator",
        "visibility_indicator",
        "visibility",
        "present_weather",
        "past_weather",
        "pressure_tendency_char",
        "pressure_tendency_amount",
        "temperature_indicator",
        "wet_bulb_indicator",
        "dew_point_indicator",
        "sst_measurement_method",
        "total_cloud_amount",
        "low_cloud_amount",
        "low_cloud_type",
        "height_indicator",
        "cloud_height",
        "mid_cloud_type",
        "high_cloud_type",
        "wave_period",
        "swell_direction",
        "swell_period",
        "marsden_square_10",
        "marsden_square_1",
        "deck_id",
        "source_id",
        "platform_id",
        "duplicate_status",
        "duplicate_check",
        "night_day_flag",
        "external_flag",
        "source_exclusion_flag",
        "observation_source",
        "observation_platform",
        "fm_code_version",
        "station_weather_indicator",
        "past_weather_2",
        "swell_2_direction",
        "swell_2_period",
        "precipitation_indicator",
        "qc_indicator",
        # "qc_indicator_fields",
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
    Load a cleaned weather DataFrame into Postgres via COPY.

    Uses a temporary table with INSERT … ON CONFLICT DO NOTHING to skip
    rows that already exist in weather_observations.

    df: Cleaned DataFrame to load.
    conn: Active psycopg2 database connection.
    """
    columns = [
        "station_id",
        "time",
        "latitude",
        "longitude",
        "location",
        "elevation",
        "station_name",
        "imma_version",
        "attachment_count",
        "time_indicator",
        "latlon_indicator",
        "ship_course",
        "ship_speed",
        "national_source_indicator",
        "id_indicator",
        "country_code",
        "wind_direction_indicator",
        "wind_direction",
        "wind_speed_indicator",
        "wind_speed",
        "visibility_indicator",
        "visibility",
        "present_weather",
        "past_weather",
        "sea_level_pressure",
        "pressure_tendency_char",
        "pressure_tendency_amount",
        "temperature_indicator",
        "air_temp",
        "wet_bulb_indicator",
        "wet_bulb_temp",
        "dew_point_indicator",
        "dew_point_temp",
        "sst_measurement_method",
        "sea_surface_temp",
        "total_cloud_amount",
        "low_cloud_amount",
        "low_cloud_type",
        "height_indicator",
        "cloud_height",
        "mid_cloud_type",
        "high_cloud_type",
        "wave_period",
        "wave_height",
        "swell_direction",
        "swell_period",
        "swell_height",
        "marsden_square_10",
        "marsden_square_1",
        "deck_id",
        "source_id",
        "platform_id",
        "duplicate_status",
        "duplicate_check",
        "night_day_flag",
        "trim_flag",
        "ncdc_qc_flags",
        "external_flag",
        "source_exclusion_flag",
        "observation_source",
        "observation_platform",
        "fm_code_version",
        "station_weather_indicator",
        "past_weather_2",
        "swell_2_direction",
        "swell_2_period",
        "swell_2_height",
        "precipitation_indicator",
        "qc_indicator",
        #"qc_indicator_fields",
        "mqcs_version",
    ]

    # keep only available columns in the desired order
    available = [c for c in columns if c in df.columns]
    buffer = StringIO()
    df[available].to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    cursor = conn.cursor()
          # Create temp table
    cursor.execute("CREATE TEMP TABLE tmp_weather_observations (LIKE weather_observations INCLUDING DEFAULTS) ON COMMIT DROP")

    # COPY to temp table
    copy_sql = "COPY tmp_weather_observations (" + ",".join(available) + ") FROM STDIN WITH CSV NULL '\\N'"
    cursor.copy_expert(copy_sql, buffer)

    # Insert from temp, skip duplicates
    cols = ",".join(available)
    cursor.execute(f"""
        INSERT INTO weather_observations ({cols})
        SELECT {cols} FROM tmp_weather_observations
        ON CONFLICT (station_id, time) DO NOTHING
    """)

    conn.commit()


def _main(argv: list[str] | None = None) -> int:
    """
    Command-line entry point.

    argv: Argument list (defaults to sys.argv when None).
    Returns: Exit code (0 on success, 2 if no files matched).
    """
    parser = argparse.ArgumentParser(
        description="Ingest CSV files to Postgres (weather_observations)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Supported formats: {', '.join(get_supported_extensions())}

Examples:
  %(prog)s './data/weather/*.csv'           # Plain CSV files
  %(prog)s './data/weather/*.tar.gz'        # Tar.gz archives (multiple CSVs)
  %(prog)s './data/weather/202501.tar.gz'   # Single archive
  %(prog)s './data/weather/*.tar.gz' --start-from 202103
  %(prog)s './data/weather/*.tar.gz' --start-from 202103 --end-at 202106
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

    print(f"✅ Done! Processed {total_rows:,} rows from {len(files)} file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
