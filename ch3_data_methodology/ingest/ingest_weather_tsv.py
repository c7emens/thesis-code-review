# Ingest ICOADS weather observation TSV files into the weather_observations table.
#
# Reads one or more TSV files in chunks, renames columns to match the database
# schema, generates a PostGIS location string, and loads rows via COPY with
# ON CONFLICT DO NOTHING de-duplication.
#
# Usage:
#   python ingest_weather_tsv.py '*.tsv'
#   python ingest_weather_tsv.py data/obs.tsv --chunksize 50000

from __future__ import annotations

import argparse
import os
import sys
from io import StringIO
from pathlib import Path
from typing import Dict

import pandas as pd
import psycopg2
import glob as glob_module


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

    Environment variables DB_HOST, DB_PORT, DB_NAME, DB_USER, and DB_PASSWORD
    override the corresponding defaults in DB_CONFIG when set.
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


def process_file(filepath: Path, chunksize: int = 100_000) -> None:
    """
    Read a TSV file in chunks, clean each chunk, and load it to the database.

    filepath: Path to the TSV file to process.
    chunksize: Number of rows per chunk (default 100 000).
    """
    conn = _get_db_conn()
    try:
        for i, chunk in enumerate(pd.read_csv(filepath, chunksize=chunksize, low_memory=False)):
            clean_data = clean(chunk)
            load_to_db(clean_data, conn)
            print(f"Chunk {i+1} done ({(i+1)*chunksize:,} rows processed).")
    finally:
        conn.close()


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize raw ICOADS columns and produce a PostGIS WKT location string.

    Renames all uppercase source columns to snake_case database names, drops
    rows with missing station ID or timestamp, removes duplicates, and builds
    the PostGIS POINT geometry from latitude/longitude columns.

    df: Raw DataFrame chunk from pd.read_csv.
    Returns: Cleaned DataFrame ready for database ingestion.
    """
    df = df.rename(columns={
        "STATION": "station_id",
        "DATE": "local_time",
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
    df = df.dropna(subset=["station_id", "local_time"])

    # drop duplicates (same station at same timestamp)
    df = df.drop_duplicates(subset=["station_id", "local_time"], keep="first")

    # handle sentinel heading value
    # if "heading" in df.columns:
    #     df.loc[df["heading"] == 511, "heading"] = pd.NA

    # convert integer columns to nullable integer
    # int_columns = [
    #     "mms_id",
    #     "vessel_type",
    #     "navigation_status",
    #     "cargo_type_code",
    # ]

    # for col in int_columns:
    #     if col in df.columns:
    #         df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

    # safe creation of location column if lat/lon available
    if "latitude" in df.columns and "longitude" in df.columns:
        df["location"] = (
            "SRID=4326;POINT(" + df["longitude"].astype(str) + " " + df["latitude"].astype(str) + ")"
        )

    return df


def load_to_db(df: pd.DataFrame, conn) -> None:
    """
    Load a DataFrame into Postgres using COPY from an in-memory TSV buffer.

    Uses a temporary table with INSERT … ON CONFLICT DO NOTHING to skip
    rows that already exist in weather_observations.

    df: Cleaned DataFrame to load.
    conn: Active psycopg2 database connection.
    """
    columns = [
        "station_id",
        "local_time",
        "latitude",
        "longitude",
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
        "qc_indicator_fields",
        "mqcs_version",
    ]

    # keep only available columns in the desired order
    available = [c for c in columns if c in df.columns]
    buffer = StringIO()
    df[available].to_csv(buffer, sep="\t", index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    cursor = conn.cursor()
          # Create temp table
    cursor.execute("CREATE TEMP TABLE tmp_weather_observations (LIKE weather_observations INCLUDING DEFAULTS) ON COMMIT DROP")

    # COPY to temp table
    copy_sql = "COPY tmp_weather_observations (" + ",".join(available) + ") FROM STDIN WITH TSV NULL '\\N'"
    cursor.copy_expert(copy_sql, buffer)

    # Insert from temp, skip duplicates
    cols = ",".join(available)
    cursor.execute(f"""
        INSERT INTO weather_observations ({cols})
        SELECT {cols} FROM tmp_weather_observations
        ON CONFLICT (station_id, local_time) DO NOTHING
    """)

    conn.commit()


def _main(argv: list[str] | None = None) -> int:
    """
    Command-line entry point.

    argv: Argument list (defaults to sys.argv when None).
    Returns: Exit code (0 on success, 2 if no files matched).
    """
    parser = argparse.ArgumentParser(description="Ingest TSV files to Postgres (vessels_data_ais)")
    parser.add_argument("pattern", help="glob pattern or file path to process (e.g. '*.tsv' or data/*.tsv)")
    parser.add_argument("--chunksize", type=int, default=100_000, help="rows per chunk")
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

    for filepath in files:
        print(f"Processing: {filepath}")
        process_file(filepath, chunksize=args.chunksize)

    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
