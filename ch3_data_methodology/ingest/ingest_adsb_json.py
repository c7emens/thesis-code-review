#!/usr/bin/env python3
# Ingest ADS-B flight data from globe_history JSON files to PostgreSQL.
#
# The JSON files are gzip-compressed (despite their .json extension) and contain
# aircraft trace data with positions, altitudes, and speeds in the readsb/tar1090
# trace format.
#
# Usage:
#   python ingest_adsb_json.py '/mnt/e/data_lake/adsb/2024/01/01'
#   python ingest_adsb_json.py '/mnt/e/data_lake/adsb/2024/**' --start-from 2024/06/01
#   python ingest_adsb_json.py '/mnt/e/data_lake/adsb' --workers 4

import argparse
import gzip
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Dict, Generator, List, Tuple

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

## Field index mapping for the readsb/tar1090 trace array format.
TRACE_FIELDS = {
    0: 'time_offset',      # Seconds from base timestamp
    1: 'latitude',
    2: 'longitude',
    3: 'alt_baro',         # Barometric altitude (feet)
    4: 'ground_speed',     # Knots
    5: 'track',            # Heading/track (degrees)
    6: 'flags',
    7: 'vert_rate',        # Vertical rate (fpm)
    8: 'metadata',         # Extended metadata dict (optional)
    9: 'source',           # e.g., 'adsb_icao', 'mlat'
    10: 'alt_geom',        # Geometric altitude (feet)
    11: 'ias',             # Indicated airspeed
    12: 'tas',             # True airspeed
    13: 'mach',            # Mach number
}


def _get_db_conn():
    """Open a database connection, overriding defaults with environment variables."""
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


def ensure_table_exists(conn) -> None:
    """
    Create the flight_data_adsb_over_ground hypertable if it does not already exist.

    conn: Active psycopg2 database connection.
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS flight_data_adsb_over_ground (
            icao TEXT NOT NULL,
            time TIMESTAMPTZ NOT NULL,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            location GEOGRAPHY(POINT, 4326),
            alt_baro INTEGER,
            alt_geom INTEGER,
            ground_speed REAL,
            track REAL,
            vert_rate INTEGER,
            source TEXT,
            registration TEXT,
            aircraft_type TEXT,
            aircraft_desc TEXT,
            PRIMARY KEY (icao, time)
        );

        -- Create hypertable if TimescaleDB is available
        SELECT create_hypertable('flight_data_adsb_over_ground', 'time',
            chunk_time_interval => INTERVAL '7 days',
            if_not_exists => TRUE
        );
    """)
    conn.commit()
    print("✓ Table flight_data_adsb_over_ground ready")


def parse_trace_file(filepath: Path) -> Generator[Dict, None, None]:
    """
    Parse a single gzip-compressed JSON trace file and yield position records.

    Skips files that are truncated, corrupt, or contain invalid JSON.
    Altitude and vertical-rate values are converted from feet to metres on yield.

    filepath: Path to the gzip-compressed trace JSON file.
    Returns: Generator yielding one dict per valid position point.
    """
    try:
        with gzip.open(filepath, 'rt') as f:
            data = json.load(f)
    except (gzip.BadGzipFile, json.JSONDecodeError, OSError, EOFError) as e:
        print(f"  ⚠️ Skipping {filepath.name}: {e}")
        return

    icao = data.get('icao', '')
    registration = data.get('r', '')
    aircraft_type = data.get('t', '')
    aircraft_desc = data.get('desc', '')
    base_timestamp = data.get('timestamp', 0)

    for point in data.get('trace', []):
        if len(point) < 6:
            continue

        time_offset = point[0] if point[0] is not None else 0
        lat = point[1]
        lon = point[2]

        # Skip invalid coordinates
        if lat is None or lon is None:
            continue
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            continue

        timestamp = datetime.fromtimestamp(base_timestamp + time_offset, tz=timezone.utc)

        # Extract altitude fields (source unit: feet) — convert to metres on ingest
        alt_baro_ft = point[3] if len(point) > 3 else None
        if alt_baro_ft is not None and not isinstance(alt_baro_ft, (int, float)):
            alt_baro_ft = 0 if alt_baro_ft == 'ground' else None

        alt_geom_ft = point[10] if len(point) > 10 else None
        if alt_geom_ft is not None and not isinstance(alt_geom_ft, (int, float)):
            alt_geom_ft = 0 if alt_geom_ft == 'ground' else None

        # vert_rate source unit: feet/min — convert to metres/min (stored as integer)
        vert_rate_fpm = point[7] if len(point) > 7 and point[7] is not None else None

        yield {
            'icao': icao,
            'time': timestamp,
            'latitude': lat,
            'longitude': lon,
            'alt_baro': round(alt_baro_ft * 0.3048) if alt_baro_ft is not None else None,
            'ground_speed': float(point[4]) if len(point) > 4 and point[4] is not None else None,
            'track': float(point[5]) if len(point) > 5 and point[5] is not None else None,
            'vert_rate': round(vert_rate_fpm * 0.3048) if vert_rate_fpm is not None else None,
            'source': point[9] if len(point) > 9 else None,
            'alt_geom': round(alt_geom_ft * 0.3048) if alt_geom_ft is not None else None,
            'registration': registration,
            'aircraft_type': aircraft_type,
            'aircraft_desc': aircraft_desc,
        }


def load_batch_to_db(records: List[Dict], conn) -> Tuple[int, int]:
    """
    Load a batch of position records into the database using COPY.

    Deduplicates records in-memory by (icao, time) before inserting, then
    uses a temporary table with INSERT ON CONFLICT DO NOTHING for safe upsert.

    records: List of position record dicts from parse_trace_file().
    conn: Active psycopg2 database connection.
    Returns: Tuple of (inserted_count, duplicates_removed).
    """
    if not records:
        return 0, 0

    # Deduplicate in-memory by (icao, time) - keep first occurrence
    seen = set()
    unique_records = []
    for rec in records:
        key = (rec['icao'], rec['time'])
        if key not in seen:
            seen.add(key)
            unique_records.append(rec)

    duplicates_removed = len(records) - len(unique_records)
    records = unique_records

    columns = [
        'icao', 'time', 'latitude', 'longitude', 'location',
        'alt_baro', 'alt_geom', 'ground_speed', 'track', 'vert_rate',
        'source', 'registration', 'aircraft_type', 'aircraft_desc'
    ]

    buffer = StringIO()
    for rec in records:
        # Create PostGIS point
        location = f"SRID=4326;POINT({rec['longitude']} {rec['latitude']})"

        row = [
            rec['icao'],
            rec['time'].isoformat(),
            str(rec['latitude']),
            str(rec['longitude']),
            location,
            str(rec['alt_baro']) if rec['alt_baro'] is not None else '\\N',
            str(rec['alt_geom']) if rec['alt_geom'] is not None else '\\N',
            str(rec['ground_speed']) if rec['ground_speed'] is not None else '\\N',
            str(rec['track']) if rec['track'] is not None else '\\N',
            str(rec['vert_rate']) if rec['vert_rate'] is not None else '\\N',
            rec['source'] or '\\N',
            rec['registration'] or '\\N',
            rec['aircraft_type'] or '\\N',
            rec['aircraft_desc'] or '\\N',
        ]
        buffer.write('\t'.join(row) + '\n')

    buffer.seek(0)
    cursor = conn.cursor()

    # Use temp table + INSERT ON CONFLICT for upsert
    cursor.execute("""
        CREATE TEMP TABLE tmp_flight_data_adsb_over_ground
        (LIKE flight_data_adsb_over_ground INCLUDING DEFAULTS) ON COMMIT DROP
    """)

    cursor.copy_from(buffer, 'tmp_flight_data_adsb_over_ground', columns=columns, null='\\N')

    cursor.execute(f"""
        INSERT INTO flight_data_adsb_over_ground ({','.join(columns)})
        SELECT {','.join(columns)} FROM tmp_flight_data_adsb_over_ground
        ON CONFLICT (icao, time) DO NOTHING
    """)

    inserted = cursor.rowcount
    conn.commit()
    return inserted, duplicates_removed


def process_day_directory(day_path: Path, conn, batch_size: int = 10000,
                          day_num: int = 1, total_days: int = 1) -> Tuple[int, int, int]:
    """
    Process all trace files in a single day directory.

    day_path: Path to the day directory (must contain a traces/ subdirectory).
    conn: Active psycopg2 database connection.
    batch_size: Number of records to accumulate before flushing to the database.
    day_num: Current day index (for progress display).
    total_days: Total number of days being processed (for progress display).
    Returns: Tuple of (files_processed, records_inserted, duplicates_skipped).
    """
    traces_dir = day_path / 'traces'
    if not traces_dir.exists():
        return 0, 0, 0

    json_files = list(traces_dir.glob('**/*.json'))
    if not json_files:
        return 0, 0, 0

    total_records = 0
    total_duplicates = 0
    total_files = len(json_files)
    batch = []
    chunk_num = 0
    rows_in_day = 0

    for json_file in json_files:
        for record in parse_trace_file(json_file):
            batch.append(record)

            if len(batch) >= batch_size:
                chunk_num += 1
                inserted, dupes = load_batch_to_db(batch, conn)
                total_records += inserted
                total_duplicates += dupes
                rows_in_day += len(batch)
                print(f"  [{day_num}/{total_days}] chunk {chunk_num}: {rows_in_day:,} rows ({inserted:,} inserted)")
                batch = []

    # Final batch
    if batch:
        chunk_num += 1
        inserted, dupes = load_batch_to_db(batch, conn)
        total_records += inserted
        total_duplicates += dupes
        rows_in_day += len(batch)
        print(f"  [{day_num}/{total_days}] chunk {chunk_num}: {rows_in_day:,} rows ({inserted:,} inserted)")

    return total_files, total_records, total_duplicates


def find_day_directories(base_path: Path) -> List[Path]:
    """
    Find all day directories (YYYY/MM/DD structure) under a base path.

    Handles input at any level of the hierarchy:
    - Base:  /mnt/e/data_lake/adsb/           -> finds all years/months/days
    - Year:  /mnt/e/data_lake/adsb/2024/      -> finds all months/days in year
    - Month: /mnt/e/data_lake/adsb/2024/01/   -> finds all days in month
    - Day:   /mnt/e/data_lake/adsb/2024/01/01 -> returns this day directly

    base_path: Root directory to search.
    Returns: Sorted list of day directory paths that contain a traces/ subdirectory.
    """
    days = []

    if base_path.is_file():
        return []

    # Check if this is already a day directory (has .done marker or traces/)
    if (base_path / '.done').exists() or (base_path / 'traces').exists():
        return [base_path]

    # Try to detect what level we're at by checking what's inside
    subdirs = sorted([d for d in base_path.iterdir() if d.is_dir()])
    if not subdirs:
        return []

    sample = subdirs[0]
    sample_name = sample.name

    # 4-digit = year directories (e.g., 2024) -> we're at base level
    if len(sample_name) == 4 and sample_name.isdigit():
        for year_dir in subdirs:
            if not (len(year_dir.name) == 4 and year_dir.name.isdigit()):
                continue
            for month_dir in sorted(year_dir.glob('[0-9][0-9]')):
                for day_dir in sorted(month_dir.glob('[0-9][0-9]')):
                    if (day_dir / 'traces').exists():
                        days.append(day_dir)
    # 2-digit - could be months OR days, check if children have traces/
    elif len(sample_name) == 2 and sample_name.isdigit():
        # If first child has traces/, these are day directories (month level input)
        if (sample / 'traces').exists():
            for day_dir in subdirs:
                if (day_dir / 'traces').exists():
                    days.append(day_dir)
        # Otherwise these are month directories (year level input)
        else:
            for month_dir in subdirs:
                for day_dir in sorted(month_dir.glob('[0-9][0-9]')):
                    if (day_dir / 'traces').exists():
                        days.append(day_dir)

    return days


def _main(argv: List[str] = None) -> int:
    """
    Command-line entry point.

    argv: Argument list (defaults to sys.argv when None).
    Returns: Exit code (0 on success, 2 on path/directory error).
    """
    parser = argparse.ArgumentParser(
        description="Ingest ADS-B JSON trace files to PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /mnt/e/data_lake/adsb/2024/01/01          # Single day
  %(prog)s /mnt/e/data_lake/adsb/2024                # Full year
  %(prog)s /mnt/e/data_lake/adsb --start-from 2024/06/01
  %(prog)s /mnt/e/data_lake/adsb --start-from 2024/06 --end-at 2024/08
        """
    )
    parser.add_argument("path", help="Path to ADS-B data directory")
    parser.add_argument("--batch-size", type=int, default=10000,
                        help="Records per batch (default: 10000)")
    parser.add_argument("--start-from", type=str, metavar="PATH",
                        help="Start from path containing this string")
    parser.add_argument("--end-at", type=str, metavar="PATH",
                        help="End at path containing this string (inclusive)")
    parser.add_argument("--skip", type=int, default=0,
                        help="Skip first N days")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be processed without ingesting")
    args = parser.parse_args(argv)

    base_path = Path(args.path)
    if not base_path.exists():
        print(f"❌ Path not found: {base_path}")
        return 2

    # Find all day directories
    print("Scanning for day directories...")
    days = find_day_directories(base_path)

    if not days:
        print(f"❌ No day directories found in {base_path}")
        return 2

    total_days = len(days)
    print(f"Found {total_days} day(s)")

    # Apply filters
    start_offset = 0
    if args.start_from:
        start_idx = None
        for i, d in enumerate(days):
            if args.start_from in str(d):
                start_idx = i
                break
        if start_idx is None:
            print(f"❌ No path found matching '{args.start_from}'")
            return 2
        print(f"Starting from: {days[start_idx]}")
        start_offset = start_idx
        days = days[start_idx:]
    elif args.skip > 0:
        print(f"Skipping first {args.skip} day(s)")
        start_offset = args.skip
        days = days[args.skip:]

    if args.end_at:
        end_idx = None
        for i in range(len(days) - 1, -1, -1):
            if args.end_at in str(days[i]):
                end_idx = i
                break
        if end_idx is None:
            print(f"❌ No path found matching '{args.end_at}'")
            return 2
        print(f"Ending at: {days[end_idx]}")
        days = days[:end_idx + 1]

    print(f"Processing {len(days)} day(s)\n")

    if args.dry_run:
        print("Dry run - would process:")
        for d in days[:10]:
            print(f"  {d}")
        if len(days) > 10:
            print(f"  ... and {len(days) - 10} more")
        return 0

    # Connect and ensure table exists
    conn = _get_db_conn()
    ensure_table_exists(conn)

    # Process each day
    grand_total_files = 0
    grand_total_records = 0
    grand_total_duplicates = 0

    for i, day_path in enumerate(days, start_offset + 1):
        print(f"[{i}/{total_days}] {day_path}")

        files, records, duplicates = process_day_directory(
            day_path, conn, args.batch_size, day_num=i, total_days=total_days
        )
        grand_total_files += files
        grand_total_records += records
        grand_total_duplicates += duplicates

        dupe_info = f" ({duplicates:,} dupes skipped)" if duplicates > 0 else ""
        print(f"  ✓ {files:,} files, {records:,} positions{dupe_info}")

    conn.close()

    print(f"\n{'═' * 50}")
    print(f"✅ Done! Processed {len(days)} day(s)")
    print(f"   Files: {grand_total_files:,}")
    print(f"   Positions: {grand_total_records:,}")
    if grand_total_duplicates > 0:
        print(f"   Duplicates skipped: {grand_total_duplicates:,}")
    print(f"{'═' * 50}")

    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
