#!/usr/bin/env python3
# Populates the PostGIS geography \c location column from latitude/longitude fields.
# @details Operates in configurable row batches to avoid lock contention on large
#          hypertables. Supports vessel_data_ais, weather_observations, and wind_turbines.
#
# Usage:
#   python populate_location.py vessel_data_ais
#   python populate_location.py vessel_data_ais --batch-size 50000
#
# @author Clemens Fritzsche
# @date 2026

import argparse
import psycopg2
import time

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "windfarm",
    "user": "thesis",
    "password": "thesis2026",
}

## Supported tables and their latitude/longitude column names.
TABLES = {
    "vessel_data_ais": {
        "lat_column": "latitude",
        "lon_column": "longitude",
    },
    "weather_observations": {
        "lat_column": "latitude",
        "lon_column": "longitude",
    },
    "wind_turbines": {
        "lat_column": "latitude",
        "lon_column": "longitude",
    },
}


def estimate_total_rows(conn, table):
    """
    Fast estimate of total rows using pg_class statistics.

    Falls back to a direct COUNT of NULL-location rows if pg_class stats are stale
    (i.e. reltuples <= 0), which can happen on freshly loaded tables before ANALYZE.

    conn: Active psycopg2 database connection.
    table: PostgreSQL table name to inspect.
    Returns: Estimated row count as an integer.
    """
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT reltuples::bigint AS estimate
        FROM pg_class
        WHERE relname = '{table}'
    """)
    result = cursor.fetchone()
    estimate = result[0] if result else 0

    # If stats are stale (-1 or 0), count rows with NULL location
    if estimate <= 0:
        print("Table stats stale, counting NULL locations...")
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE location IS NULL")
        estimate = cursor.fetchone()[0]

    return estimate


def format_time(seconds):
    """
    Formats a duration in seconds as a human-readable string.

    seconds: Duration in seconds (float).
    Returns: Formatted string, e.g. "42s", "3.5m", "1.2h".
    """
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


def update_in_batches(conn, table, lat_col, lon_col, batch_size=100000):
    """
    Updates the \c location geography column in batches using ST_MakePoint.

    Iterates until no rows with a NULL location remain. Each batch selects rows
    via ctid to avoid a sequential scan. Progress, speed, and ETA are printed
    after every batch.

    conn: Active psycopg2 database connection.
    table: Target table name.
    lat_col: Name of the latitude column.
    lon_col: Name of the longitude column.
    batch_size: Number of rows to update per transaction (default: 100 000).
    Returns: Total number of rows updated.
    """
    cursor = conn.cursor()
    total_updated = 0
    batch_num = 0
    batch_times = []

    estimated_total = estimate_total_rows(conn, table)
    print(f"Estimated total rows: {estimated_total:,}")
    print()

    start_time = time.time()

    while True:
        batch_num += 1
        batch_start = time.time()

        sql = f"""
            UPDATE {table}
            SET location = ST_SetSRID(ST_MakePoint({lon_col}, {lat_col}), 4326)::geography
            WHERE ctid IN (
                SELECT ctid FROM {table}
                WHERE location IS NULL
                  AND {lat_col} IS NOT NULL
                  AND {lon_col} IS NOT NULL
                LIMIT {batch_size}
            )
        """

        cursor.execute(sql)
        updated = cursor.rowcount
        conn.commit()

        batch_time = time.time() - batch_start
        batch_times.append(batch_time)

        if updated == 0:
            break

        total_updated += updated

        progress = (total_updated / estimated_total * 100) if estimated_total > 0 else 0
        rows_per_sec = total_updated / (time.time() - start_time)
        remaining_rows = estimated_total - total_updated
        eta_seconds = remaining_rows / rows_per_sec if rows_per_sec > 0 else 0

        print(f"Batch {batch_num}: {updated:,} rows | "
              f"Total: {total_updated:,} ({progress:.1f}%) | "
              f"Speed: {rows_per_sec:,.0f}/s | "
              f"ETA: {format_time(eta_seconds)}")

    return total_updated


def main():
    """
    Entry point — parses arguments and runs the location backfill.

    Returns: Exit code (0 on success).
    """
    parser = argparse.ArgumentParser(description="Populate location column from lat/lon")
    parser.add_argument("table", choices=["vessel_data_ais", "weather_observations", "wind_turbines"],
                        help="Table to update")
    parser.add_argument("--batch-size", "-b", type=int, default=100000,
                        help="Rows per batch (default: 100000)")
    args = parser.parse_args()

    table = args.table
    config = TABLES[table]
    lat_col = config["lat_column"]
    lon_col = config["lon_column"]

    print(f"Table: {table}")
    print(f"Batch size: {args.batch_size:,}")
    print()

    conn = psycopg2.connect(**DB_CONFIG)

    start = time.time()
    total = update_in_batches(conn, table, lat_col, lon_col, args.batch_size)
    elapsed = time.time() - start

    print()
    if total > 0:
        print(f"✅ Done! Updated {total:,} rows in {format_time(elapsed)}")
        print(f"   Average speed: {total/elapsed:,.0f} rows/sec")
    else:
        print("Nothing to update - all locations already populated")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
