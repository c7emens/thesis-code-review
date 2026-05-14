#!/usr/bin/env python3
"""
Vessel position outlier detection — Stage 2 post-processing.

Reads AIS vessel tracks from TimescaleDB, removes position outliers using
the theoretical-velocity approach (Baumgärtner et al. 2024 §2.2), and
writes flagged (mms_id, time) pairs to the table `vessel_position_outliers`
so Stage 3 can filter them out.

Usage examples
--------------
# Dry run — just print stats, no writes
python run_vessel_outliers.py --start 2024-01-01 --end 2024-02-01 --dry-run

# Process a date range and write outlier flags to DB
python run_vessel_outliers.py --start 2024-01-01 --end 2024-02-01

# Process a single vessel
python run_vessel_outliers.py --start 2024-01-01 --end 2025-01-01 --mmsi 123456789

# Custom thresholds
python run_vessel_outliers.py --start 2024-01-01 --end 2024-02-01 \\
    --max-speed 50 --abs-threshold 25 --rel-factor 4
"""

import argparse
import logging
import math
import sys
import time as time_mod

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from detect_outliers import clean_tracks, MAX_SPEED_VESSEL_KT

# Gap-detection thresholds
_GAP_MIN_MINUTES = 30    # minimum time silence to be recorded as a track gap
_GAP_MIN_DIST_KM = 1.0   # minimum straight-line distance — filters stationary blackouts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# CLI

def parse_args():
    parser = argparse.ArgumentParser(
        description="Detect and flag position outliers in AIS vessel tracks.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Date range
    parser.add_argument(
        "--start", required=True, metavar="YYYY-MM-DD",
        help="Start of time window (inclusive).",
    )
    parser.add_argument(
        "--end", required=True, metavar="YYYY-MM-DD",
        help="End of time window (exclusive).",
    )

    # Optional filters
    parser.add_argument(
        "--mmsi", metavar="MMSI",
        help="Process a single vessel MMSI only (default: all vessels).",
    )

    # Thresholds
    parser.add_argument(
        "--max-speed", type=float, default=MAX_SPEED_VESSEL_KT, metavar="KT",
        help=f"Absolute max realistic speed in knots (default: {MAX_SPEED_VESSEL_KT}).",
    )
    parser.add_argument(
        "--abs-threshold", type=float, default=20.0, metavar="KT",
        help="Absolute divergence threshold between theoretical and reported speed (default: 20).",
    )
    parser.add_argument(
        "--rel-factor", type=float, default=3.0, metavar="X",
        help="Relative factor threshold: theoretical / reported > X flags outlier (default: 3).",
    )

    # DB connection
    parser.add_argument(
        "--db-host", default="localhost", help="TimescaleDB host (default: localhost).",
    )
    parser.add_argument(
        "--db-port", type=int, default=5432, help="TimescaleDB port (default: 5432).",
    )
    parser.add_argument(
        "--db-name", default="windfarm", help="Database name (default: windfarm).",
    )
    parser.add_argument(
        "--db-user", default="thesis", help="Database user (default: thesis).",
    )
    parser.add_argument(
        "--db-password", default="", help="Database password.",
    )

    # Behaviour
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print stats only — do not write to the database.",
    )
    parser.add_argument(
        "--chunk-days", type=int, default=7, metavar="N",
        help="Process N days at a time to limit memory use (default: 7).",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Show per-vessel outlier counts.",
    )

    return parser.parse_args()


# Database helpers

def connect(args):
    return psycopg2.connect(
        host=args.db_host, port=args.db_port,
        dbname=args.db_name, user=args.db_user, password=args.db_password,
    )


def ensure_outlier_table(conn):
    """Create vessel_position_outliers and vessel_track_gaps if they don't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vessel_position_outliers (
                mms_id  TEXT        NOT NULL,
                time    TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (mms_id, time)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vessel_track_gaps (
                mms_id           TEXT             NOT NULL,
                gap_start        TIMESTAMPTZ      NOT NULL,
                gap_end          TIMESTAMPTZ      NOT NULL,
                gap_minutes      REAL             NOT NULL,
                lat_start        DOUBLE PRECISION NOT NULL,
                lon_start        DOUBLE PRECISION NOT NULL,
                lat_end          DOUBLE PRECISION NOT NULL,
                lon_end          DOUBLE PRECISION NOT NULL,
                dist_km          REAL             NOT NULL,
                implied_speed_kt REAL,
                PRIMARY KEY (mms_id, gap_start)
            );
        """)
    conn.commit()
    log.info("Tables vessel_position_outliers and vessel_track_gaps ready.")


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a  = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def scan_gaps(df: pd.DataFrame) -> list[tuple]:
    """
    Scan a cleaned AIS track DataFrame for significant spatial gaps.

    A gap is recorded when consecutive positions for the same vessel have:
      - time delta  > _GAP_MIN_MINUTES
      - straight-line distance > _GAP_MIN_DIST_KM

    Returns list of tuples:
      (mms_id, gap_start, gap_end, gap_min, lat0, lon0, lat1, lon1, dist_km, implied_kt)
    """
    gaps = []
    for mms_id, grp in df.groupby("mms_id", sort=False):
        grp = grp.sort_values("time").reset_index(drop=True)
        for i in range(len(grp) - 1):
            t0   = grp.at[i,     "time"]
            lat0 = grp.at[i,     "latitude"]
            lon0 = grp.at[i,     "longitude"]
            t1   = grp.at[i + 1, "time"]
            lat1 = grp.at[i + 1, "latitude"]
            lon1 = grp.at[i + 1, "longitude"]

            dt_s    = (t1 - t0).total_seconds()
            gap_min = dt_s / 60
            if gap_min < _GAP_MIN_MINUTES:
                continue

            dist_km = _haversine_km(lat0, lon0, lat1, lon1)
            if dist_km < _GAP_MIN_DIST_KM:
                continue

            implied_kt = (dist_km / 1.852) / (dt_s / 3600) if dt_s > 0 else None
            gaps.append((mms_id, t0, t1, round(gap_min, 1),
                         lat0, lon0, lat1, lon1,
                         round(dist_km, 3), round(implied_kt, 1) if implied_kt else None))
    return gaps


def write_gaps(conn, gaps: list[tuple]) -> None:
    """Insert track gaps, ignoring duplicates."""
    if not gaps:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO vessel_track_gaps
               (mms_id, gap_start, gap_end, gap_minutes,
                lat_start, lon_start, lat_end, lon_end,
                dist_km, implied_speed_kt)
               VALUES %s ON CONFLICT DO NOTHING""",
            gaps,
        )
    conn.commit()


def fetch_tracks(conn, start, end, mmsi=None):
    query = """
        SELECT mms_id, time, latitude, longitude, speed_over_ground
        FROM vessel_data_ais
        WHERE time >= %s AND time < %s
          AND latitude  IS NOT NULL
          AND longitude IS NOT NULL
    """
    params = [start, end]
    if mmsi:
        query += " AND mms_id = %s"
        params.append(mmsi)
    query += " ORDER BY mms_id, time"

    log.info("Fetching tracks %s → %s%s …", start, end, f" (MMSI {mmsi})" if mmsi else "")
    t0 = time_mod.time()
    df = pd.read_sql(query, conn, params=params, parse_dates=["time"])
    log.info("  Fetched %d records for %d vessels in %.1fs.",
             len(df), df["mms_id"].nunique(), time_mod.time() - t0)
    return df


def write_outliers(conn, outlier_pairs):
    """Insert (mms_id, time) outlier flags, ignoring duplicates."""
    if not outlier_pairs:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO vessel_position_outliers (mms_id, time) VALUES %s "
            "ON CONFLICT DO NOTHING",
            outlier_pairs,
        )
    conn.commit()


# Main

def main():
    args = parse_args()

    # Inject thresholds into detect_outliers module so flag_outlier() can use them
    import detect_outliers as _do
    _do.MAX_SPEED_VESSEL_KT     = args.max_speed
    _do.ABS_THRESHOLD_VESSEL_KT = args.abs_threshold
    _do.REL_FACTOR_VESSEL       = args.rel_factor

    conn = connect(args)
    if not args.dry_run:
        ensure_outlier_table(conn)

    # Chunk the date range to control memory
    date_range = pd.date_range(args.start, args.end, freq=f"{args.chunk_days}D")
    if date_range[-1] < pd.Timestamp(args.end):
        date_range = date_range.append(pd.DatetimeIndex([args.end]))

    total_before = total_after = 0
    all_outlier_pairs = []
    all_gap_rows      = []

    for i in range(len(date_range) - 1):
        chunk_start = str(date_range[i].date())
        chunk_end   = str(date_range[i + 1].date())

        df = fetch_tracks(conn, chunk_start, chunk_end, args.mmsi)
        if df.empty:
            log.warning("  No data in this chunk, skipping.")
            continue

        cleaned = clean_tracks(df, id_col="mms_id", mode="vessel")

        n_before = len(df)
        n_after  = len(cleaned)
        n_removed = n_before - n_after
        total_before += n_before
        total_after  += n_after

        if args.verbose:
            removed_mask = ~df.set_index(["mms_id", "time"]).index.isin(
                cleaned.set_index(["mms_id", "time"]).index
            )
            per_vessel = df[removed_mask.values].groupby("mms_id").size()
            for mmsi_id, count in per_vessel.items():
                log.info("    MMSI %-12s  removed %d outlier(s)", mmsi_id, count)

        log.info("  Chunk %s→%s: %d removed / %d (%.2f%%)",
                 chunk_start, chunk_end, n_removed, n_before,
                 100 * n_removed / n_before if n_before else 0)

        # Collect outlier (mms_id, time) pairs
        outliers = df[~df.set_index(["mms_id", "time"]).index.isin(
            cleaned.set_index(["mms_id", "time"]).index
        )]
        all_outlier_pairs.extend(zip(outliers["mms_id"], outliers["time"]))

        # Scan cleaned track for significant spatial gaps
        all_gap_rows.extend(scan_gaps(cleaned))

    # Summary
    n_total_removed = total_before - total_after
    log.info(
        "\nSummary: %d outliers removed from %d records (%.3f%%)",
        n_total_removed, total_before,
        100 * n_total_removed / total_before if total_before else 0,
    )
    log.info("Track gaps found: %d (dt > %d min, dist > %.1f km)",
             len(all_gap_rows), _GAP_MIN_MINUTES, _GAP_MIN_DIST_KM)

    if args.dry_run:
        log.info("Dry run — nothing written to database.")
    else:
        write_outliers(conn, all_outlier_pairs)
        log.info("Wrote %d outlier flags to vessel_position_outliers.", len(all_outlier_pairs))
        write_gaps(conn, all_gap_rows)
        log.info("Wrote %d track gaps to vessel_track_gaps.", len(all_gap_rows))

    conn.close()


if __name__ == "__main__":
    main()
