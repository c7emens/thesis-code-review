#!/usr/bin/env python3
# Stage 2 — fetch full-day tracks for every (mms_id, date) hit from Stage 1.
#
# Reads the stage1_vessel_hits table and, for each unique vessel_date, queries
# vessel_data_ais for the full day track of every mms_id that appeared near a
# turbine on that date.  No bbox filter is applied so the complete trajectory
# (port → turbine → port) is captured.
#
# A ±1-day window is used to capture voyages that cross UTC midnight.
# Already-fetched dates are recorded in stage2_vessel_dates and skipped on
# re-run, making the script fully resumable.
#
# Output:
# - PostgreSQL table  : stage2_vessel_tracks  (full position records)
# - PostgreSQL table  : stage2_vessel_dates   (completion log for resume)
# - CSV file          : stage2_vessel_tracks_YEAR.csv
#
# Usage:
#   python stage2_vessel_fetch_tracks.py
#   python stage2_vessel_fetch_tracks.py --year 2024
#   python stage2_vessel_fetch_tracks.py --dry-run
#
# See: stage1_vessel_tripwire.py  -- Stage 1: produces stage1_vessel_hits.

import argparse
import csv
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras

from pipeline_common import DB_CONFIG, conn

DEFAULT_YEAR    = 2024
DEFAULT_CSV_DIR = Path("/mnt/e/data_lake/vessels")

CSV_HEADER = [
    "mms_id", "vessel_date",
    "time_utc",
    "latitude", "longitude",
    "speed_over_ground", "course_over_ground", "heading",
    "vessel_name", "vessel_type", "navigation_status",
]


# DDL

_CREATE_TRACKS = """
CREATE TABLE IF NOT EXISTS stage2_vessel_tracks (
    mms_id             TEXT             NOT NULL,
    vessel_date        DATE             NOT NULL,
    time_utc           TIMESTAMPTZ      NOT NULL,
    latitude           DOUBLE PRECISION,
    longitude          DOUBLE PRECISION,
    speed_over_ground  DOUBLE PRECISION,
    course_over_ground DOUBLE PRECISION,
    heading            SMALLINT,
    vessel_name        TEXT,
    vessel_type        SMALLINT,
    navigation_status  SMALLINT,
    PRIMARY KEY (mms_id, time_utc)
);
"""

_CREATE_TRACKS_IDX = """
CREATE INDEX IF NOT EXISTS idx_stage2_vessel_tracks_date
    ON stage2_vessel_tracks (vessel_date, mms_id);
"""

_CREATE_DATES = """
CREATE TABLE IF NOT EXISTS stage2_vessel_dates (
    vessel_date  DATE        PRIMARY KEY,
    year         SMALLINT    NOT NULL,
    n_vessels    INTEGER     NOT NULL,
    n_positions  INTEGER     NOT NULL,
    elapsed_s    REAL        NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_UPSERT_TRACKS = """
INSERT INTO stage2_vessel_tracks
    (mms_id, vessel_date, time_utc, latitude, longitude,
     speed_over_ground, course_over_ground, heading,
     vessel_name, vessel_type, navigation_status)
VALUES %s
ON CONFLICT (mms_id, time_utc) DO NOTHING;
"""

_INSERT_DATE = """
INSERT INTO stage2_vessel_dates
    (vessel_date, year, n_vessels, n_positions, elapsed_s)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (vessel_date) DO NOTHING;
"""


# Helpers

def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_CREATE_TRACKS)
        cur.execute(_CREATE_TRACKS_IDX)
        cur.execute(_CREATE_DATES)
    conn.commit()


def load_stage1_hits(
    conn,
    year: int,
    min_positions: int,
    highest_min_speed: float | None,
    ne_incremental: bool = False,
    mmsi: str | None = None,
) -> dict[date, list[str]]:
    """
    Return Stage 1 hits grouped by vessel_date for the given year.

    ne_incremental: If True, only load hits with chunk_label LIKE 'ne_%'
                           (newly classified offshore turbines).
    mmsi: If set, only return hits for this MMSI.
    """
    conditions = ["EXTRACT(YEAR FROM vessel_date) = %s"]
    params: list = [year]
    if min_positions > 1:
        conditions.append("n_positions >= %s")
        params.append(min_positions)
    if highest_min_speed is not None:
        conditions.append("min_speed IS NOT NULL AND min_speed <= %s")
        params.append(highest_min_speed)
    if ne_incremental:
        conditions.append("chunk_label LIKE %s")
        params.append("ne_%")
    if mmsi:
        conditions.append("mms_id = %s")
        params.append(mmsi)

    sql = ("SELECT vessel_date, mms_id FROM stage1_vessel_hits "
           f"WHERE {' AND '.join(conditions)} ORDER BY vessel_date, mms_id")

    with conn.cursor() as cur:
        cur.execute(sql, params)
        by_date: dict[date, list[str]] = defaultdict(list)
        for row in cur.fetchall():
            by_date[row[0]].append(row[1])
    return dict(by_date)


def load_completed_dates(conn) -> set[date]:
    with conn.cursor() as cur:
        cur.execute("SELECT vessel_date FROM stage2_vessel_dates")
        return {row[0] for row in cur.fetchall()}


def filter_already_fetched(conn, by_date: dict[date, list[str]]) -> dict[date, list[str]]:
    """Remove mms_ids that already have tracks in stage2_vessel_tracks."""
    result: dict[date, list[str]] = {}
    for vessel_date, mms_ids in by_date.items():
        start_dt = datetime(vessel_date.year, vessel_date.month, vessel_date.day,
                            tzinfo=timezone.utc) - timedelta(days=1)
        end_dt   = start_dt + timedelta(days=3)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT mms_id FROM stage2_vessel_tracks "
                "WHERE time_utc >= %s AND time_utc < %s AND mms_id = ANY(%s)",
                (start_dt, end_dt, mms_ids),
            )
            already = {row[0] for row in cur.fetchall()}
        new_ids = [m for m in mms_ids if m not in already]
        if new_ids:
            result[vessel_date] = new_ids
    return result


def fetch_day_tracks(conn, vessel_date: date, mms_ids: list[str]) -> list[tuple]:
    """
    Fetch full tracks for the given vessels on the given date ±1 day.

    The ±1-day window captures voyages that cross UTC midnight.

    conn: Active psycopg2 connection.
    vessel_date: The hit date from Stage 1.
    mms_ids: List of mms_id strings to fetch.
    Returns: List of raw result tuples.
    """
    start_dt = datetime(vessel_date.year, vessel_date.month, vessel_date.day,
                        tzinfo=timezone.utc) - timedelta(days=1)
    end_dt   = start_dt + timedelta(days=3)   # day-1 to day+2 (exclusive)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                mms_id, time, latitude, longitude,
                speed_over_ground, course_over_ground, heading,
                vessel_name, vessel_type, navigation_status
            FROM vessel_data_ais
            WHERE time >= %s AND time < %s
              AND mms_id = ANY(%s)
            ORDER BY mms_id, time
            """,
            (start_dt, end_dt, mms_ids),
        )
        return cur.fetchall()


def save_date_results(
    conn,
    vessel_date: date,
    year: int,
    rows: list[tuple],
    elapsed_s: float,
    mms_ids: list[str],
) -> int:
    """
    Insert Stage 2 position rows and mark the date as complete.

    conn: Active psycopg2 connection.
    vessel_date: The date that was fetched.
    year: Calendar year.
    rows: Raw query result rows.
    elapsed_s: Query elapsed time in seconds.
    mms_ids: List of mms_ids that were queried.
    Returns: Number of rows inserted.
    """
    tuples = [
        (mms_id, vessel_date, time_utc, lat, lon,
         sog, cog, heading, vessel_name, vessel_type, nav_status)
        for mms_id, time_utc, lat, lon, sog, cog, heading,
            vessel_name, vessel_type, nav_status in rows
    ]

    with conn.cursor() as cur:
        if tuples:
            psycopg2.extras.execute_values(cur, _UPSERT_TRACKS, tuples)
        cur.execute(
            _INSERT_DATE,
            (vessel_date, year, len(mms_ids), len(tuples), elapsed_s),
        )
    conn.commit()
    return len(tuples)


def append_csv(path: Path, rows: list[tuple], vessel_date: date) -> None:
    write_header = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(CSV_HEADER)
        for (mms_id, time_utc, lat, lon, sog, cog,
             heading, vessel_name, vessel_type, nav_status) in rows:
            w.writerow([
                mms_id, vessel_date.isoformat(),
                time_utc.isoformat() if time_utc else None,
                lat, lon, sog, cog, heading,
                vessel_name, vessel_type, nav_status,
            ])


# Entry point

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stage 2 — fetch full-day tracks for vessel Stage 1 hits.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s --year 2024
  %(prog)s --dry-run
        """,
    )
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR, metavar="YYYY",
                        help=f"Year to process (default: {DEFAULT_YEAR})")
    parser.add_argument("--min-positions", metavar="N", type=int, default=5,
                        help="Minimum n_positions a Stage 1 hit must have (default: 5).")
    parser.add_argument("--highest-min-speed", metavar="KT", type=float, default=5,
                        help="Only fetch tracks for vessels whose minimum recorded speed "
                             "near a turbine was <= KT knots. Default: 5 kt "
                             "(keeps ~73%% of hits; cuts transit vessels above 5 kt).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be fetched without querying")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_CSV_DIR, metavar="DIR",
                        help=f"CSV output directory (default: {DEFAULT_CSV_DIR})")
    parser.add_argument("--ne-incremental", action="store_true",
                        help="Only fetch tracks for vessels from the NE-incremental Stage 1 run "
                             "(chunk_label LIKE 'ne_%%'). Skips vessels already in stage2_vessel_tracks.")

    test_group = parser.add_argument_group("single-candidate test mode")
    test_group.add_argument("--mmsi", metavar="ID",
                            help="Filter to a single vessel by MMSI")
    test_group.add_argument("--single-date", metavar="YYYY-MM-DD",
                            help="Fetch only this specific date")
    test_group.add_argument("--limit", metavar="N", type=int,
                            help="Process only the first N (mms_id, date) pairs")
    args = parser.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)
    ensure_tables(conn)

    hits_by_date = load_stage1_hits(
        conn, args.year, args.min_positions, args.highest_min_speed,
        ne_incremental=args.ne_incremental,
        mmsi=args.mmsi,
    )

    # --single-date: keep only hits for that date
    if args.single_date:
        sd = date.fromisoformat(args.single_date)
        hits_by_date = {d: v for d, v in hits_by_date.items() if d == sd}

    if not hits_by_date:
        print(f"No Stage 1 hits found for {args.year} with current filters.")
        print(f"  --min-positions {args.min_positions}"
              + (f"  --highest-min-speed {args.highest_min_speed} kt"
                 if args.highest_min_speed is not None else ""))
        print(f"Run stage1_vessel_tripwire.py first.")
        conn.close()
        return 1

    if args.ne_incremental:
        print("NE-incremental mode: filtering out vessels already in stage2_vessel_tracks...")
        pending = filter_already_fetched(conn, hits_by_date)
    else:
        completed = load_completed_dates(conn)
        pending   = {d: v for d, v in sorted(hits_by_date.items())
                     if d not in completed}

    total_dates   = len(hits_by_date)
    total_vessels = sum(len(v) for v in hits_by_date.values())

    print(f"Year             : {args.year}")
    print(f"Pre-filters      : min_positions >= {args.min_positions}"
          + (f"  |  min_speed <= {args.highest_min_speed} kt"
             if args.highest_min_speed is not None else ""))
    print(f"Stage 1 hits     : {total_vessels} (mms_id, date) pairs across "
          f"{total_dates} dates")
    if args.ne_incremental:
        print(f"Pending          : {len(pending)} dates with new vessels")
    else:
        print(f"Already fetched  : {len(completed)} dates")
        print(f"Pending          : {len(pending)} dates")

    if args.dry_run:
        print("\nDRY RUN — dates that would be fetched:")
        for d, mms_ids in sorted(pending.items()):
            print(f"  {d}  vessels: {', '.join(mms_ids)}")
        conn.close()
        return 0

    # --limit: truncate pending to first N (date, vessel) pairs
    if args.limit and pending:
        limited = {}
        count = 0
        for d in sorted(pending):
            for mmsi_id in pending[d]:
                if count >= args.limit:
                    break
                limited.setdefault(d, []).append(mmsi_id)
                count += 1
            if count >= args.limit:
                break
        pending = limited
        print(f"Limit            : first {args.limit} pairs → {len(pending)} dates")

    if not pending:
        print("\nAll dates already fetched. Nothing to do.")
        conn.close()
        return 0

    csv_suffix  = f"ne_{args.year}" if args.ne_incremental else str(args.year)
    csv_path    = args.out_dir / f"stage2_vessel_tracks_{csv_suffix}.csv"
    total_pos   = 0
    n_done      = 0
    elapsed_log = []
    run_start   = time.monotonic()

    for vessel_date, mms_ids in sorted(pending.items()):
        n_done += 1
        eta_str = ""
        if n_done > 1:
            avg_s = (time.monotonic() - run_start) / (n_done - 1)
            remaining_s = avg_s * (len(pending) - n_done + 1)
            eta_str = f"  ETA ≈ {remaining_s / 60:.1f} min"
        print(f"\n[{n_done}/{len(pending)}]  {vessel_date}  "
              f"({len(mms_ids)} vessels: {', '.join(mms_ids)}){eta_str}", flush=True)

        t0      = time.monotonic()
        rows    = fetch_day_tracks(conn, vessel_date, mms_ids)
        elapsed = time.monotonic() - t0

        elapsed_log.append(elapsed)
        n_saved = save_date_results(conn, vessel_date, args.year,
                                    rows, elapsed, mms_ids)
        append_csv(csv_path, rows, vessel_date)
        total_pos += n_saved

        avg_t   = sum(elapsed_log) / len(elapsed_log)
        eta_min = avg_t * (len(pending) - n_done) / 60
        eta_str = f"  ETA ~{eta_min:.0f} min" if n_done < len(pending) else ""
        print(f"  → {n_saved:,} positions saved  [{elapsed:.1f}s]{eta_str}")

    print(f"\n{'═' * 62}")
    print(f"  Stage 2 complete")
    print(f"  Dates fetched   : {n_done}")
    print(f"  Total positions : {total_pos:,}")
    print(f"  DB table        : stage2_vessel_tracks")
    print(f"  CSV             : {csv_path}")
    print(f"{'═' * 62}")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
