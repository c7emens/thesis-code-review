#!/usr/bin/env python3
# Calibrate OpenSky Trino Stage 1 query performance.
#
# Runs timed test queries against the OpenSky Trino database to determine the
# optimal time-chunk size for Stage 1 (tripwire) queries before committing to
# a full-year helicopter fetch.
#
# Builds the per-turbine bounding-box WHERE clause dynamically from the local
# wind_turbines table — no hardcoded coordinates required.
#
# Output:
# - Live progress bar (splits completed, rows scanned, GB processed)
# - Query execution time for the selected test window
# - Extrapolated times for monthly and quarterly chunks
# - Chunk-size recommendation based on the 30-minute Trino timeout
# - Per-ICAO breakdown with position counts and altitude range
# - Optional outer-bbox pre-filter comparison
#
# Usage:
#   python calibrate_trino_stage1.py --user you@example.com
#   python calibrate_trino_stage1.py --user you@example.com --weeks 2
#   python calibrate_trino_stage1.py --user you@example.com --no-outer-bbox
#   python calibrate_trino_stage1.py --user you@example.com --project Vineyard_Wind

import argparse
import os
import signal
from dotenv import load_dotenv; load_dotenv("/mnt/d/thesis/.env")
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import trino
import trino.auth
from opensky_auth import make_oauth2_auth


# Local database

## Default TimescaleDB connection parameters.
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "windfarm",
    "user":     "thesis",
    "password": "thesis2026",
}

# OpenSky Trino

## OpenSky Trino gateway hostname.
OPENSKY_HOST    = "trino.opensky-network.org"
## OpenSky Trino HTTPS port.
OPENSKY_PORT    = 443
## Trino catalog for OpenSky data.
OPENSKY_CATALOG = "minio"
## Trino schema for OpenSky data.
OPENSKY_SCHEMA  = "osky"

# Query parameters

## Altitude ceiling for low-altitude filter (metres; 609 m ≈ 2,000 ft).
MAX_ALT_M = 609

## Per-turbine bbox half-width in latitude degrees (≈ 2 km).
BBOX_LAT = 0.018

## Per-turbine bbox half-width in longitude degrees (≈ 2 km at 41°N).
BBOX_LON = 0.024


# Turbine helpers

def load_turbines(conn, project: str | None) -> list[tuple[float, float, str]]:
    """
    Load turbine coordinates from the local wind_turbines table.

    conn: Active psycopg2 connection.
    project: Restrict to this project_name, or None for all farms.
    Returns: List of (latitude, longitude, project_name) tuples.
    """
    cur = conn.cursor()
    if project:
        cur.execute(
            "SELECT latitude, longitude, project_name FROM wind_turbines "
            "WHERE project_name = %s ORDER BY latitude, longitude",
            (project,),
        )
    else:
        cur.execute(
            "SELECT latitude, longitude, project_name FROM wind_turbines "
            "ORDER BY project_name, latitude, longitude"
        )
    rows = cur.fetchall()
    cur.close()
    return rows


def build_turbine_conditions(turbines: list[tuple]) -> str:
    """
    Build a SQL OR block with one bounding-box condition per turbine.

    Each rectangle approximates a 2 km radius centred on the turbine at 41°N.

    turbines: List of (lat, lon, project) tuples.
    Returns: Multi-line SQL fragment for use inside AND ( ... ).
    """
    parts = [
        f"    (lat BETWEEN {lat - BBOX_LAT:.6f} AND {lat + BBOX_LAT:.6f}"
        f" AND lon BETWEEN {lon - BBOX_LON:.6f} AND {lon + BBOX_LON:.6f})"
        for lat, lon, _ in turbines
    ]
    return "\n  OR\n".join(parts)


def build_outer_bbox(turbines: list[tuple], buffer: float = 0.05) -> dict:
    """
    Compute a coarse bounding box covering all turbines plus a buffer.

    Used as a fast pre-filter before the per-turbine OR conditions. Trino may
    use Parquet row-group statistics to skip files that fall entirely outside
    this range.

    turbines: List of (lat, lon, project) tuples.
    buffer: Extra margin in degrees.
    Returns: Dict with lat_min, lat_max, lon_min, lon_max.
    """
    lats = [t[0] for t in turbines]
    lons = [t[1] for t in turbines]
    return {
        "lat_min": min(lats) - buffer,
        "lat_max": max(lats) + buffer,
        "lon_min": min(lons) - buffer,
        "lon_max": max(lons) + buffer,
    }


# Query builders

def _outer_clause(outer_bbox: dict | None) -> str:
    """
    Return the coarse-bbox SQL lines, or an empty string if disabled.

    outer_bbox: Dict from build_outer_bbox(), or None.
    Returns: Two-line SQL fragment or empty string.
    """
    if not outer_bbox:
        return ""
    return (
        f"  AND lat BETWEEN {outer_bbox['lat_min']:.4f} AND {outer_bbox['lat_max']:.4f}\n"
        f"  AND lon BETWEEN {outer_bbox['lon_min']:.4f} AND {outer_bbox['lon_max']:.4f}\n"
    )


def build_calibration_query(h_start, h_end, turbine_conditions, outer_bbox) -> str:
    """
    Build the Stage 1 COUNT calibration query.

    h_start: Window start as Unix epoch integer.
    h_end: Window end as Unix epoch integer (exclusive).
    turbine_conditions: Per-turbine OR block from build_turbine_conditions().
    outer_bbox: Coarse bbox dict or None.
    Returns: Trino SQL string.
    """
    return (
        f"SELECT\n"
        f"    COUNT(DISTINCT icao24)      AS unique_aircraft,\n"
        f"    COUNT(*)                    AS total_positions,\n"
        f"    MIN(FROM_UNIXTIME(time))    AS first_seen,\n"
        f"    MAX(FROM_UNIXTIME(time))    AS last_seen\n"
        f"FROM {OPENSKY_CATALOG}.{OPENSKY_SCHEMA}.state_vectors_data4\n"
        f"WHERE hour  >= {h_start}\n"
        f"  AND hour  <  {h_end}\n"
        f"  AND baroaltitude < {MAX_ALT_M}\n"
        f"  AND onground = false\n"
        f"{_outer_clause(outer_bbox)}"
        f"  AND (\n{turbine_conditions}\n  )"
    )


def build_icao_query(h_start, h_end, turbine_conditions, outer_bbox) -> str:
    """
    Build the per-ICAO breakdown query for the test window.

    h_start: Window start as Unix epoch integer.
    h_end: Window end as Unix epoch integer (exclusive).
    turbine_conditions: Per-turbine OR block.
    outer_bbox: Coarse bbox dict or None.
    Returns: Trino SQL string.
    """
    return (
        f"SELECT\n"
        f"    icao24,\n"
        f"    COUNT(*)                          AS positions,\n"
        f"    ROUND(MIN(baroaltitude))          AS min_alt_m,\n"
        f"    ROUND(MAX(baroaltitude))          AS max_alt_m,\n"
        f"    ROUND(AVG(velocity))              AS avg_speed_ms,\n"
        f"    MIN(FROM_UNIXTIME(time))          AS first_seen,\n"
        f"    MAX(FROM_UNIXTIME(time))          AS last_seen\n"
        f"FROM {OPENSKY_CATALOG}.{OPENSKY_SCHEMA}.state_vectors_data4\n"
        f"WHERE hour  >= {h_start}\n"
        f"  AND hour  <  {h_end}\n"
        f"  AND baroaltitude < {MAX_ALT_M}\n"
        f"  AND onground = false\n"
        f"{_outer_clause(outer_bbox)}"
        f"  AND (\n{turbine_conditions}\n  )\n"
        f"GROUP BY icao24\n"
        f"ORDER BY positions DESC"
    )


# Progress bar

## Sentinel placed in a cursor slot when its query finishes.
_DONE = object()


def _format_line(slot: list, elapsed: float) -> str:
    """
    Format one short progress line (no label) for \\r-overwrite.

    Omits the query label — the separator header already shows it.  Keeps the
    total width under ~85 chars so it never wraps on a standard terminal.

    slot: Single-element list: active cursor, None, or _DONE.
    elapsed: Seconds since heartbeat start.
    Returns: Formatted string, no trailing newline.
    """
    cur = slot[0]
    if cur is None:
        return f"  [{elapsed:>5.1f}s] waiting..."
    if cur is _DONE:
        return f"  [{elapsed:>5.1f}s] ✓ done"
    stats = cur.stats
    if stats is None:
        msg = "submitted, waiting for cluster..."
    else:
        total_s = stats.get("totalSplits", 0)
        done_s  = stats.get("completedSplits", 0)
        rows    = stats.get("processedRows", 0)
        gb      = stats.get("processedBytes", 0) / 1e9
        if total_s > 0:
            pct = 100 * done_s / total_s
            bar = ("█" * int(pct / 5)).ljust(20, "░")
            msg = (f"{bar} {pct:5.1f}%  "
                   f"{done_s}/{total_s} splits  "
                   f"{rows / 1e6:.1f}M rows  {gb:.2f} GB")
        else:
            msg = stats.get("state", "?")
    return f"  [{elapsed:>5.1f}s] {msg}"


def start_heartbeat(
    slot: list,
    t0: float,
) -> tuple[threading.Event, threading.Thread]:
    """
    Start a background thread that \\r-overwrites one progress line.

    Stays silent while cur.stats is None (auth / submission phase) so the
    OAuth URL is never overwritten.  Once stats appear, overwrites the same
    terminal line every 5 s using the same print(\\r..., end="") pattern as
    fetch_opensky_helicopters.py.

    slot: Single-element list: active cursor, or _DONE when finished.
    t0: Batch start time from time.monotonic().
    Returns: (stop_event, thread) pair.
    """
    stop = threading.Event()

    def _run():
        while not stop.is_set():
            elapsed = time.monotonic() - t0
            cur     = slot[0]

            if cur is not None and cur is not _DONE and cur.stats is None:
                stop.wait(2)
                continue

            print(f"\r{_format_line(slot, elapsed):<80}", end="", flush=True)
            stop.wait(5)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return stop, t


# Query runner

def run_timed_query(
    sky_conn,
    sql: str,
    label: str,
    active_cursor: list,
) -> tuple[list, float]:
    """
    Execute a Trino query with live progress bar, Ctrl+C cancel,
           and QUERY_QUEUE_FULL retry.

    Retries up to 5 times with 60-second pauses when the Trino queue is full
    (e.g. a previous Ctrl+C left a query queued). Cancels the active query
    cleanly on SIGINT.

    sky_conn: Active Trino DBAPI connection.
    sql: SQL string to execute.
    label: Short description shown on the progress bar.
    active_cursor: Single-element list used to expose the cursor to the
                          SIGINT handler so it can be cancelled.
    Returns: Tuple (rows, elapsed_seconds).
    """
    print(f"\n{'-' * 62}")
    print(f"  {label}")
    print(f"{'-' * 62}")

    t0   = time.monotonic()
    slot = [None]
    cur  = sky_conn.cursor()
    slot[0] = cur
    active_cursor[0] = cur

    stop, hb = start_heartbeat(slot, t0)

    # Retry on QUERY_QUEUE_FULL (left over from a previous Ctrl+C).
    for attempt in range(1, 6):
        try:
            cur.execute(sql)
            break
        except Exception as exc:
            if "QUERY_QUEUE_FULL" in str(exc) and attempt < 5:
                stop.set(); hb.join()
                print(f"  ⚠  Queue full — retrying in 60 s (attempt {attempt}/5)...",
                      flush=True)
                time.sleep(60)
                cur = sky_conn.cursor()
                slot[0] = cur
                active_cursor[0] = cur
                stop, hb = start_heartbeat(slot, time.monotonic())
            else:
                stop.set(); hb.join()
                raise

    slot[0] = _DONE
    stop.set()
    hb.join()
    print()   # blank line after progress row

    elapsed = time.monotonic() - t0
    print(f"  Query ID : {cur.query_id}")

    stats = cur.stats or {}
    scanned_rows  = stats.get("processedRows", 0)
    scanned_bytes = stats.get("processedBytes", 0)
    print(f"  Scanned  : {scanned_rows / 1e9:.2f} B rows  "
          f"/ {scanned_bytes / 1e9:.1f} GB  "
          f"in {elapsed:.1f} s")

    # fetchmany loop with heartbeat so terminal doesn't go silent on large results
    rows = []
    while True:
        chunk = cur.fetchmany(10_000)
        if not chunk:
            break
        rows.extend(chunk)

    cur.close()
    active_cursor[0] = None
    return rows, elapsed


# Recommendation

def print_recommendation(elapsed_s: float, test_weeks: int) -> None:
    """
    Extrapolate query time and recommend the optimal chunk size.

    Assumes linear scaling with time range — valid because Trino scans a fixed
    number of partitions per unit time regardless of result size.

    elapsed_s: Measured query time in seconds.
    test_weeks: Number of weeks in the test window.
    """
    per_week    = elapsed_s / test_weeks
    per_2week   = per_week * 2
    per_month   = per_week * 4.33
    per_quarter = per_week * 13.0
    LIMIT       = 1500   # 25 min — safe margin below 30-min timeout

    print(f"\n{'═' * 62}")
    print("  EXTRAPOLATION  (assumes linear scaling)")
    print(f"{'═' * 62}")
    print(f"  Per week      : {per_week:>6.0f} s  ({per_week/60:.1f} min)")
    print(f"  Per 2 weeks   : {per_2week:>6.0f} s  ({per_2week/60:.1f} min)")
    print(f"  Per month     : {per_month:>6.0f} s  ({per_month/60:.1f} min)")
    print(f"  Per quarter   : {per_quarter:>6.0f} s  ({per_quarter/60:.1f} min)")

    print(f"\n  RECOMMENDATION for full-year 2024 (2 concurrent queries):")
    if per_quarter < LIMIT:
        batches = 2
        total   = batches * per_quarter
        chunk   = "QUARTERLY"
        n       = 4
        t_each  = per_quarter
    elif per_month < LIMIT:
        batches = 6
        total   = batches * per_month
        chunk   = "MONTHLY"
        n       = 12
        t_each  = per_month
    else:
        batches = 13
        total   = batches * per_2week
        chunk   = "BI-WEEKLY"
        n       = 26
        t_each  = per_2week

    print(f"  → Use {chunk} chunks")
    print(f"    {n} queries × {t_each/60:.1f} min, "
          f"{batches} batches of 2 concurrent")
    print(f"    Estimated total Stage 1 time: {total/60:.0f} min")
    print(f"{'═' * 62}")


# Entry point

def main() -> int:
    """
    Command-line entry point.

    Returns: Exit code: 0 on success, 1 on error.
    """
    parser = argparse.ArgumentParser(
        description="Calibrate OpenSky Trino Stage 1 query performance.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --user you@example.com
  %(prog)s --user you@example.com --weeks 2
  %(prog)s --user you@example.com --no-outer-bbox
  %(prog)s --user you@example.com --project Vineyard_Wind
        """,
    )
    parser.add_argument("--user",          metavar="EMAIL",
                        default=os.environ.get("OPENSKY_USERNAME"),
                        help="OpenSky username. Defaults to $OPENSKY_USERNAME (from /mnt/d/thesis/.env). Password auth also reads $OPENSKY_PASS or $OPENSKY_PASSWORD from .env.")
    parser.add_argument("--weeks",         metavar="N",   type=int, default=1,
                        help="Test window width in weeks (default: 1)")
    parser.add_argument("--start",         metavar="DATE", default="2024-01-01",
                        help="Test window start date (default: 2024-01-01)")
    parser.add_argument("--project",       metavar="NAME",
                        help="Restrict to one project (default: all farms)")
    parser.add_argument("--no-outer-bbox", action="store_true",
                        help="Disable the coarse outer-bbox pre-filter")
    parser.add_argument("--skip-icao",     action="store_true",
                        help="Skip the per-ICAO breakdown query")
    args = parser.parse_args()

    # Load turbines
    local_conn = psycopg2.connect(**DB_CONFIG)
    turbines   = load_turbines(local_conn, args.project)
    local_conn.close()

    if not turbines:
        print(f"Error: no turbines found"
              + (f" for project '{args.project}'" if args.project else ""),
              file=sys.stderr)
        return 1

    projects = sorted({t[2] for t in turbines})
    print(f"Turbines loaded : {len(turbines)} across {len(projects)} farm(s): "
          f"{', '.join(projects)}")

    turbine_conditions = build_turbine_conditions(turbines)
    outer_bbox = None if args.no_outer_bbox else build_outer_bbox(turbines)

    if outer_bbox:
        print(f"Outer bbox      : lat [{outer_bbox['lat_min']:.3f}, "
              f"{outer_bbox['lat_max']:.3f}]  "
              f"lon [{outer_bbox['lon_min']:.3f}, {outer_bbox['lon_max']:.3f}]")
    else:
        print("Outer bbox      : disabled")

    # Build time window
    t_start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    t_end   = t_start + timedelta(weeks=args.weeks)
    h_start = int(t_start.timestamp())
    h_end   = int(t_end.timestamp())
    print(f"Test window     : {t_start.date()} → {t_end.date()} ({args.weeks} week(s))")

    if not args.user:
        print("Error: OpenSky username required — set OPENSKY_USERNAME in .env or pass --user",
              file=sys.stderr)
        return 1

    # Connect to Trino
    print("\nConnecting to OpenSky Trino...")
    auth = make_oauth2_auth()
    print("⚠  If this is your first run today, an OAuth URL will appear — open it once.\n"
          "   Subsequent runs reuse the cached token (~24 h validity).\n")

    sky_conn = trino.dbapi.connect(
        host            = OPENSKY_HOST,
        port            = OPENSKY_PORT,
        http_scheme     = "https",
        user            = args.user,
        auth            = auth,
        catalog         = OPENSKY_CATALOG,
        schema          = OPENSKY_SCHEMA,
        request_timeout = 1800,
    )

    # Cancel active query on Ctrl+C instead of leaving it queued.
    _active_cursor = [None]

    def _sigint(sig, frame):
        cur = _active_cursor[0]
        if cur is not None:
            try:
                print("\n  Cancelling Trino query...", flush=True)
                cur.cancel()
            except Exception:
                pass
        sys.exit(1)

    signal.signal(signal.SIGINT, _sigint)

    # Calibration query
    calib_sql = build_calibration_query(h_start, h_end, turbine_conditions, outer_bbox)
    bbox_label = "with outer bbox" if outer_bbox else "no outer bbox"
    calib_rows, elapsed = run_timed_query(
        sky_conn, calib_sql,
        f"Stage 1 calibration — {args.weeks}w, {bbox_label}",
        _active_cursor,
    )

    if calib_rows:
        unique_ac, total_pos, first_seen, last_seen = calib_rows[0]
        print(f"\n  Results:")
        print(f"    Unique aircraft : {unique_ac}")
        print(f"    Total positions : {total_pos:,}")
        print(f"    First seen      : {first_seen}")
        print(f"    Last seen       : {last_seen}")

    print_recommendation(elapsed, args.weeks)

    # ICAO breakdown
    if not args.skip_icao and calib_rows and calib_rows[0][0] > 0:
        icao_sql  = build_icao_query(h_start, h_end, turbine_conditions, outer_bbox)
        icao_rows, _ = run_timed_query(
            sky_conn, icao_sql,
            "ICAO breakdown — same window",
            _active_cursor,
        )

        if icao_rows:
            print(f"\n  Aircraft found near turbines ({len(icao_rows)} unique):")
            print(f"  {'ICAO':<10} {'Pos':>7} {'MinAlt m':>9} {'MaxAlt m':>9} "
                  f"{'AvgSpd m/s':>11}  First seen")
            print(f"  {'-'*10} {'-'*7} {'-'*9} {'-'*9} {'-'*11}  {'-'*20}")
            for row in icao_rows:
                icao, pos, min_alt, max_alt, avg_spd, first, _ = row
                spd_str = f"{avg_spd:>11.1f}" if avg_spd is not None else f"{'N/A':>11}"
                print(f"  {icao:<10} {pos:>7,} {min_alt!s:>9} {max_alt!s:>9} "
                      f"{spd_str}  {first}")
        else:
            print("  No aircraft found in this window — try --no-outer-bbox "
                  "or a later date range.")

    sky_conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
