#!/usr/bin/env python3
# Stage 2 — fetch full-day tracks for every (icao24, date) hit from Stage 1.
#
# Reads the stage1_helicopter_hits table and, for each unique flight_date, queries
# state_vectors_data4 for the full day track of every icao24 that appeared
# near a turbine on that date.  No bbox or altitude filter is applied so the
# complete trajectory (port → turbine → port) is captured.
#
# Queries are grouped by date (one Trino query per unique flight_date), which
# keeps each query small — a typical day with 2–5 aircraft returns <<1 M rows
# and completes in under 2 minutes.
#
# Already-fetched dates are recorded in stage2_helicopter_dates and skipped on re-run,
# making the script fully resumable.
#
# Output:
# - PostgreSQL table  : stage2_helicopter_tracks   (full position records)
# - PostgreSQL table  : stage2_helicopter_dates    (completion log for resume)
# - CSV file          : stage2_helicopter_tracks_YEAR.csv
#
# Usage:
#   python fetch_stage2_helicopter_tracks.py
#   python fetch_stage2_helicopter_tracks.py --year 2024
#   python fetch_stage2_helicopter_tracks.py --dry-run
#
# See: run_helicopter_stage1_tripwire.py  -- Stage 1: produces stage1_helicopter_hits.

import argparse
import csv
import os
import signal
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from dotenv import load_dotenv; load_dotenv("/mnt/d/thesis/.env")
from pathlib import Path
import threading
import time

import psycopg2
import psycopg2.extras
import trino
import trino.auth
from opensky_auth import make_oauth2_auth

from pipeline_common import DB_CONFIG, conn

# OpenSky Trino

OPENSKY_HOST    = "trino.opensky-network.org"
OPENSKY_PORT    = 443
OPENSKY_CATALOG = "minio"
OPENSKY_SCHEMA  = "osky"

# Output

DEFAULT_YEAR    = 2024
DEFAULT_CSV_DIR = Path("/mnt/e/data_lake/helicopters")

CSV_HEADER = [
    "icao24", "flight_date",
    "time_unix", "time_utc",
    "lat", "lon",
    "baro_alt_m", "velocity_ms", "heading",
    "onground",
]


# DDL

_CREATE_TRACKS = """
CREATE TABLE IF NOT EXISTS stage2_helicopter_tracks (
    icao24      TEXT             NOT NULL,
    flight_date DATE             NOT NULL,
    time_unix   BIGINT           NOT NULL,
    time_utc    TIMESTAMPTZ      NOT NULL,
    lat         DOUBLE PRECISION,
    lon         DOUBLE PRECISION,
    baro_alt_m  DOUBLE PRECISION,
    velocity_ms DOUBLE PRECISION,
    heading     DOUBLE PRECISION,
    onground    BOOLEAN,
    PRIMARY KEY (icao24, time_unix)
);
"""

_CREATE_TRACKS_IDX = """
CREATE INDEX IF NOT EXISTS idx_stage2_helicopter_tracks_date
    ON stage2_helicopter_tracks (flight_date, icao24);
"""

_CREATE_DATES = """
CREATE TABLE IF NOT EXISTS stage2_helicopter_dates (
    flight_date  DATE     PRIMARY KEY,
    year         SMALLINT NOT NULL,
    n_icao24s    INTEGER  NOT NULL,
    n_positions  INTEGER  NOT NULL,
    elapsed_s    REAL     NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_UPSERT_TRACKS = """
INSERT INTO stage2_helicopter_tracks
    (icao24, flight_date, time_unix, time_utc, lat, lon,
     baro_alt_m, velocity_ms, heading, onground)
VALUES %s
ON CONFLICT (icao24, time_unix) DO NOTHING;
"""

_INSERT_DATE = """
INSERT INTO stage2_helicopter_dates
    (flight_date, year, n_icao24s, n_positions, elapsed_s)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (flight_date) DO NOTHING;
"""


# Query builder

def build_day_query(flight_date: date, icao24s: list[str]) -> str:
    """
    Build a Trino query for all positions of the given icao24s on one day,
    extended ±1 day to capture flights that cross midnight UTC.

    No altitude or bbox filter — we want the complete trajectory so we can
    identify port departures, turbine visits, and return legs.  The ±1-day
    buffer ensures a flight starting at 23:30 on day N and landing at 00:30
    on day N+1 is captured in full.  Duplicate rows from overlapping windows
    are handled by ON CONFLICT DO NOTHING on insert.

    flight_date: The calendar date to fetch (UTC).
    icao24s: List of ICAO hex strings to include.
    Returns: Trino SQL string.
    """
    day_start = int(datetime(flight_date.year, flight_date.month, flight_date.day,
                             tzinfo=timezone.utc).timestamp())
    day_end   = day_start + 86_400

    icao_list = ", ".join(f"'{i}'" for i in icao24s)
    return (
        f"SELECT\n"
        f"    icao24,\n"
        f"    time,\n"
        f"    lat,\n"
        f"    lon,\n"
        f"    baroaltitude,\n"
        f"    velocity,\n"
        f"    heading,\n"
        f"    onground\n"
        f"FROM {OPENSKY_CATALOG}.{OPENSKY_SCHEMA}.state_vectors_data4\n"
        f"WHERE hour >= {day_start - 86_400}\n"
        f"  AND hour <  {day_end   + 86_400}\n"
        f"  AND icao24 IN ({icao_list})\n"
        f"  AND (baroaltitude < 3000 OR baroaltitude IS NULL)\n"
        f"ORDER BY icao24, time"
    )


# Progress bar

_DONE = object()


def start_heartbeat(slot: list, t0: float) -> tuple[threading.Event, threading.Thread]:
    """
    Start a background thread that \\r-overwrites one progress line.

    Silent while cur.stats is None (auth / submission) so the OAuth URL is
    never overwritten.

    slot: Single-element list: active cursor, or _DONE when finished.
    t0: Query start time from time.monotonic().
    Returns: (stop_event, thread) pair.
    """
    stop = threading.Event()

    def _run():
        while not stop.is_set():
            elapsed = time.monotonic() - t0
            cur     = slot[0]

            if cur is None or (cur is not _DONE and cur.stats is None):
                stop.wait(2)
                continue

            if cur is _DONE:
                msg = "✓ done"
            else:
                stats   = cur.stats
                total_s = stats.get("totalSplits", 0)
                done_s  = stats.get("completedSplits", 0)
                rows    = stats.get("processedRows", 0)
                gb      = stats.get("processedBytes", 0) / 1e9
                if total_s > 0:
                    pct = 100 * done_s / total_s
                    bar = ("█" * int(pct / 5)).ljust(20, "░")
                    msg = (f"{bar} {pct:5.1f}%  "
                           f"{done_s}/{total_s} splits  "
                           f"{rows / 1e6:.2f}M rows  {gb:.3f} GB")
                else:
                    msg = stats.get("state", "?")

            print(f"\r  [{elapsed:>5.1f}s] {msg:<75}", end="", flush=True)
            stop.wait(5)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return stop, t


# Local DB helpers

def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_CREATE_TRACKS)
        try:
            cur.execute(_CREATE_TRACKS_IDX)
        except Exception:
            conn.rollback()
            # Index already exists — safe to continue
        cur.execute(_CREATE_DATES)
    conn.commit()


def load_stage1_helicopter_hits(
    conn,
    year: int,
    min_positions: int,
    highest_min_alt_m: float | None,
    ne_incremental: bool = False,
    icao24: str | None = None,
) -> dict[date, list[str]]:
    """
    Return Stage 1 hits grouped by flight_date, with pre-filters applied.

    ne_incremental: If True, only load hits with chunk_label LIKE 'ne_%'.
    icao24: If set, only return hits for this aircraft.
    """
    conditions = [
        "EXTRACT(YEAR FROM flight_date) = %s",
        "n_positions >= %s",
    ]
    params: list = [year, min_positions]

    if highest_min_alt_m is not None:
        conditions.append("min_alt_m IS NOT NULL AND min_alt_m <= %s")
        params.append(highest_min_alt_m)
    if ne_incremental:
        conditions.append("chunk_label LIKE %s")
        params.append("ne_%")
    if icao24:
        conditions.append("icao24 = %s")
        params.append(icao24)

    sql = (
        "SELECT flight_date, icao24 FROM stage1_helicopter_hits "
        "WHERE " + " AND ".join(conditions) +
        " ORDER BY flight_date, icao24"
    )

    with conn.cursor() as cur:
        cur.execute(sql, params)
        by_date: dict[date, list[str]] = defaultdict(list)
        for row in cur.fetchall():
            by_date[row[0]].append(row[1])
    return dict(by_date)


def filter_already_fetched(conn, by_date: dict[date, list[str]]) -> dict[date, list[str]]:
    """Remove icao24s that already have tracks in stage2_helicopter_tracks
    within ±1 day of the flight_date (lenient, used by --ne-incremental)."""
    result: dict[date, list[str]] = {}
    for flight_date, icao24s in by_date.items():
        start_dt = datetime(flight_date.year, flight_date.month, flight_date.day,
                            tzinfo=timezone.utc) - timedelta(days=1)
        end_dt   = start_dt + timedelta(days=3)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT icao24 FROM stage2_helicopter_tracks "
                "WHERE time_utc >= %s AND time_utc < %s AND icao24 = ANY(%s)",
                (start_dt, end_dt, icao24s),
            )
            already = {row[0] for row in cur.fetchall()}
        new_ids = [i for i in icao24s if i not in already]
        if new_ids:
            result[flight_date] = new_ids
    return result


def filter_already_fetched_exact(conn, by_date: dict[date, list[str]]) -> dict[date, list[str]]:
    """Remove icao24s that already have tracks for the EXACT flight_date
    (used by --per-icao-incremental, for per-day backfill)."""
    result: dict[date, list[str]] = {}
    for flight_date, icao24s in by_date.items():
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT icao24 FROM stage2_helicopter_tracks "
                "WHERE flight_date = %s AND icao24 = ANY(%s)",
                (flight_date, icao24s),
            )
            already = {row[0] for row in cur.fetchall()}
        new_ids = [i for i in icao24s if i not in already]
        if new_ids:
            result[flight_date] = new_ids
    return result


def load_completed_dates(conn) -> set[date]:
    """
    Return the set of flight_dates already recorded in stage2_helicopter_dates.
    conn: Active psycopg2 connection.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT flight_date FROM stage2_helicopter_dates")
        return {row[0] for row in cur.fetchall()}


def save_date_results(
    conn,
    flight_date: date,
    year: int,
    rows: list[tuple],
    elapsed_s: float,
    icao24s: list[str],
) -> int:
    """
    Insert Stage 2 position rows and mark the date as complete.

    conn: Active psycopg2 connection.
    flight_date: The date that was fetched.
    year: Calendar year.
    rows: Raw Trino result rows.
    elapsed_s: Query elapsed time in seconds.
    icao24s: List of icao24s that were queried.
    Returns: Number of rows inserted.
    """
    tuples = []
    for icao24, time_unix, lat, lon, baro_alt, velocity, heading, onground in rows:
        time_utc = datetime.fromtimestamp(time_unix, tz=timezone.utc)
        tuples.append((
            icao24,
            flight_date,
            time_unix,
            time_utc,
            lat,
            lon,
            baro_alt,
            velocity,
            heading,
            onground,
        ))

    with conn.cursor() as cur:
        if tuples:
            psycopg2.extras.execute_values(cur, _UPSERT_TRACKS, tuples)
        cur.execute(
            _INSERT_DATE,
            (flight_date, year, len(icao24s), len(tuples), elapsed_s),
        )
    conn.commit()
    return len(tuples)


def append_csv(path: Path, rows: list[tuple], flight_date: date) -> None:
    """
    Append Stage 2 position rows to the output CSV.

    path: Output CSV path.
    rows: Raw Trino result rows for this date.
    flight_date: The flight date (added as a column).
    """
    write_header = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(CSV_HEADER)
        for icao24, time_unix, lat, lon, baro_alt, velocity, heading, onground in rows:
            time_utc = datetime.fromtimestamp(time_unix, tz=timezone.utc).isoformat()
            w.writerow([
                icao24, flight_date.isoformat(),
                time_unix, time_utc,
                lat, lon,
                baro_alt, velocity, heading,
                onground,
            ])


# Entry point

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stage 2 — fetch full-day tracks for Stage 1 hits.",
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
    parser.add_argument("--user", metavar="EMAIL",
                        default=os.environ.get("OPENSKY_USERNAME"),
                        help="OpenSky username. Defaults to $OPENSKY_USERNAME "
                             "(from /mnt/d/thesis/.env). Password auth also reads "
                             "$OPENSKY_PASS or $OPENSKY_PASSWORD from .env.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be fetched without querying Trino")

    # Stage 1 pre-filters (applied before deciding which dates/aircraft to fetch)
    filter_group = parser.add_argument_group("Stage 1 pre-filters")
    filter_group.add_argument(
        "--min-positions", type=int, default=30, metavar="N",
        help="Minimum n_positions a Stage 1 hit must have (default: 30). "
             "Lower values include aircraft that barely triggered the tripwire.",
    )
    filter_group.add_argument(
        "--highest-min-alt", type=float, default=200, metavar="M",
        help="Only fetch tracks for aircraft that flew below M metres while near a "
             "turbine (filters on min_alt_m <= M). E.g. --highest-min-alt 150 excludes "
             "traffic that stayed above 150 m — likely transiting, not O&M. "
             "Default: 200 m (keeps ~26%% of hits; cuts high-altitude transit traffic).",
    )

    parser.add_argument("--out-dir", type=Path, default=DEFAULT_CSV_DIR, metavar="DIR",
                        help=f"CSV output directory (default: {DEFAULT_CSV_DIR})")
    parser.add_argument("--per-icao-incremental", action="store_true",
                        help="Fetch only icao24s missing from stage2_helicopter_tracks, "
                             "regardless of stage2_helicopter_dates completion or chunk_label. "
                             "Use this to backfill per-aircraft gaps within already-completed dates.")
    parser.add_argument("--ne-incremental", action="store_true",
                        help="Only fetch tracks for aircraft from the NE-incremental Stage 1 run "
                             "(chunk_label LIKE 'ne_%%'). Skips icao24s already in stage2_helicopter_tracks.")

    test_group = parser.add_argument_group("single-candidate test mode")
    test_group.add_argument("--icao24", metavar="HEX",
                            help="Filter to a single aircraft by ICAO24 hex code")
    test_group.add_argument("--single-date", metavar="YYYY-MM-DD",
                            help="Fetch only this specific date")
    test_group.add_argument("--limit", metavar="N", type=int,
                            help="Process only the first N (icao24, date) pairs")
    args = parser.parse_args()

    if not args.user and not args.dry_run:
        print("Error: OpenSky username required — set OPENSKY_USERNAME in .env or pass --user",
              file=sys.stderr)
        return 1

    # Load Stage 1 hits
    local_conn = psycopg2.connect(**DB_CONFIG)
    ensure_tables(local_conn)

    hits_by_date = load_stage1_helicopter_hits(
        local_conn, args.year, args.min_positions, args.highest_min_alt,
        ne_incremental=args.ne_incremental,
        icao24=args.icao24,
    )

    # --single-date: keep only hits for that date
    if args.single_date:
        sd = date.fromisoformat(args.single_date)
        hits_by_date = {d: v for d, v in hits_by_date.items() if d == sd}

    if not hits_by_date:
        print(f"No Stage 1 hits found for {args.year} with current filters.")
        print(f"  --min-positions {args.min_positions}"
              + (f"  --highest-min-alt {args.highest_min_alt}" if args.highest_min_alt else ""))
        local_conn.close()
        return 1

    if args.per_icao_incremental:
        print("Per-icao incremental: filtering out (icao24,date) pairs already in stage2_helicopter_tracks...")
        pending = filter_already_fetched_exact(local_conn, hits_by_date)
    elif args.ne_incremental:
        print("NE-incremental mode: filtering out aircraft already in stage2_helicopter_tracks...")
        pending = filter_already_fetched(local_conn, hits_by_date)
    else:
        completed = load_completed_dates(local_conn)
        pending   = {d: v for d, v in sorted(hits_by_date.items()) if d not in completed}

    total_dates = len(hits_by_date)
    total_icao  = sum(len(v) for v in hits_by_date.values())

    print(f"Year             : {args.year}")
    print(f"Pre-filters      : min_positions >= {args.min_positions}"
          + (f"  |  min_alt_m <= {args.highest_min_alt} m" if args.highest_min_alt else ""))
    print(f"Stage 1 hits     : {total_icao} (icao24, date) pairs across {total_dates} dates")
    if args.per_icao_incremental:
        n_pending_pairs = sum(len(v) for v in pending.values())
        print(f"Pending          : {n_pending_pairs} (icao24, date) pairs across {len(pending)} dates")
    elif args.ne_incremental:
        print(f"Pending          : {len(pending)} dates with new aircraft")
    else:
        print(f"Already fetched  : {len(completed)} dates")
        print(f"Pending          : {len(pending)} dates")

    if args.dry_run:
        print("\nDRY RUN — dates that would be fetched:")
        for d, icao24s in sorted(pending.items()):
            print(f"  {d}  icao24s: {', '.join(icao24s)}")
        local_conn.close()
        return 0

    # --limit: truncate pending to first N (icao24, date) pairs
    if args.limit and pending:
        limited = {}
        count = 0
        for d in sorted(pending):
            for icao in pending[d]:
                if count >= args.limit:
                    break
                limited.setdefault(d, []).append(icao)
                count += 1
            if count >= args.limit:
                break
        pending = limited
        print(f"Limit            : first {args.limit} pairs → {len(pending)} dates")

    if not pending:
        print("\nAll dates already fetched. Nothing to do.")
        local_conn.close()
        return 0

    # Connect to Trino
    print(f"\nConnecting to OpenSky Trino...")
    auth = make_oauth2_auth()
    print("⚠  If prompted, open the OAuth URL once. Subsequent queries reuse the cached token.\n")

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

    # SIGINT: cancel active query
    _active_cursor = [None]

    def _sigint(sig, frame):
        cur = _active_cursor[0]
        if cur is not None:
            try:
                cur.cancel()
            except Exception:
                pass
        sys.exit(1)

    signal.signal(signal.SIGINT, _sigint)

    # Fetch loop
    csv_suffix  = f"ne_{args.year}" if args.ne_incremental else str(args.year)
    csv_path    = args.out_dir / f"stage2_helicopter_tracks_{csv_suffix}.csv"
    total_pos   = 0
    n_done      = 0
    elapsed_log = []

    for flight_date, icao24s in sorted(pending.items()):
        n_done += 1
        prev_date = flight_date - timedelta(days=1)
        next_date = flight_date + timedelta(days=1)
        print(f"\n[{n_done}/{len(pending)}]  {prev_date} ← {flight_date} → {next_date}  "
              f"({len(icao24s)} aircraft: {', '.join(icao24s)})")

        sql  = build_day_query(flight_date, icao24s)
        slot = [None]
        cur  = sky_conn.cursor()
        slot[0] = cur
        _active_cursor[0] = cur

        t0         = time.monotonic()
        hb_stop, hb = start_heartbeat(slot, t0)

        for attempt in range(1, 6):
            try:
                cur.execute(sql)
                break
            except Exception as exc:
                if "QUERY_QUEUE_FULL" in str(exc) and attempt < 5:
                    hb_stop.set(); hb.join()
                    print(f"\n  ⚠  Queue full — retrying in 60 s (attempt {attempt}/5)...",
                          flush=True)
                    time.sleep(60)
                    cur = sky_conn.cursor()
                    slot[0] = cur
                    _active_cursor[0] = cur
                    hb_stop, hb = start_heartbeat(slot, t0)
                else:
                    hb_stop.set(); hb.join()
                    raise

        rows = []
        while True:
            chunk = cur.fetchmany(50_000)
            if not chunk:
                break
            rows.extend(chunk)

        slot[0] = _DONE
        hb_stop.set()
        hb.join()
        print()

        elapsed = time.monotonic() - t0
        elapsed_log.append(elapsed)
        cur.close()
        _active_cursor[0] = None

        n_saved = save_date_results(
            local_conn, flight_date, args.year, rows, elapsed, icao24s
        )
        # CSV output disabled — DB only (storage constraint)
        # append_csv(csv_path, rows, flight_date)
        total_pos += n_saved

        avg_t   = sum(elapsed_log) / len(elapsed_log)
        eta_min = avg_t * (len(pending) - n_done) / 60
        eta_str = f"  ETA ~{eta_min:.0f} min" if n_done < len(pending) else ""
        print(f"  → {n_saved:,} positions saved  [{elapsed:.0f}s]{eta_str}")

    # Summary
    print(f"\n{'═' * 62}")
    print(f"  Stage 2 complete")
    print(f"  Dates fetched   : {n_done}")
    print(f"  Total positions : {total_pos:,}")
    print(f"  DB table        : stage2_helicopter_tracks")
    print(f"  CSV             : {csv_path}")
    print(f"{'═' * 62}")
    print("\n  Next: run helicopter_maintenance_events.py to detect O&M events.\n")

    local_conn.close()
    sky_conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
