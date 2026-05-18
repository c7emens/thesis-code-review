#!/usr/bin/env python3
# Stage 1 helicopter detection — monthly OpenSky Trino tripwire queries.
#
# Queries the OpenSky Trino database for every aircraft that passed within
# the configured bounding boxes of known wind turbines. Returns DISTINCT
# (icao24, flight_date) pairs for Stage 2 to fetch full day tracks.
#
# Two turbine sources are supported via --source:
#
#   local (default) — uses the local wind_turbines table (3 US thesis farms).
#                     One per-turbine bounding box condition (BBOX ≈ 2 km).
#                     Controlled by --project to restrict to one farm.
#
#   global          — uses the osm_wind_turbines table (worldwide OSM data).
#                     Turbines are grid-clustered at --resolution degrees to
#                     keep the SQL OR block manageable for large regions.
#                     Iterates over 22 continental regions; use --region to
#                     restrict to one. Chunk labels include the region slug
#                     so they coexist with local-mode results in the DB.
#
# Runs 12 monthly chunks two-at-a-time (≈ 9 min/chunk) to stay under the
# OpenSky Trino 30-minute hard query limit. Already-completed chunks are
# skipped automatically, making re-runs fully resumable.
#
# Output:
# - PostgreSQL table : stage1_helicopter_hits   (icao24, flight_date, …)
# - PostgreSQL table : stage1_helicopter_chunks (completion log for resume)
# - CSV file         : stage1_helicopter_hits[_global]_YEAR.csv
#
# Usage:
#   # Global mode (default — worldwide OSM turbines)
#   python stage1_helicopter_tripwire.py --user you@example.com
#   python stage1_helicopter_tripwire.py --user you@example.com --region "Europe NW"
#   python stage1_helicopter_tripwire.py --user you@example.com --resolution 0.05 --all-turbines
#
#   # Local mode (US thesis farms)
#   python stage1_helicopter_tripwire.py --user you@example.com --source local
#   python stage1_helicopter_tripwire.py --user you@example.com --source local --project Vineyard_Wind
#
#   # Dry run (either mode)
#   python stage1_helicopter_tripwire.py --user you@example.com --dry-run
#   python stage1_helicopter_tripwire.py --user you@example.com --source local --dry-run
#
# See: calibrate_helicopter_trino_stage1.py -- measures per-week query cost first
# See: stage2_helicopter_fetch_tracks.py    -- Stage 2: full day track fetch

import argparse
import calendar
import csv
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import trino
import trino.auth
from dotenv import load_dotenv; load_dotenv()
from opensky_auth import make_oauth2_auth

from pipeline_common import (
    DB_CONFIG, conn,
    REGIONS,
    load_local_turbines, load_osm_turbines,
    grid_cluster, build_cluster_conditions, build_outer_bbox,
)

# OpenSky Trino

OPENSKY_HOST    = "trino.opensky-network.org"
OPENSKY_PORT    = 443
OPENSKY_CATALOG = "minio"
OPENSKY_SCHEMA  = "osky"

# Query parameters

## Altitude ceiling for low-altitude filter (metres; 609 m ≈ 2,000 ft).
MAX_ALT_M = 609

## Per-turbine bbox half-widths for local mode (≈ 2 km).
BBOX_LAT = 0.018
BBOX_LON = 0.024

## Default grid resolution for OSM mode (0.1° ≈ 11 km at mid-latitudes).
DEFAULT_RESOLUTION = 0.1

DEFAULT_YEAR    = 2024
DEFAULT_OUT_DIR = Path("/mnt/e/data_lake/helicopters")

# OSM regions

# `REGIONS` imported from pipeline_common


# DDL

_CREATE_HITS = """
CREATE TABLE IF NOT EXISTS stage1_helicopter_hits (
    icao24      TEXT             NOT NULL,
    flight_date DATE             NOT NULL,
    n_positions INTEGER          NOT NULL,
    min_alt_m   DOUBLE PRECISION,
    max_alt_m   DOUBLE PRECISION,
    chunk_label TEXT             NOT NULL,
    fetched_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (icao24, flight_date)
);
"""

_CREATE_HITS_IDX = """
CREATE INDEX IF NOT EXISTS idx_stage1_helicopter_hits_date
    ON stage1_helicopter_hits (flight_date, icao24);
"""

_CREATE_CHUNKS = """
CREATE TABLE IF NOT EXISTS stage1_helicopter_chunks (
    chunk_label  TEXT             PRIMARY KEY,
    year         SMALLINT         NOT NULL,
    month        SMALLINT         NOT NULL,
    start_date   DATE             NOT NULL,
    end_date     DATE             NOT NULL,
    n_hits       INTEGER          NOT NULL,
    elapsed_s    DOUBLE PRECISION NOT NULL,
    completed_at TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);
"""

_UPSERT_HITS = """
INSERT INTO stage1_helicopter_hits
    (icao24, flight_date, n_positions, min_alt_m, max_alt_m, chunk_label)
VALUES %s
ON CONFLICT (icao24, flight_date) DO NOTHING;
"""

_INSERT_CHUNK = """
INSERT INTO stage1_helicopter_chunks
    (chunk_label, year, month, start_date, end_date, n_hits, elapsed_s)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (chunk_label) DO NOTHING;
"""


# Turbine loading
# `load_local_turbines`, `load_osm_turbines`, `grid_cluster`,
# `build_cluster_conditions`, `build_outer_bbox` imported from pipeline_common.
# `build_turbine_conditions` is helicopter-specific (uses `lat`/`lon` columns
# with hard-coded BBOX_LAT/BBOX_LON) so kept local.


def build_turbine_conditions(turbines: list[tuple]) -> str:
    """One per-turbine bbox condition (local mode, ≈ 2 km half-width).
    Helicopter-specific: uses `lat`/`lon` (Trino column names)."""
    parts = [
        f"    (lat BETWEEN {lat - BBOX_LAT:.6f} AND {lat + BBOX_LAT:.6f}"
        f" AND lon BETWEEN {lon - BBOX_LON:.6f} AND {lon + BBOX_LON:.6f})"
        for lat, lon, *_ in turbines
    ]
    return "\n  OR\n".join(parts)


# Query builder

def build_stage1_query(
    h_start: int,
    h_end: int,
    spatial_conditions: str,
    outer_bbox: dict | None,
    icao24: str | None = None,
    limit: int | None = None,
) -> str:
    """Build the Stage 1 Trino query for one time chunk."""
    outer = ""
    if outer_bbox:
        outer = (
            f"  AND lat BETWEEN {outer_bbox['lat_min']:.4f} AND {outer_bbox['lat_max']:.4f}\n"
            f"  AND lon BETWEEN {outer_bbox['lon_min']:.4f} AND {outer_bbox['lon_max']:.4f}\n"
        )
    icao_filter = f"  AND icao24 = '{icao24}'\n" if icao24 else ""
    limit_clause = f"\nLIMIT {limit}" if limit else ""
    return (
        f"SELECT\n"
        f"    icao24,\n"
        f"    CAST(from_unixtime(time) AS DATE)  AS flight_date,\n"
        f"    COUNT(*)                            AS n_positions,\n"
        f"    ROUND(MIN(baroaltitude))            AS min_alt_m,\n"
        f"    ROUND(MAX(baroaltitude))            AS max_alt_m\n"
        f"FROM {OPENSKY_CATALOG}.{OPENSKY_SCHEMA}.state_vectors_data4\n"
        f"WHERE hour  >= {h_start}\n"
        f"  AND hour  <  {h_end}\n"
        f"  AND baroaltitude < {MAX_ALT_M}\n"
        f"  AND onground = false\n"
        f"  AND lat IS NOT NULL\n"
        f"  AND lon IS NOT NULL\n"
        f"{outer}{icao_filter}"
        f"  AND (\n{spatial_conditions}\n  )\n"
        f"GROUP BY icao24, CAST(from_unixtime(time) AS DATE)\n"
        f"ORDER BY flight_date, icao24{limit_clause}"
    )


# Chunk generation

def monthly_chunks(year: int, label_prefix: str = "") -> list[tuple]:
    """Generate 12 monthly (label, h_start, h_end, month, start_date, end_date) tuples."""
    result = []
    for m in range(1, 13):
        start = date(year, m, 1)
        end   = date(year, m, calendar.monthrange(year, m)[1]) + timedelta(days=1)
        label = f"{label_prefix}{year}-M{m:02d}"
        h_start = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
        h_end   = int(datetime(end.year,   end.month,   end.day,   tzinfo=timezone.utc).timestamp())
        result.append((label, h_start, h_end, m, start, end))
    return result


# Progress heartbeat

_DONE = object()


def _format_progress_line(label: str, slot: list, elapsed: float) -> str:
    prefix = f"  [{label}] [{elapsed:>5.1f}s]"
    cur = slot[0]
    if cur is None:
        return f"{prefix} waiting to start..."
    if cur is _DONE:
        return f"{prefix} ✓ done"
    stats = cur.stats
    if stats is None:
        msg = "waiting for auth — open browser URL" if elapsed < 30 else "submitted, waiting..."
    else:
        total_s = stats.get("totalSplits", 0)
        done_s  = stats.get("completedSplits", 0)
        rows    = stats.get("processedRows", 0)
        gb      = stats.get("processedBytes", 0) / 1e9
        if total_s > 0:
            pct = 100 * done_s / total_s
            bar = ("█" * int(pct / 5)).ljust(20, "░")
            msg = (f"{bar} {pct:5.1f}%  {done_s}/{total_s} splits  "
                   f"{rows / 1e6:.1f}M rows  {gb:.2f} GB")
        else:
            msg = stats.get("state", "?")
    return f"{prefix} {msg}"


def start_batch_heartbeat(
    cursor_slots: dict[str, list],
    t0: float,
) -> tuple[threading.Event, threading.Thread]:
    labels = list(cursor_slots.keys())
    n      = len(labels)
    stop   = threading.Event()
    _skip  = [False]

    for label in labels:
        sys.stdout.write(_format_progress_line(label, cursor_slots[label], 0.0)[:90].ljust(90) + "\n")
    sys.stdout.flush()

    def _run():
        while not stop.is_set():
            elapsed  = time.monotonic() - t0
            any_auth = any(
                s[0] is not None and s[0] is not _DONE and s[0].stats is None
                for s in cursor_slots.values()
            )
            if any_auth:
                _skip[0] = True
                stop.wait(2)
                continue
            if not _skip[0]:
                sys.stdout.write(f"\033[{n}A")
            for label in labels:
                sys.stdout.write(f"\r{_format_progress_line(label, cursor_slots[label], elapsed)[:90]:<90}\n")
            sys.stdout.flush()
            _skip[0] = False
            stop.wait(5)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return stop, t


# Chunk runner

def run_chunk(
    chunk_label: str,
    h_start: int,
    h_end: int,
    sky_conn,
    spatial_conditions: str,
    outer_bbox: dict | None,
    cursor_slot: list,
    active_cursors: dict,
    icao24: str | None = None,
    limit: int | None = None,
) -> tuple[str, list[tuple], float]:
    sql = build_stage1_query(h_start, h_end, spatial_conditions, outer_bbox,
                             icao24=icao24, limit=limit)
    cur = sky_conn.cursor()
    cursor_slot[0] = cur
    active_cursors[chunk_label] = cur
    t0 = time.monotonic()

    for attempt in range(1, 6):
        try:
            cur.execute(sql)
            break
        except Exception as exc:
            if "QUERY_QUEUE_FULL" in str(exc) and attempt < 5:
                print(f"\n  [{chunk_label}] ⚠  Queue full — retrying in 60s (attempt {attempt}/5)...",
                      file=sys.stderr, flush=True)
                time.sleep(60)
                cur = sky_conn.cursor()
                cursor_slot[0] = cur
                active_cursors[chunk_label] = cur
            else:
                cursor_slot[0] = _DONE
                active_cursors.pop(chunk_label, None)
                raise

    rows = []
    while True:
        batch = cur.fetchmany(10_000)
        if not batch:
            break
        rows.extend(batch)

    elapsed = time.monotonic() - t0
    cur.close()
    cursor_slot[0] = _DONE
    active_cursors.pop(chunk_label, None)
    return chunk_label, rows, elapsed


# Database helpers

def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_CREATE_HITS)
        cur.execute(_CREATE_HITS_IDX)
        cur.execute(_CREATE_CHUNKS)
    conn.commit()


def load_completed_chunks(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT chunk_label FROM stage1_helicopter_chunks")
        return {row[0] for row in cur.fetchall()}


def save_chunk_results(conn, chunk_label, year, month, start_d, end_d, rows, elapsed_s) -> int:
    with conn.cursor() as cur:
        if rows:
            psycopg2.extras.execute_values(cur, _UPSERT_HITS, [(*r, chunk_label) for r in rows])
        cur.execute(_INSERT_CHUNK, (chunk_label, year, month, start_d, end_d, len(rows), elapsed_s))
    conn.commit()
    return len(rows)


# CSV helpers

_CSV_HEADER = ["icao24", "flight_date", "n_positions", "min_alt_m", "max_alt_m", "chunk_label"]


def append_csv(path: Path, rows: list[tuple], chunk_label: str) -> None:
    write_header = not path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(_CSV_HEADER)
        for r in rows:
            w.writerow([*r, chunk_label])


# Batch runner (shared by both modes)

def run_batch_loop(
    pending: list[tuple],
    spatial_conditions: str,
    outer_bbox: dict | None,
    conns: list,
    local_conn,
    csv_path: Path,
    year: int,
    icao24: str | None = None,
    limit: int | None = None,
) -> int:
    """
    Run pending chunks two-at-a-time. Returns total hits saved.
    Each tuple in pending: (label, h_start, h_end, month, start_d, end_d).
    """
    total_hits = 0
    for batch_start in range(0, len(pending), 2):
        batch   = pending[batch_start: batch_start + 2]
        n_batch = batch_start // 2 + 1
        n_total = (len(pending) + 1) // 2
        print(f"{'-' * 62}")
        print(f"  Batch {n_batch}/{n_total}  —  " + "  +  ".join(c[0] for c in batch))
        print(f"{'-' * 62}")

        cursor_slots = {label: [None] for label, *_ in batch}
        batch_t0     = time.monotonic()
        hb_stop, hb  = start_batch_heartbeat(cursor_slots, batch_t0)

        futures = {}
        with ThreadPoolExecutor(max_workers=2) as executor:
            for i, (label, h_start, h_end, month, start_d, end_d) in enumerate(batch):
                f = executor.submit(
                    run_chunk,
                    label, h_start, h_end, conns[i],
                    spatial_conditions, outer_bbox,
                    cursor_slots[label], active_cursors,
                    icao24=icao24, limit=limit,
                )
                futures[f] = (label, month, start_d, end_d)

        hb_stop.set(); hb.join()
        print()

        for f in futures:
            label, month, start_d, end_d = futures[f]
            chunk_label, rows, elapsed = f.result()
            n = save_chunk_results(local_conn, chunk_label, year, month, start_d, end_d, rows, elapsed)
            append_csv(csv_path, rows, chunk_label)
            total_hits += n
            print(f"  {chunk_label}: {n:,} hits  [{elapsed:.0f}s]")

    return total_hits


# Mode implementations

def run_local_mode(args, local_conn, conns) -> int:
    """Farm-scoped mode using wind_turbines table with per-turbine bbox."""
    turbines = load_local_turbines(local_conn, args.project)
    if not turbines:
        msg = f"for project '{args.project}'" if args.project else ""
        print(f"Error: no turbines found {msg}", file=sys.stderr)
        return 1

    projects = sorted({t[2] for t in turbines})
    print(f"Source           : local wind_turbines")
    print(f"Turbines         : {len(turbines)} across {len(projects)} farm(s): {', '.join(projects)}")
    print(f"Bbox half-width  : lat ±{BBOX_LAT}°  lon ±{BBOX_LON}°  (≈ 2 km)")

    spatial_conditions = build_turbine_conditions(turbines)
    outer_bbox = None if args.no_outer_bbox else build_outer_bbox(turbines)
    if outer_bbox and not args.no_outer_bbox:
        print(f"Outer bbox       : lat [{outer_bbox['lat_min']:.3f}, {outer_bbox['lat_max']:.3f}]  "
              f"lon [{outer_bbox['lon_min']:.3f}, {outer_bbox['lon_max']:.3f}]")

    if args.icao24:
        print(f"ICAO24 filter    : {args.icao24}")
    if args.single_date:
        print(f"Single date      : {args.single_date}")
    if args.limit:
        print(f"Result limit     : {args.limit}")

    completed = load_completed_chunks(local_conn)
    all_chunks = monthly_chunks(args.year)

    # --single-date: keep only the chunk containing that date
    if args.single_date:
        sd = date.fromisoformat(args.single_date)
        all_chunks = [c for c in all_chunks if c[4] <= sd < c[5]]

    pending    = [c for c in all_chunks if c[0] not in completed]
    csv_path   = args.out_dir / f"stage1_helicopter_hits_{args.year}.csv"

    print(f"\nYear             : {args.year}")
    print(f"Chunks pending   : {len(pending)} / {len(all_chunks)}")
    if args.dry_run:
        for label, *_ in pending:
            print(f"  would run: {label}")
        return 0
    if not pending:
        print("All chunks already completed.")
        return 0

    total = run_batch_loop(pending, spatial_conditions, outer_bbox, conns, local_conn, csv_path, args.year,
                           icao24=args.icao24, limit=args.limit)

    print(f"\n{'═' * 62}")
    print(f"  Stage 1 (local) complete")
    print(f"  Total hits : {total:,}  |  CSV: {csv_path}")
    print(f"{'═' * 62}")
    return 0


def run_osm_mode(args, local_conn, conns) -> int:
    """Global mode using osm_wind_turbines with grid clustering per region."""
    offshore_only  = not args.all_turbines
    ne_incremental = args.ne_incremental
    region_filter  = args.region
    label_prefix   = "ne_" if ne_incremental else ""

    completed = load_completed_chunks(local_conn)
    all_months = monthly_chunks(args.year)

    # --single-date: keep only the chunk containing that date
    if args.single_date:
        sd = date.fromisoformat(args.single_date)
        all_months = [c for c in all_months if c[4] <= sd < c[5]]

    csv_suffix = f"global_ne_{args.year}" if ne_incremental else f"global_{args.year}"
    csv_path   = args.out_dir / f"stage1_helicopter_hits_{csv_suffix}.csv"

    source_desc = ("newly classified offshore (is_offshore_ne)" if ne_incremental
                   else "offshore only" if offshore_only else "all")
    print(f"Source           : osm_wind_turbines ({source_desc})")
    print(f"Grid resolution  : {args.resolution}°")
    if region_filter:
        print(f"Region filter    : {region_filter}")

    # Build region plan
    region_plan = []
    for region_name, south, west, north, east in REGIONS:
        if region_filter and region_name != region_filter:
            continue
        turbines = load_osm_turbines(local_conn, south, west, north, east,
                                     offshore_only, ne_incremental)
        if not turbines:
            continue
        clusters    = grid_cluster(turbines, args.resolution)
        region_slug = region_name.replace(" ", "_").replace("/", "_")
        outer_bbox  = {"lat_min": south, "lat_max": north, "lon_min": west, "lon_max": east}
        pending = [
            (f"{label_prefix}{args.year}-{region_slug}-M{m:02d}", h_start, h_end, m, start_d, end_d)
            for _, h_start, h_end, m, start_d, end_d in all_months
            if f"{label_prefix}{args.year}-{region_slug}-M{m:02d}" not in completed
        ]
        if pending:
            region_plan.append((region_name, outer_bbox, clusters, turbines, pending))

    print(f"\nRegions with turbines : {len(region_plan)}")
    for region_name, _, clusters, turbines, pending in region_plan:
        print(f"  {region_name:<25} {len(turbines):>5} turbines → "
              f"{len(clusters):>4} cells  {len(pending):>2} months pending")

    if args.dry_run or not region_plan:
        return 0

    total_hits = 0
    for r_idx, (region_name, outer_bbox, clusters, turbines, pending) in enumerate(region_plan, 1):
        spatial_conditions = build_cluster_conditions(clusters)
        print(f"\n{'━' * 62}")
        print(f"  Region {r_idx}/{len(region_plan)}: {region_name}  "
              f"({len(turbines)} turbines, {len(clusters)} cells, {len(pending)} months)")
        print(f"{'━' * 62}")
        total_hits += run_batch_loop(
            pending, spatial_conditions, outer_bbox, conns, local_conn, csv_path, args.year,
            icao24=args.icao24, limit=args.limit,
        )

    print(f"\n{'═' * 62}")
    print(f"  Stage 1 (OSM global) complete")
    print(f"  Total hits : {total_hits:,}  |  CSV: {csv_path}")
    print(f"{'═' * 62}")
    return 0


# CLI

def parse_args():
    parser = argparse.ArgumentParser(
        description="Stage 1 helicopter tripwire — monthly OpenSky Trino queries.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Global mode (default — worldwide OSM turbines)
  %(prog)s --user you@example.com
  %(prog)s --user you@example.com --region "Europe NW"
  %(prog)s --user you@example.com --resolution 0.05 --all-turbines

  # Local mode (US thesis farms)
  %(prog)s --user you@example.com --source local
  %(prog)s --user you@example.com --source local --project Vineyard_Wind --year 2025

  # Dry run (no Trino queries executed)
  %(prog)s --user you@example.com --dry-run
  %(prog)s --user you@example.com --source local --dry-run
        """,
    )

    parser.add_argument("--user", metavar="EMAIL",
                        default=os.environ.get("OPENSKY_USERNAME"),
                        help="OpenSky username. Defaults to $OPENSKY_USERNAME "
                             "(from /mnt/d/thesis/.env). Password auth also reads "
                             "$OPENSKY_PASS or $OPENSKY_PASSWORD from .env.")
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR, metavar="YYYY",
                        help=f"Year to process (default: {DEFAULT_YEAR})")
    parser.add_argument("--source", choices=["local", "global"], default="global",
                        help="Turbine source: 'local' (thesis farms) or 'global' (worldwide OSM, default)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print query plan without executing Trino queries")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, metavar="DIR",
                        help=f"CSV output directory (default: {DEFAULT_OUT_DIR})")

    # Local-mode options
    local_group = parser.add_argument_group("local mode options")
    local_group.add_argument("--project", metavar="NAME",
                             help="Restrict to one farm project (default: all)")
    local_group.add_argument("--no-outer-bbox", action="store_true",
                             help="Disable coarse outer-bbox pre-filter")

    # OSM-mode options
    osm_group = parser.add_argument_group("global mode options")
    osm_group.add_argument("--resolution", type=float, default=DEFAULT_RESOLUTION, metavar="DEG",
                           help=f"Grid cell size in degrees (default: {DEFAULT_RESOLUTION})")
    osm_group.add_argument("--region", metavar="NAME",
                           help="Restrict to one region name, e.g. 'Europe NW' (default: all)")
    osm_group.add_argument("--all-turbines", action="store_true",
                           help="Include onshore turbines (default: offshore only)")
    osm_group.add_argument("--ne-incremental", action="store_true",
                           help="Only process turbines newly classified offshore via Natural Earth "
                                "(is_offshore_ne=TRUE and is_offshore!=TRUE). "
                                "Use after running fix_osm_offshore_flag.py.")

    test_group = parser.add_argument_group("single-candidate test mode")
    test_group.add_argument("--icao24", metavar="HEX",
                            help="Filter to a single aircraft by ICAO24 hex code")
    test_group.add_argument("--single-date", metavar="YYYY-MM-DD",
                            help="Process only the month containing this date")
    test_group.add_argument("--limit", metavar="N", type=int,
                            help="Limit query results to N rows")

    return parser.parse_args()


# Entry point

active_cursors: dict = {}   # module-level so SIGINT handler can reach it


def main() -> int:
    args = parse_args()

    if not args.user and not args.dry_run:
        print("Error: OpenSky username required — set OPENSKY_USERNAME in .env or pass --user",
              file=sys.stderr)
        return 1

    local_conn = psycopg2.connect(**DB_CONFIG)
    ensure_tables(local_conn)

    if args.dry_run:
        conns = [None, None]
    else:
        print("Connecting to OpenSky Trino ...")
        auth  = make_oauth2_auth()

        def _make_conn():
            return trino.dbapi.connect(
                host=OPENSKY_HOST, port=OPENSKY_PORT, http_scheme="https",
                user=args.user, auth=auth,
                catalog=OPENSKY_CATALOG, schema=OPENSKY_SCHEMA,
                request_timeout=1800,
            )

        conn1 = _make_conn()
        print("Authenticating ...")
        _c = conn1.cursor(); _c.execute("SELECT 1"); _c.fetchone(); _c.close()
        print("✓ Authenticated.\n")
        conn2  = _make_conn()
        conns  = [conn1, conn2]

        def _sigint(sig, frame):
            print("\n  Cancelling active queries ...", flush=True)
            for lbl, cur in list(active_cursors.items()):
                try:
                    cur.cancel(); print(f"  Cancelled {lbl}", flush=True)
                except Exception:
                    pass
            sys.exit(1)

        signal.signal(signal.SIGINT, _sigint)

    rc = run_local_mode(args, local_conn, conns) if args.source == "local" \
        else run_osm_mode(args, local_conn, conns)

    local_conn.close()
    if not args.dry_run:
        conns[0].close()
        conns[1].close()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
