#!/usr/bin/env python3
# Stage 1 vessel detection — monthly queries against local vessel_data_ais.
#
# Queries the local TimescaleDB vessel_data_ais hypertable for any vessel that
# passed within ~2 km of any wind turbine in the configured farms.  Returns
# DISTINCT (mms_id, vessel_date) pairs, which Stage 2 uses to fetch full day
# tracks.
#
# Runs monthly chunks sequentially.  Already-completed chunks are skipped
# automatically, making re-runs resumable.
#
# Output:
# - PostgreSQL table  : stage1_vessel_hits    (mms_id, vessel_date, …)
# - PostgreSQL table  : stage1_vessel_chunks  (completion log for resume)
# - CSV file          : stage1_vessel_hits_YEAR.csv
#
# Usage:
#   # Global mode (default — worldwide OSM turbines)
#   python stage1_vessel_tripwire.py
#   python stage1_vessel_tripwire.py --region "Europe NW"
#   python stage1_vessel_tripwire.py --resolution 0.05 --all-turbines
#   python stage1_vessel_tripwire.py --vessel-type 31 52 --highest-min-speed 2
#
#   # Local mode (US thesis farms)
#   python stage1_vessel_tripwire.py --source local
#   python stage1_vessel_tripwire.py --source local --project Vineyard_Wind --year 2025
#   python stage1_vessel_tripwire.py --source local --bbox-lat 0.027 --bbox-lon 0.036
#
#   # Dry run
#   python stage1_vessel_tripwire.py --dry-run
#   python stage1_vessel_tripwire.py --source local --dry-run
#
# See: stage2_vessel_fetch_tracks.py  -- Stage 2: full day track fetch.

import argparse
import calendar
import csv
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras

from pipeline_common import (
    DB_CONFIG, conn,
    REGIONS,
    load_local_turbines, load_osm_turbines,
    grid_cluster, build_cluster_conditions, build_outer_bbox,
)

# Query parameters

## Default per-turbine bbox half-width in latitude degrees (≈ 2 km).
DEFAULT_BBOX_LAT = 0.018
## Default per-turbine bbox half-width in longitude degrees (≈ 2 km at 41°N).
DEFAULT_BBOX_LON = 0.024

## Default grid resolution for global/OSM mode (0.1° ≈ 11 km).
DEFAULT_RESOLUTION = 0.1

## Default CSV output directory.
DEFAULT_OUT_DIR = Path("/mnt/e/data_lake/vessels")

DEFAULT_YEAR = 2024

# OSM regions

# `REGIONS` imported from pipeline_common


# DDL

_CREATE_HITS = """
CREATE TABLE IF NOT EXISTS stage1_vessel_hits (
    mms_id       TEXT             NOT NULL,
    vessel_date  DATE             NOT NULL,
    n_positions  INTEGER          NOT NULL,
    min_speed    DOUBLE PRECISION,
    max_speed    DOUBLE PRECISION,
    vessel_name  TEXT,
    vessel_type  SMALLINT,
    chunk_label  TEXT             NOT NULL,
    fetched_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (mms_id, vessel_date)
);
"""

_CREATE_HITS_IDX = """
CREATE INDEX IF NOT EXISTS idx_stage1_vessel_hits_date
    ON stage1_vessel_hits (vessel_date, mms_id);
"""

_CREATE_CHUNKS = """
CREATE TABLE IF NOT EXISTS stage1_vessel_chunks (
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
INSERT INTO stage1_vessel_hits
    (mms_id, vessel_date, n_positions, min_speed, max_speed,
     vessel_name, vessel_type, chunk_label)
VALUES %s
ON CONFLICT (mms_id, vessel_date) DO NOTHING;
"""

_INSERT_CHUNK = """
INSERT INTO stage1_vessel_chunks
    (chunk_label, year, month, start_date, end_date, n_hits, elapsed_s)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (chunk_label) DO NOTHING;
"""


# Turbine helpers
# `load_local_turbines`, `load_osm_turbines`, `grid_cluster`,
# `build_cluster_conditions`, `build_outer_bbox` imported from pipeline_common.
# `build_turbine_conditions` is vessel-specific (uses `latitude`/`longitude`
# column names + parameterised bbox sizes) so kept local.


def build_turbine_conditions(
    turbines: list[tuple],
    bbox_lat: float,
    bbox_lon: float,
) -> str:
    """Build a SQL OR block with one bounding-box condition per turbine.
    Vessel-specific: uses `latitude`/`longitude` columns and parameterised bbox."""
    parts = [
        f"    (latitude  BETWEEN {lat - bbox_lat:.6f} AND {lat + bbox_lat:.6f}"
        f" AND longitude BETWEEN {lon - bbox_lon:.6f} AND {lon + bbox_lon:.6f})"
        for lat, lon, _ in turbines
    ]
    return "\n  OR\n".join(parts)


# Query builder

def build_stage1_query(
    turbine_conditions: str,
    outer_bbox: dict | None,
    vessel_types: list[int] | None,
    min_positions: int,
    highest_min_speed: float | None,
    mmsi: str | None = None,
    limit: int | None = None,
) -> str:
    """
    Build the Stage 1 tripwire query for one time chunk.

    Returns one row per (mms_id, vessel_date) where the vessel came within
    ~2 km of at least one turbine, subject to optional pre-filters.

    turbine_conditions: Per-turbine OR block SQL.
    outer_bbox: Coarse bbox dict or None.
    vessel_types: Restrict to these AIS vessel_type codes, or None.
    min_positions: Minimum AIS pings near a turbine to qualify.
    highest_min_speed: Only keep hits where min SOG <= this value (kt).
    Returns: Parameterized SQL string (uses %(start)s / %(end)s).
    """
    outer = ""
    if outer_bbox:
        outer = (
            f"  AND latitude  BETWEEN {outer_bbox['lat_min']:.4f}"
            f" AND {outer_bbox['lat_max']:.4f}\n"
            f"  AND longitude BETWEEN {outer_bbox['lon_min']:.4f}"
            f" AND {outer_bbox['lon_max']:.4f}\n"
        )

    vtype_filter = ""
    if vessel_types:
        codes = ", ".join(str(c) for c in vessel_types)
        vtype_filter = f"  AND vessel_type IN ({codes})\n"

    mmsi_filter = "  AND mms_id = %(mmsi)s\n" if mmsi else ""

    having_clauses = [f"COUNT(*) >= {min_positions}"]
    if highest_min_speed is not None:
        having_clauses.append(f"MIN(speed_over_ground) <= {highest_min_speed}")
    having = "HAVING " + "\n    AND ".join(having_clauses)

    limit_clause = f"\nLIMIT %(limit)s" if limit else ""

    return f"""
SELECT
    mms_id,
    (time AT TIME ZONE 'UTC')::date          AS vessel_date,
    COUNT(*)                                  AS n_positions,
    MIN(speed_over_ground)                    AS min_speed,
    MAX(speed_over_ground)                    AS max_speed,
    MIN(vessel_name)                          AS vessel_name,
    MIN(vessel_type)                          AS vessel_type
FROM vessel_data_ais
WHERE time >= %(start)s
  AND time <  %(end)s
{outer}{vtype_filter}{mmsi_filter}  AND (
{turbine_conditions}
  )
GROUP BY mms_id, (time AT TIME ZONE 'UTC')::date
{having}{limit_clause}
"""


# Chunk generation

def monthly_chunks(year: int) -> list[tuple[str, int, date, date]]:
    """
    Generate twelve monthly (label, month, start_date, end_date) tuples.

    year: Calendar year to cover.
    Returns: List of 12 tuples in chronological order.
    """
    result = []
    for m in range(1, 13):
        start = date(year, m, 1)
        last  = calendar.monthrange(year, m)[1]
        end   = date(year, m, last) + timedelta(days=1)   # exclusive
        label = f"{year}-M{m:02d}"
        result.append((label, m, start, end))
    return result


# Database helpers

def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_CREATE_HITS)
        cur.execute(_CREATE_HITS_IDX)
        cur.execute(_CREATE_CHUNKS)
    conn.commit()


def load_completed_chunks(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT chunk_label FROM stage1_vessel_chunks")
        return {row[0] for row in cur.fetchall()}


def save_chunk_results(
    conn,
    chunk_label: str,
    year: int,
    month: int,
    start_d: date,
    end_d: date,
    rows: list[tuple],
    elapsed_s: float,
) -> int:
    with conn.cursor() as cur:
        if rows:
            tagged = [(*r, chunk_label) for r in rows]
            psycopg2.extras.execute_values(cur, _UPSERT_HITS, tagged)
        cur.execute(
            _INSERT_CHUNK,
            (chunk_label, year, month, start_d, end_d, len(rows), elapsed_s),
        )
    conn.commit()
    return len(rows)


# CSV helpers

_CSV_HEADER = ["mms_id", "vessel_date", "n_positions", "min_speed",
               "max_speed", "vessel_name", "vessel_type", "chunk_label"]


def append_csv(path: Path, rows: list[tuple], chunk_label: str) -> None:
    write_header = not path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(_CSV_HEADER)
        for r in rows:
            w.writerow([*r, chunk_label])


# Entry point

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stage 1 vessel tripwire — monthly queries against vessel_data_ais.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Global mode (default — worldwide OSM turbines)
  %(prog)s
  %(prog)s --region "Europe NW"
  %(prog)s --resolution 0.05 --all-turbines
  %(prog)s --vessel-type 31 52 --highest-min-speed 2

  # Local mode (US thesis farms)
  %(prog)s --source local
  %(prog)s --source local --project Vineyard_Wind --year 2025
  %(prog)s --source local --bbox-lat 0.027 --bbox-lon 0.036

  # Dry run
  %(prog)s --dry-run
  %(prog)s --source local --dry-run
        """,
    )
    parser.add_argument("--source", choices=["local", "global"], default="global",
                        help="Turbine source: 'local' (thesis farms) or 'global' (worldwide OSM, default)")
    parser.add_argument("--year", metavar="YYYY", type=int, default=DEFAULT_YEAR,
                        help=f"Year to process (default: {DEFAULT_YEAR})")
    parser.add_argument("--vessel-type", metavar="CODE", type=int, nargs="+",
                        dest="vessel_types",
                        help="Filter by AIS vessel_type code(s). "
                             "E.g. 31 (tug), 52 (tug), 79 (cargo). Default: all types.")
    parser.add_argument("--min-positions", metavar="N", type=int, default=5,
                        help="Minimum AIS pings within turbine bbox to qualify "
                             "as a hit (default: 5). Filters bbox-edge artefacts "
                             "where a vessel clips the boundary with only 1–3 pings.")
    parser.add_argument("--highest-min-speed", metavar="KT", type=float, default=5,
                        help="Only keep hits where the vessel's minimum recorded "
                             "speed near a turbine was <= KT knots. Default: 5 kt "
                             "(keeps ~73%% of hits; cuts transit vessels above 5 kt).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print query plan without executing")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, metavar="DIR",
                        help=f"CSV output directory (default: {DEFAULT_OUT_DIR})")

    test_group = parser.add_argument_group("single-candidate test mode")
    test_group.add_argument("--mmsi", metavar="ID",
                            help="Filter to a single vessel by MMSI")
    test_group.add_argument("--single-date", metavar="YYYY-MM-DD",
                            help="Process only the month containing this date")
    test_group.add_argument("--limit", metavar="N", type=int,
                            help="Limit query results to N rows")

    local_group = parser.add_argument_group("local mode options")
    local_group.add_argument("--project", metavar="NAME",
                             help="Restrict to one farm project (default: all)")
    local_group.add_argument("--bbox-lat", metavar="DEG", type=float,
                             default=DEFAULT_BBOX_LAT,
                             help=f"Per-turbine bbox half-width in latitude degrees "
                                  f"(default: {DEFAULT_BBOX_LAT}, ≈ 2 km)")
    local_group.add_argument("--bbox-lon", metavar="DEG", type=float,
                             default=DEFAULT_BBOX_LON,
                             help=f"Per-turbine bbox half-width in longitude degrees "
                                  f"(default: {DEFAULT_BBOX_LON}, ≈ 2 km at 41°N)")
    local_group.add_argument("--no-outer-bbox", action="store_true",
                             help="Disable coarse outer-bbox pre-filter")

    global_group = parser.add_argument_group("global mode options")
    global_group.add_argument("--resolution", metavar="DEG", type=float,
                              default=DEFAULT_RESOLUTION,
                              help=f"Grid cell size in degrees (default: {DEFAULT_RESOLUTION})")
    global_group.add_argument("--region", metavar="NAME",
                              help="Restrict to one region name, e.g. 'Europe NW' (default: all)")
    global_group.add_argument("--all-turbines", action="store_true",
                              help="Include onshore turbines (default: offshore only)")
    global_group.add_argument("--ne-incremental", action="store_true",
                              help="Only process turbines newly classified offshore via Natural Earth "
                                   "(is_offshore_ne=TRUE and is_offshore!=TRUE). "
                                   "Use after running fix_osm_offshore_flag.py.")

    args = parser.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)
    ensure_tables(conn)

    # Turbine / condition setup
    if args.source == "local":
        turbines = load_local_turbines(conn, args.project)
        if not turbines:
            print(f"Error: no turbines found"
                  + (f" for project '{args.project}'" if args.project else ""),
                  file=sys.stderr)
            conn.close()
            return 1
        projects = sorted({t[2] for t in turbines})
        print(f"Source           : local ({len(turbines)} turbines across "
              f"{len(projects)} farm(s): {', '.join(projects)})")
        print(f"Bbox per turbine : ±{args.bbox_lat:.4f}° lat  ±{args.bbox_lon:.4f}° lon")
        turbine_conditions = build_turbine_conditions(turbines, args.bbox_lat, args.bbox_lon)
        outer_bbox = None if args.no_outer_bbox else build_outer_bbox(turbines)
        region_runs = [(None, turbine_conditions, outer_bbox)]   # single run
        csv_suffix = f"{args.year}"

    else:  # global
        region_filter  = args.region
        ne_incremental = args.ne_incremental
        active_regions = [r for r in REGIONS if region_filter is None
                          or r[0] == region_filter]
        if not active_regions:
            print(f"Error: region '{region_filter}' not found. Available regions:",
                  file=sys.stderr)
            for r in REGIONS:
                print(f"  {r[0]}", file=sys.stderr)
            conn.close()
            return 1
        offshore_only = not args.all_turbines
        source_desc = ("newly classified offshore (is_offshore_ne)" if ne_incremental
                       else "offshore only" if offshore_only else "all turbines")
        print(f"Source           : global OSM  ({len(active_regions)} region(s), "
              f"resolution={args.resolution}°, {source_desc})")
        region_runs = []
        for name, south, west, north, east in active_regions:
            osm = load_osm_turbines(conn, south, west, north, east,
                                    offshore_only, ne_incremental)
            if not osm:
                continue
            clusters = grid_cluster(osm, args.resolution)
            conditions = build_cluster_conditions(
                clusters, lat_col="latitude", lon_col="longitude")
            bbox = None if args.no_outer_bbox else {
                "lat_min": south, "lat_max": north,
                "lon_min": west,  "lon_max": east,
            }
            region_runs.append((name, conditions, bbox))
            print(f"  {name:<24}  {len(osm):>5} turbines → {len(clusters)} clusters")
        if not region_runs:
            print("Error: no OSM turbines found for the selected region(s).",
                  file=sys.stderr)
            conn.close()
            return 1
        csv_suffix = f"global_ne_{args.year}" if ne_incremental else f"global_{args.year}"

    if args.vessel_types:
        print(f"Vessel types     : {args.vessel_types}")
    print(f"Min positions    : {args.min_positions}")
    if args.highest_min_speed is not None:
        print(f"Highest min speed: <= {args.highest_min_speed} kt")

    if args.mmsi:
        print(f"MMSI filter      : {args.mmsi}")
    if args.single_date:
        print(f"Single date      : {args.single_date}")
    if args.limit:
        print(f"Result limit     : {args.limit}")

    # Chunk planning
    all_chunks = monthly_chunks(args.year)

    # --single-date: keep only the chunk containing that date
    if args.single_date:
        sd = date.fromisoformat(args.single_date)
        all_chunks = [
            (label, month, start, end)
            for label, month, start, end in all_chunks
            if start <= sd < end
        ]

    completed  = load_completed_chunks(conn)

    # In global mode each chunk label is prefixed with the region slug
    def slug(name: str | None) -> str:
        return name.lower().replace(" ", "_").replace("/", "_") if name else ""

    ne_prefix = "ne_" if (args.source == "global" and args.ne_incremental) else ""
    pending_runs = []   # (region_name, conditions, outer_bbox, chunk_tuples)
    for region_name, conditions, outer_bbox in region_runs:
        prefix = f"{ne_prefix}{slug(region_name)}_" if region_name else ne_prefix
        run_chunks = [
            (f"{prefix}{label}", month, start, end)
            for label, month, start, end in all_chunks
            if f"{prefix}{label}" not in completed
        ]
        if run_chunks:
            pending_runs.append((region_name, conditions, outer_bbox, run_chunks))

    total_pending = sum(len(r[3]) for r in pending_runs)
    print(f"\nYear             : {args.year}")
    print(f"Chunks total     : {len(region_runs) * len(all_chunks)}  "
          f"|  completed: {len(completed)}  |  pending: {total_pending}")

    if total_pending == 0:
        print("\nAll chunks already completed. Nothing to do.")
        conn.close()
        return 0

    if args.dry_run:
        print("\nDRY RUN — chunks that would run:")
        for region_name, _, _, run_chunks in pending_runs:
            if region_name:
                print(f"\n  [{region_name}]")
            for label, _, start, end in run_chunks:
                print(f"    {label}  {start} → {end}")
        conn.close()
        return 0

    # Execution
    csv_path   = args.out_dir / f"stage1_vessel_hits_{csv_suffix}.csv"
    total_hits = 0
    chunk_idx  = 0
    run_start  = time.monotonic()

    for region_name, conditions, outer_bbox in [(r[0], r[1], r[2]) for r in pending_runs]:
        run_chunks = next(r[3] for r in pending_runs if r[0] == region_name)
        sql = build_stage1_query(
            conditions, outer_bbox,
            args.vessel_types, args.min_positions, args.highest_min_speed,
            mmsi=args.mmsi, limit=args.limit,
        )
        if region_name:
            print(f"\n-- Region: {region_name} --")

        for label, month, start_d, end_d in run_chunks:
            chunk_idx += 1
            eta_str = ""
            if chunk_idx > 1:
                elapsed_run = time.monotonic() - run_start
                avg_s = elapsed_run / (chunk_idx - 1)
                remaining_s = avg_s * (total_pending - chunk_idx + 1)
                eta_str = f"  ETA ≈ {remaining_s / 60:.0f} min"

            print(f"\n[{chunk_idx}/{total_pending}]  {label}  "
                  f"({start_d} → {end_d}){eta_str}", flush=True)

            start_dt = datetime(start_d.year, start_d.month, start_d.day,
                                tzinfo=timezone.utc)
            end_dt   = datetime(end_d.year, end_d.month, end_d.day,
                                tzinfo=timezone.utc)

            t0 = time.monotonic()
            params = {"start": start_dt, "end": end_dt}
            if args.mmsi:
                params["mmsi"] = args.mmsi
            if args.limit:
                params["limit"] = args.limit
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            elapsed = time.monotonic() - t0

            n = save_chunk_results(conn, label, args.year, month,
                                   start_d, end_d, rows, elapsed)
            append_csv(csv_path, rows, label)
            total_hits += n
            print(f"  → {n:,} (mms_id, date) hits  [{elapsed:.0f}s]")

    print(f"\n{'═' * 62}")
    print(f"  Stage 1 complete")
    print(f"  Total hits  : {total_hits:,}")
    print(f"  DB table    : stage1_vessel_hits")
    print(f"  CSV         : {csv_path}")
    print(f"{'═' * 62}")
    print("\n  Next: run stage2_vessel_fetch_tracks.py\n")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
