"""Shared utilities for the helicopter and vessel pipelines.

Houses code that is duplicated verbatim or near-verbatim across the two
pipelines: database connection, geometry helpers, turbine loaders, regional
constants, date iteration, the table-management helper that incorporates the
bug-class-5 fix (CREATE-IF-NOT-EXISTS + scoped DELETE instead of DROP-and-
recreate), and CSV / chunk-completion helpers.

Domain-specific logic (classification, scoring, transit/port detection,
Trino vs. local-DB query patterns) stays in the per-pipeline scripts.
"""
from __future__ import annotations

import csv
import math
import statistics
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator

import psycopg2


# Database

DB_CONFIG = dict(
    host="localhost", port=5432, dbname="windfarm",
    user="thesis", password="thesis2026",
)


def conn():
    """Return a fresh psycopg2 connection to the windfarm database."""
    return psycopg2.connect(**DB_CONFIG)


# Geometry

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres between two WGS-84 points."""
    R = 6_371_000
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# OSM regions

# Continental bounding boxes used by Stage 1 fetchers when --source=global.
# Format: (name, south, west, north, east). Identical across both pipelines.
REGIONS = [
    ("US East",              35.0,  -90.0,  55.0,  -60.0),
    ("US Midwest",           35.0, -105.0,  55.0,  -90.0),
    ("US West",              25.0, -125.0,  55.0, -105.0),
    ("Canada North",         55.0, -140.0,  72.0,  -60.0),
    ("Alaska",               55.0, -170.0,  72.0, -140.0),
    ("Mexico / Caribbean",   15.0, -120.0,  35.0,  -60.0),
    ("Europe NW",            50.0,  -15.0,  72.0,   15.0),
    ("Europe SW Iberia",     35.0,  -15.0,  50.0,    0.0),
    ("Europe SW France",     35.0,    0.0,  50.0,   15.0),
    ("Europe NE North",      57.0,   15.0,  72.0,   35.0),
    ("Europe NE South",      45.0,   15.0,  57.0,   35.0),
    ("Europe SE",            35.0,   15.0,  50.0,   45.0),
    ("North Africa/MidEast", 10.0,  -20.0,  40.0,   60.0),
    ("Sub-Saharan Africa",  -40.0,  -20.0,  10.0,   60.0),
    ("South America",       -60.0,  -85.0,  15.0,  -30.0),
    ("South Asia",            5.0,   60.0,  35.0,   95.0),
    ("East Asia N",          35.0,   95.0,  55.0,  130.0),
    ("East Asia S",          10.0,   95.0,  35.0,  130.0),
    ("Japan / Korea",        25.0,  130.0,  55.0,  148.0),
    ("Southeast Asia",      -15.0,   95.0,  25.0,  130.0),
    ("Oceania",             -50.0,  110.0,   0.0,  180.0),
    ("Far East / Pacific",   20.0,  145.0,  72.0,  180.0),
]


# Turbine loaders

def load_local_turbines(conn, project: str | None = None) -> list[tuple[float, float, str]]:
    """Load turbines from the local wind_turbines table.
    Returns a list of (latitude, longitude, project_name) tuples."""
    sql = ("SELECT latitude, longitude, project_name FROM wind_turbines"
           + (" WHERE project_name = %s" if project else "")
           + " ORDER BY project_name, latitude, longitude")
    with conn.cursor() as cur:
        cur.execute(sql, (project,) if project else ())
        return cur.fetchall()


def load_osm_turbines(
    conn,
    south: float, west: float, north: float, east: float,
    offshore_only: bool = True,
    ne_incremental: bool = False,
) -> list[tuple[float, float]]:
    """Load turbines from osm_wind_turbines within a bounding box."""
    if ne_incremental:
        extra = (" AND is_offshore_ne = TRUE "
                 "AND (is_offshore IS NULL OR is_offshore = FALSE)")
    elif offshore_only:
        extra = " AND is_offshore = TRUE"
    else:
        extra = ""
    sql = ("SELECT latitude, longitude FROM osm_wind_turbines "
           "WHERE latitude BETWEEN %s AND %s "
           "AND longitude BETWEEN %s AND %s" + extra)
    with conn.cursor() as cur:
        cur.execute(sql, (south, north, west, east))
        return cur.fetchall()


def grid_cluster(
    turbines: list[tuple[float, float]],
    resolution: float,
) -> list[tuple[float, float, float, float]]:
    """Cluster turbines into grid cells of `resolution` degrees.
    Returns sorted list of (lat_min, lat_max, lon_min, lon_max) tuples."""
    half = resolution / 2.0
    cells: set[tuple[float, float]] = set()
    for lat, lon in turbines:
        cells.add((round(lat / resolution) * resolution,
                   round(lon / resolution) * resolution))
    return sorted(
        (lat - half, lat + half, lon - half, lon + half)
        for lat, lon in cells
    )


def build_cluster_conditions(
    clusters: list[tuple],
    lat_col: str = "lat",
    lon_col: str = "lon",
) -> str:
    """One bounding-box condition per grid cell.
    Default column names (`lat`, `lon`) match the helicopter Trino schema; pass
    `lat_col='latitude', lon_col='longitude'` for vessel-side Postgres queries."""
    parts = [
        f"    ({lat_col} BETWEEN {lat_min:.5f} AND {lat_max:.5f}"
        f" AND {lon_col} BETWEEN {lon_min:.5f} AND {lon_max:.5f})"
        for lat_min, lat_max, lon_min, lon_max in clusters
    ]
    return "\n  OR\n".join(parts)


def build_outer_bbox(turbines: list[tuple], buffer: float = 0.05) -> dict:
    """Coarse bounding box covering all turbines plus a degree-buffer."""
    lats = [t[0] for t in turbines]
    lons = [t[1] for t in turbines]
    return {
        "lat_min": min(lats) - buffer, "lat_max": max(lats) + buffer,
        "lon_min": min(lons) - buffer, "lon_max": max(lons) + buffer,
    }


def get_farm_bbox(conn, project_name: str | None = None,
                  buffer_km: float = 50) -> dict:
    """Compute a lat/lon bounding box covering target turbines plus a buffer."""
    with conn.cursor() as cur:
        if project_name:
            cur.execute(
                "SELECT MIN(latitude), MAX(latitude), MIN(longitude), MAX(longitude) "
                "FROM wind_turbines WHERE project_name = %s", (project_name,)
            )
        else:
            cur.execute(
                "SELECT MIN(latitude), MAX(latitude), MIN(longitude), MAX(longitude) "
                "FROM wind_turbines"
            )
        lat_min, lat_max, lon_min, lon_max = cur.fetchone()
    buf_lat = buffer_km / 111.0
    buf_lon = buffer_km / (111.0 * math.cos(math.radians((lat_min + lat_max) / 2)))
    return {
        "bbox_lat_min": lat_min - buf_lat,
        "bbox_lat_max": lat_max + buf_lat,
        "bbox_lon_min": lon_min - buf_lon,
        "bbox_lon_max": lon_max + buf_lon,
    }


# Project listing

def list_projects(conn) -> None:
    """Print one line per wind-farm project with turbine counts."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT project_name, COUNT(*) FROM wind_turbines "
            "GROUP BY project_name ORDER BY project_name"
        )
        print("Available projects:")
        for name, n in cur.fetchall():
            print(f"  {name:<28} ({n} turbines)")


# Date / time iteration

def iter_months(start: date, end: date) -> Iterator[tuple[date, date]]:
    """Yield (month_start, month_end) date pairs covering [start, end)."""
    cur = start.replace(day=1)
    while cur < end:
        if cur.month == 12:
            nxt = cur.replace(year=cur.year + 1, month=1)
        else:
            nxt = cur.replace(month=cur.month + 1)
        yield cur, min(nxt, end)
        cur = nxt


def expand_year_shortcut(args, year_attr: str = "year",
                         start_attr: str = "start",
                         end_attr: str = "end") -> None:
    """If args.year is set, derive args.start / args.end as f"{year}-01-01" /
    f"{year+1}-01-01". Mutates `args` in place."""
    yr = getattr(args, year_attr, None)
    if yr:
        setattr(args, start_attr, f"{yr}-01-01")
        setattr(args, end_attr,   f"{yr + 1}-01-01")


# Table management

def ensure_table_and_clear(
    conn,
    table: str,
    create_ddl: str,
    date_col: str | None = None,
    d_start: date | None = None,
    d_end: date | None = None,
) -> int:
    """CREATE TABLE IF NOT EXISTS (preserves columns added by downstream
    scripts), then optionally DELETE rows in [d_start, d_end) so re-runs don't
    accumulate stale rows from prior parameter configurations.

    Replaces the older `DROP TABLE IF EXISTS … ; CREATE TABLE …` pattern,
    which destroys schema additions made by other scripts (e.g. Phase A
    helicopter imputation columns) on every re-run.

    Returns the number of rows deleted (0 if no scoped clear was requested).
    """
    with conn.cursor() as cur:
        cur.execute(create_ddl)
        if date_col and d_start and d_end:
            cur.execute(
                f"DELETE FROM {table} "
                f"WHERE {date_col} >= %s AND {date_col} < %s",
                (d_start, d_end),
            )
            deleted = cur.rowcount
        else:
            deleted = 0
    conn.commit()
    return deleted


# Chunk-completion bookkeeping

def load_completed_chunks(conn, table_name: str) -> set[str]:
    """Return the set of chunk_label values already recorded as complete."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT chunk_label FROM {table_name}")
        return {row[0] for row in cur.fetchall()}


def save_chunk_results(
    conn,
    table_name: str,
    chunk_label: str,
    year: int,
    month: int,
    start_d: date,
    end_d: date,
    n_hits: int,
    elapsed_s: float,
) -> None:
    """Record a completed Stage 1 chunk."""
    sql = (f"INSERT INTO {table_name} "
           "(chunk_label, year, month, start_date, end_date, n_hits, elapsed_s) "
           "VALUES (%s, %s, %s, %s, %s, %s, %s) "
           "ON CONFLICT (chunk_label) DO NOTHING")
    with conn.cursor() as cur:
        cur.execute(sql, (chunk_label, year, month, start_d, end_d,
                          n_hits, elapsed_s))
    conn.commit()


# CSV writing

def append_csv(path: Path, rows: list[tuple], chunk_label: str,
               header: list[str]) -> None:
    """Append `rows` to `path`. Writes header on first creation. Each row is
    suffixed with `chunk_label` for traceability."""
    new_file = not path.exists()
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(header + ["chunk_label"])
        for row in rows:
            w.writerow(list(row) + [chunk_label])


# Stage-3 shared scoring primitives
#
# These are the unified scoring primitives used by both stage3_helicopter_events
# and stage3_vessel_events. The architecture:
#
#   - 3-channel per-position evidence (helicopter: prox+alt+speed;
#     vessel: prox+sog) is computed in the per-pipeline classifier
#     because the channels and denominators differ
#   - The aggregation (peak_ev = median(top-K), s_dur = linear cap, score)
#     and the gap-inference primitive (virtual_gap_fix) are identical
#     across both pipelines and live here

def peak_ev_top_k_median(point_evs: list[float], k: int = 5) -> float:
    """Median of the top-K per-position evidences.

    Robust to single-fix outliers (one fix that happens to land close+low+slow
    by chance) — requires sustained co-location to score high. The dominant
    aggregation primitive in both classifiers.

    Returns 0.0 for an empty list. For lists shorter than K, returns the
    median of all values.
    """
    if not point_evs:
        return 0.0
    top_k = sorted(point_evs, reverse=True)[:k]
    return statistics.median(top_k)


def s_dur_linear(duration_min: float, denom: float = 60.0) -> float:
    """Linear visit-duration component.

    Returns 0 at 0 min, rises linearly, caps at 1.0 once `denom` minutes is
    reached. Used by both classifiers as the visit-level duration weight in
    `score = 100 * (W_EVIDENCE * peak_ev + W_DURATION * s_dur)`.
    """
    if duration_min <= 0:
        return 0.0
    return min(1.0, duration_min / denom)


def score_to_tier_bucket(score: float, high: float = 75.0, mid: float = 40.0) -> int:
    """Backward-compatible tier mapping derived from continuous score.

    `tier` semantics preserved for downstream consumers (Stage 4, EDA, HTML
    reports) that previously read `WHERE tier IN (1,2)` or `GROUP BY tier`:

        score >= 75   -> 1   (high confidence; was Tier 1 SURE)
        40 <= score   -> 2   (moderate; was Tier 2 LIKELY)
        score < 40    -> 0   (sub-threshold; written but excluded by Stage 4)

    Tier 3 is no longer emitted by the new pipeline — gap inference is
    folded into the score via `virtual_gap_fix`.
    """
    if score >= high:
        return 1
    if score >= mid:
        return 2
    return 0


def great_circle_closest_approach(
    lat1: float, lon1: float,
    lat2: float, lon2: float,
    tlat: float, tlon: float,
) -> tuple[float, float, float]:
    """Closest-approach point on the great-circle segment from (lat1, lon1)
    to (lat2, lon2) relative to a target point (tlat, tlon).

    Returns (closest_lat, closest_lon, distance_m) where distance_m is the
    haversine distance from the closest-approach point to the target. If
    the great-circle perpendicular foot falls outside the [A, B] arc, returns
    whichever endpoint is closer to the target.

    Used by `virtual_gap_fix` to compute the gap-derived virtual fix's
    position. Approximation: treats Earth as a sphere (haversine convention),
    same accuracy bound as the rest of the spatial pipeline (~0.3% on the
    U.S. East Coast).
    """
    def to_xyz(lat: float, lon: float) -> tuple[float, float, float]:
        rlat, rlon = math.radians(lat), math.radians(lon)
        return (math.cos(rlat) * math.cos(rlon),
                math.cos(rlat) * math.sin(rlon),
                math.sin(rlat))

    A = to_xyz(lat1, lon1)
    B = to_xyz(lat2, lon2)
    T = to_xyz(tlat, tlon)

    # Great-circle plane normal: n = A x B
    n = (A[1] * B[2] - A[2] * B[1],
         A[2] * B[0] - A[0] * B[2],
         A[0] * B[1] - A[1] * B[0])
    n_mag = math.sqrt(sum(c * c for c in n))
    if n_mag < 1e-10:
        # A and B coincide (or are antipodal — unlikely on short segments)
        return lat1, lon1, haversine_m(lat1, lon1, tlat, tlon)
    n_hat = tuple(c / n_mag for c in n)

    # Project T onto great-circle plane: T_proj = T - (T·n_hat) n_hat
    dot_T_n = sum(T[i] * n_hat[i] for i in range(3))
    T_proj = tuple(T[i] - dot_T_n * n_hat[i] for i in range(3))
    T_proj_mag = math.sqrt(sum(c * c for c in T_proj))
    if T_proj_mag < 1e-10:
        # Degenerate: T is at a pole of the great circle
        return lat1, lon1, haversine_m(lat1, lon1, tlat, tlon)
    closest_xyz = tuple(c / T_proj_mag for c in T_proj)

    closest_lat = math.degrees(math.asin(max(-1.0, min(1.0, closest_xyz[2]))))
    closest_lon = math.degrees(math.atan2(closest_xyz[1], closest_xyz[0]))

    # Determine whether closest_xyz lies on arc [A, B] or outside it.
    # The arc property: arc(A→closest) + arc(closest→B) ≈ arc(A→B). If the
    # perpendicular foot is outside the arc, the sum is strictly larger.
    arc_AB = haversine_m(lat1, lon1, lat2, lon2)
    arc_Ac = haversine_m(lat1, lon1, closest_lat, closest_lon)
    arc_cB = haversine_m(closest_lat, closest_lon, lat2, lon2)
    on_arc = abs(arc_Ac + arc_cB - arc_AB) < max(1.0, 0.001 * arc_AB)

    if on_arc:
        dist_m = haversine_m(closest_lat, closest_lon, tlat, tlon)
        return closest_lat, closest_lon, dist_m

    # Closest is outside the arc — return whichever endpoint is closer
    dA = haversine_m(lat1, lon1, tlat, tlon)
    dB = haversine_m(lat2, lon2, tlat, tlon)
    if dA <= dB:
        return lat1, lon1, dA
    return lat2, lon2, dB


def virtual_gap_fix(
    prev_pos: dict,
    next_pos: dict,
    turbine_lat: float,
    turbine_lon: float,
    *,
    max_gap_min: float = 60.0,
    min_gap_min: float = 5.0,
    min_span_m: float = 100.0,
) -> dict | None:
    """Synthesise a virtual fix at the great-circle closest-approach point of
    the segment [prev_pos, next_pos] to a given turbine, if the gap between
    fixes is navigationally meaningful.

    The virtual fix represents the inferred position of the vessel/aircraft
    during a coverage gap, used to fold gap-derived evidence into the
    continuous score (replaces the old separate Tier-3 INFERRED event class).

    Filters that produce `None` (gap not navigationally meaningful):
        - Gap shorter than `min_gap_min` minutes (just a normal observation cadence)
        - Bracketing fixes within `min_span_m` metres (stationary blackout, no motion)
        - Gap longer than `max_gap_min` minutes (confidence decayed to 0)

    Inputs `prev_pos` and `next_pos` are dicts that must contain `time_utc`
    (datetime), `lat`, `lon`, and optionally `sog_kt` (vessels), `velocity_ms`
    (helicopters), `baro_alt_m` (helicopters). Speed/altitude on the virtual
    fix is the mean of the two bracketing fixes.

    Returns a dict with the same shape as a real fix plus
    `is_virtual=True` and `confidence` (linear decay 1.0 → 0.0 over
    `max_gap_min`). Callers multiply per-position evidence by `confidence`
    before joining the per-position list.
    """
    gap_min = (next_pos["time_utc"] - prev_pos["time_utc"]).total_seconds() / 60.0
    if gap_min < min_gap_min:
        return None
    if gap_min >= max_gap_min:
        return None

    span_m = haversine_m(prev_pos["lat"], prev_pos["lon"],
                         next_pos["lat"], next_pos["lon"])
    if span_m < min_span_m:
        return None

    confidence = max(0.0, 1.0 - gap_min / max_gap_min)
    if confidence <= 0.0:
        return None

    closest_lat, closest_lon, closest_dist_m = great_circle_closest_approach(
        prev_pos["lat"], prev_pos["lon"],
        next_pos["lat"], next_pos["lon"],
        turbine_lat, turbine_lon,
    )

    vfix: dict = {
        "lat": closest_lat,
        "lon": closest_lon,
        "distance_m": closest_dist_m,
        "is_virtual": True,
        "confidence": confidence,
        "gap_min": gap_min,
        # Virtual fix's "time" is the mid-gap moment — useful for Stage 4
        # cross-modal joins that bucket by time
        "time_utc": prev_pos["time_utc"]
                    + (next_pos["time_utc"] - prev_pos["time_utc"]) / 2,
    }
    # Speed/altitude: mean of bracketing fixes, gracefully ignoring None
    def _mean_of(key: str) -> float | None:
        a = prev_pos.get(key)
        b = next_pos.get(key)
        if a is None and b is None:
            return None
        if a is None:
            return b
        if b is None:
            return a
        return (a + b) / 2.0

    for key in ("sog_kt", "velocity_ms", "baro_alt_m", "heading", "course_over_ground"):
        v = _mean_of(key)
        if v is not None:
            vfix[key] = v
    return vfix


def segment_point_distance_m(
    lat1: float, lon1: float,
    lat2: float, lon2: float,
    plat: float, plon: float,
) -> float:
    """Closest-approach distance from a great-circle segment to a point.

    Convenience wrapper over `great_circle_closest_approach` that returns
    only the distance in metres (drops the closest-point coordinates). Used
    by both classifiers wherever they need just the closest-distance number.
    """
    _lat, _lon, dist_m = great_circle_closest_approach(
        lat1, lon1, lat2, lon2, plat, plon)
    return dist_m
