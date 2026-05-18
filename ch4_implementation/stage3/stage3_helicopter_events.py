#!/usr/bin/env python3
# Stage 3 — detect helicopter maintenance-candidate events near offshore wind turbines.
#
# Strategy:
#   1. Query stage2_helicopter_tracks for helicopter positions within a horizontal
#      radius and altitude ceiling of each wind turbine (PostGIS spatial filter).
#      All records in stage2_helicopter_tracks are helicopters — no type filter needed.
#   2. Group raw position hits by (icao24, turbine_code) into contiguous "visits"
#      — a new visit starts whenever the time gap between consecutive positions
#      exceeds VISIT_GAP_MINUTES.
#   3. Apply classify_visit() to each visit to decide whether it looks like
#      maintenance-related activity rather than a transit or overflight.
#   4. Write qualifying events to CSV and insert into stage3_helicopter_events DB table.
#
# Data source
# stage2_helicopter_tracks columns:
#   icao24, flight_date, time_unix, time_utc, lat, lon,
#   baro_alt_m, velocity_ms, heading, onground
#
# Usage:
#   python stage3_helicopter_events.py --list-projects
#   python stage3_helicopter_events.py --project Block_Island --output events.csv
#   python stage3_helicopter_events.py --project Vineyard_Wind --year 2024
#   python stage3_helicopter_events.py --radius 500 --max-alt 400 --gap 20
#
# See: stage2_helicopter_fetch_tracks.py  -- Stage 2: produces stage2_helicopter_tracks.

import argparse
import csv
import math
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from itertools import groupby
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

# Shared utilities (DB_CONFIG, conn, haversine_m, geometry helpers, table mgmt)
from pipeline_common import (
    DB_CONFIG, conn,
    haversine_m,
    get_farm_bbox, iter_months, list_projects,
    ensure_table_and_clear,
    peak_ev_top_k_median, s_dur_linear, virtual_gap_fix,
    score_to_tier_bucket,
)


DEFAULT_OUTPUT = Path("/mnt/d/thesis/presentation/helicopter_events.csv")

# Spatial / altitude search parameters

## Horizontal search radius around each turbine (metres).
SEARCH_RADIUS_M = 1000

## Maximum barometric altitude (metres AMSL) to be considered "near" a turbine.
# Typical hub heights: Block Island ~79 m, Vineyard Wind / Revolution Wind ~140 m.
# 600 m gives enough ceiling to catch approach and departure phases too.
MAX_ALT_M = 600

# Visit segmentation

## Gap in minutes between consecutive positions that triggers a new visit.
# 90 min accounts for sparse offshore ADS-B coverage (no ground receivers
# within range → gaps of 30-60+ min between fixes). Safe because grouping
# key is (icao24, turbine_code), so only same-helicopter-same-turbine merges.
VISIT_GAP_MINUTES = 90

# Output columns

OUTPUT_FIELDS = [
    "icao24",
    "project_name", "turbine_code", "turbine_name",
    "visit_start", "visit_end", "duration_minutes",
    "n_positions",
    "min_distance_m", "min_alt_m",
    "min_speed_ms", "max_speed_ms", "median_speed_ms",
    "score", "s_evidence", "s_duration", "n_virtual_fixes",
    "departure_airport", "return_airport",
    "transit_out_min", "transit_back_min", "airport_distance_km",
]


# DDL

_CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS stage3_helicopter_events (
    icao24           TEXT             NOT NULL,
    project_name     TEXT             NOT NULL,
    turbine_code     TEXT             NOT NULL,
    turbine_name     TEXT,
    visit_start      TIMESTAMPTZ      NOT NULL,
    visit_end        TIMESTAMPTZ      NOT NULL,
    duration_minutes REAL             NOT NULL,
    n_positions      INTEGER          NOT NULL,
    min_distance_m   REAL,
    min_alt_m        REAL,
    min_speed_ms     REAL,
    max_speed_ms     REAL,
    median_speed_ms  REAL,
    score            REAL,
    s_evidence       REAL,
    s_duration       REAL,
    n_virtual_fixes  INTEGER          DEFAULT 0,
    departure_airport    TEXT,
    return_airport       TEXT,
    transit_out_min      REAL,
    transit_back_min     REAL,
    airport_distance_km  REAL,
    PRIMARY KEY (icao24, turbine_code, visit_start)
);
"""

_UPSERT_EVENT = """
INSERT INTO stage3_helicopter_events
    (icao24, project_name, turbine_code, turbine_name,
     visit_start, visit_end, duration_minutes, n_positions,
     min_distance_m, min_alt_m, min_speed_ms, max_speed_ms, median_speed_ms,
     score, s_evidence, s_duration, n_virtual_fixes,
     departure_airport, return_airport,
     transit_out_min, transit_back_min, airport_distance_km)
VALUES
    (%(icao24)s, %(project_name)s, %(turbine_code)s, %(turbine_name)s,
     %(visit_start)s, %(visit_end)s, %(duration_minutes)s, %(n_positions)s,
     %(min_distance_m)s, %(min_alt_m)s, %(min_speed_ms)s, %(max_speed_ms)s, %(median_speed_ms)s,
     %(score)s, %(s_evidence)s, %(s_duration)s, %(n_virtual_fixes)s,
     %(departure_airport)s, %(return_airport)s,
     %(transit_out_min)s, %(transit_back_min)s, %(airport_distance_km)s)
ON CONFLICT (icao24, turbine_code, visit_start) DO NOTHING;
"""


# Visit classifier (scoring-based, mirrors vessel pipeline)

# Scoring denominators — distance/alt/speed at which each component reaches zero.
# Tightening proximity/altitude (500/300) was tested but killed 142 real visits
# at recall cost; reverted to 1000/600 which match approach phase reality.
PROX_DENOM_M   = 1000.0
ALT_DENOM_M    = 600.0
SPEED_DENOM_MS = 25.0     # tightened from 30; data-driven (q25=14.6 m/s in low-alt visit positions)

# Per-point evidence weights (3-way co-location: close + low + slow)
W_PROX       = 0.45
W_ALT        = 0.30
W_SPEED      = 0.25

# Final score aggregation — evidence dominates; duration is a tie-breaker.
W_EVIDENCE   = 0.80
W_DURATION   = 0.20
DUR_DENOM    = 60.0

# Top-K aggregation: median of top-K per-position evidence values.
# Median-top-5 requires sustained co-location (was: mean of top-3).
TOPK_FOR_PEAK = 5

# Classification threshold (calibrated by ROC sweep against Master Flight Report)
SCORE_THRESHOLD = 40       # provisional; calibrate_threshold.py will derive empirically

# Hard pre-filters
MIN_POSITIONS = 3
MIN_DURATION  = 5          # minutes
# MIN_APPROACH removed: hard cliff at 750 m created discontinuity.
# Scoring's PROX_DENOM_M=500 already gives s_prox=0 beyond 500 m.

# Default speed score for missing data — empirical q25 of low-alt visit speed
# (14.6 m/s with SPEED_DENOM_MS=25 → s_speed=0.42).
# Missing rate is only 0.1% in practice so this rarely fires.
MISSING_SPEED_DEFAULT = 0.42


def _score_helicopter_point(distance_m: float, baro_alt_m: float | None,
                             velocity_ms: float | None) -> float | None:
    """Per-position evidence for the helicopter classifier (3-channel:
    prox + alt + speed). Returns None if required fields are missing.

    Shared between real fixes and virtual gap-fixes — the only difference
    at the call site is whether the result is multiplied by the virtual
    fix's `confidence` weight before joining `point_ev`.
    """
    if distance_m is None or baro_alt_m is None:
        return None
    s_prox = min(1.0, max(0.0, 1.0 - float(distance_m) / PROX_DENOM_M))
    s_alt  = min(1.0, max(0.0, 1.0 - float(baro_alt_m) / ALT_DENOM_M))
    if velocity_ms is not None:
        s_spd = min(1.0, max(0.0, 1.0 - float(velocity_ms) / SPEED_DENOM_MS))
    else:
        s_spd = MISSING_SPEED_DEFAULT
    return W_PROX * s_prox + W_ALT * s_alt + W_SPEED * s_spd


def classify_visit(positions: list[dict],
                   turbine_lat: float | None = None,
                   turbine_lon: float | None = None) -> tuple[bool, float, dict]:
    """
    Score a visit using 3-way per-position co-location evidence + optional
    virtual gap-fix inference (folded into the same continuous score).

    Per real position p:
      s_prox  = max(0, 1 - distance_m / PROX_DENOM_M)   # 1000 m
      s_alt   = max(0, 1 - baro_alt_m / ALT_DENOM_M)    # 600 m
      s_speed = max(0, 1 - velocity_ms / SPEED_DENOM_MS) # 25 m/s
      ev_p    = W_PROX·s_prox + W_ALT·s_alt + W_SPEED·s_speed

    For each gap between consecutive positions (gap > MIN_GAP_FOR_VFIX_MIN),
    a virtual fix is generated at the great-circle closest-approach point
    to (turbine_lat, turbine_lon) (if provided). Its evidence is scored
    identically and multiplied by `confidence = 1 - gap_min/MAX_GAP_MIN`
    before joining `point_ev`. This unified mechanism replaces the
    helicopter-vs-vessel split — both pipelines now do the same thing.

    Aggregation:
      peak_evidence = median(top-K) of point_ev (real + virtual)
      s_duration    = min(1, duration_min / DUR_DENOM)
      score         = 100 · (W_EVIDENCE·peak_evidence + W_DURATION·s_duration)

    Returns (qualifies, score, sub_scores_dict).
    """
    if len(positions) < MIN_POSITIONS:
        return False, 0.0, {}

    first, last = positions[0], positions[-1]
    duration_min = (last["time_utc"] - first["time_utc"]).total_seconds() / 60
    if duration_min < MIN_DURATION:
        return False, 0.0, {}

    point_ev: list[float] = []

    # Real fixes
    for p in positions:
        ev = _score_helicopter_point(
            p["distance_m"], p["baro_alt_m"], p.get("velocity_ms"))
        if ev is not None:
            point_ev.append(ev)

    # Virtual gap-fixes: one per consecutive-pair gap, weighted by confidence
    n_virtual = 0
    if turbine_lat is not None and turbine_lon is not None:
        for prev_pos, next_pos in zip(positions[:-1], positions[1:]):
            vfix = virtual_gap_fix(prev_pos, next_pos, turbine_lat, turbine_lon)
            if vfix is None:
                continue
            ev = _score_helicopter_point(
                vfix["distance_m"], vfix.get("baro_alt_m"), vfix.get("velocity_ms"))
            if ev is None:
                continue
            point_ev.append(ev * vfix["confidence"])
            n_virtual += 1

    if not point_ev:
        return False, 0.0, {}

    peak_evidence = peak_ev_top_k_median(point_ev, k=TOPK_FOR_PEAK)
    s_dur = s_dur_linear(duration_min, denom=DUR_DENOM)

    raw_score = (W_EVIDENCE * peak_evidence + W_DURATION * s_dur) * 100
    score = round(min(100.0, max(0.0, raw_score)), 1)

    # Always return qualifies=True so the caller emits the visit with its
    # score regardless of band; the SCORE_THRESHOLD is applied at downstream
    # query time (`WHERE score >= 40`) so it stays tunable post-hoc. The
    # qualifies flag is retained for backward-compatible signature.
    return (True,
            score,
            {
                "s_evidence":      round(peak_evidence, 4),
                "s_duration":      round(s_dur, 4),
                "n_virtual_fixes": n_virtual,
            })


# Database query

## Fetch helicopter positions near turbines for one date window.
# Filters by flight_date (the indexed Trino fetch-batch tag) with a ±1d buffer,
# then bounds precisely by time_utc. The buffer is needed because Stage 2 stores
# positions under the requested fetch date, not DATE(time_utc): a Day-N position
# can live under flight_date = N±1 (see stage2_helicopter_fetch_tracks.py:165).
# Without the buffer, midnight-crossing visits would be split across monthly
# chunks and dropped below MIN_DURATION.
PROXIMITY_QUERY = """
    SELECT
        h.icao24,
        h.time_utc,
        h.lat,
        h.lon,
        h.baro_alt_m,
        h.velocity_ms,
        h.heading,
        t.project_name,
        t.turbine_code,
        t.turbine_name,
        ROUND(
            ST_Distance(
                ST_SetSRID(ST_MakePoint(h.lon, h.lat), 4326)::geography,
                ST_SetSRID(ST_MakePoint(t.longitude, t.latitude), 4326)::geography
            )::numeric, 1
        ) AS distance_m
    FROM stage2_helicopter_tracks h
    JOIN wind_turbines t
        ON ST_DWithin(
            ST_SetSRID(ST_MakePoint(h.lon, h.lat), 4326)::geography,
            ST_SetSRID(ST_MakePoint(t.longitude, t.latitude), 4326)::geography,
            %(radius)s
        )
    WHERE h.flight_date >= %(fd_start)s
      AND h.flight_date <  %(fd_end)s
      AND h.time_utc    >= %(d_start)s
      AND h.time_utc    <  %(d_end)s
      AND h.baro_alt_m  IS NOT NULL
      AND h.baro_alt_m  <= %(max_alt)s
      AND h.lat  BETWEEN %(bbox_lat_min)s  AND %(bbox_lat_max)s
      AND h.lon  BETWEEN %(bbox_lon_min)s  AND %(bbox_lon_max)s
      {project_filter}
      {icao24_filter}
    ORDER BY h.icao24, t.turbine_code, h.time_utc
"""


# Helpers
# `ensure_table_and_clear`, `list_projects`, `get_farm_bbox`, `iter_months`
# are imported from pipeline_common above.


def fetch_proximity_hits(conn, d_start: date, d_end: date,
                         radius_m: int, max_alt_m: int,
                         project_name=None, bbox: dict | None = None,
                         icao24: str | None = None) -> list[dict]:
    """
    Fetch all helicopter positions near turbines for a given date window.
    """
    # Buffer the partition filter ±1d to catch positions filed under neighbour
    # fetch-batches; bound precisely by time_utc to avoid bleeding into the
    # adjacent month's results.
    d_start_ts = datetime.combine(d_start, datetime.min.time(), tzinfo=timezone.utc)
    d_end_ts   = datetime.combine(d_end,   datetime.min.time(), tzinfo=timezone.utc)
    params = {
        "radius":   radius_m,
        "max_alt":  max_alt_m,
        "fd_start": d_start - timedelta(days=1),
        "fd_end":   d_end   + timedelta(days=1),
        "d_start":  d_start_ts,
        "d_end":    d_end_ts,
    }
    params.update(bbox or {})

    project_filter = ""
    if project_name:
        project_filter = "AND t.project_name = %(project_name)s"
        params["project_name"] = project_name

    icao24_filter = ""
    if icao24:
        icao24_filter = "AND h.icao24 = %(icao24)s"
        params["icao24"] = icao24

    query = PROXIMITY_QUERY.format(project_filter=project_filter,
                                   icao24_filter=icao24_filter)
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query, params)
        return [dict(r) for r in cur.fetchall()]


# Visit aggregation

def aggregate_visits(hits: list[dict], visit_gap_minutes: int) -> list[dict]:
    """
    Group position hits into visits and compute visit-level statistics.

    A visit is a contiguous run of positions for the same (icao24, turbine_code)
    where consecutive fixes are no more than visit_gap_minutes apart.
    """
    if not hits:
        return []

    visits = []
    gap    = timedelta(minutes=visit_gap_minutes)
    key_fn = lambda h: (h["icao24"], h["turbine_code"])

    for (icao24, turbine_code), group in groupby(hits, key=key_fn):
        positions     = list(group)
        visit_pos     = [positions[0]]

        for pos in positions[1:]:
            if pos["time_utc"] - visit_pos[-1]["time_utc"] > gap:
                visits.append(_summarise_visit(icao24, turbine_code, visit_pos))
                visit_pos = [pos]
            else:
                visit_pos.append(pos)

        visits.append(_summarise_visit(icao24, turbine_code, visit_pos))

    return visits


def _summarise_visit(icao24: str, turbine_code: str, positions: list[dict]) -> dict:
    """
    Build a visit summary dict from a list of position records.
    """
    first, last = positions[0], positions[-1]
    duration_min = (last["time_utc"] - first["time_utc"]).total_seconds() / 60

    speeds        = [p["velocity_ms"] for p in positions if p["velocity_ms"] is not None]
    speeds_sorted = sorted(speeds) if speeds else []
    if speeds_sorted:
        n = len(speeds_sorted)
        median_spd = (speeds_sorted[n // 2] + speeds_sorted[(n - 1) // 2]) / 2
    else:
        median_spd = None

    return {
        "icao24":          icao24,
        "project_name":    first["project_name"],
        "turbine_code":    turbine_code,
        "turbine_name":    first["turbine_name"],
        "visit_start":     first["time_utc"],
        "visit_end":       last["time_utc"],
        "duration_minutes": round(duration_min, 1),
        "n_positions":     len(positions),
        "min_distance_m":  min((p["distance_m"] for p in positions if p["distance_m"] is not None), default=None),
        "min_alt_m":       min((p["baro_alt_m"] for p in positions if p["baro_alt_m"] is not None), default=None),
        "min_speed_ms":    min(speeds) if speeds else None,
        "max_speed_ms":    max(speeds) if speeds else None,
        "median_speed_ms": median_spd,
        "_positions":      positions,   # kept for classify_visit; stripped before output
        "score":           None,       # populated by classify_visit in main loop
        "s_evidence":      None,       # sub-score: peak 3-way co-location evidence
        "s_duration":      None,       # sub-score: duration component
        "n_virtual_fixes": 0,          # virtual gap-fixes contributing to score
        # Transit fields — populated later by detect_helicopter_transit()
        "departure_airport":   None,
        "return_airport":      None,
        "transit_out_min":     None,
        "transit_back_min":    None,
        "airport_distance_km": None,
    }


# Deduplication: merge overlapping events per aircraft

CLOSE_APPROACH_M = 100  # events with min_distance below this are "confirmed visits"


def deduplicate_events(events: list[dict]) -> list[dict]:
    """
    Remove spatial bleed from overlapping events while preserving multi-turbine
    visits within a single sortie.

    Strategy: cluster events by time overlap, then within each cluster:
    - Keep ALL events with min_distance_m <= 100 m (confirmed close approach)
    - From the remaining (far-away) events, keep only the best-scoring one
      IF no close-approach event exists in the cluster

    This correctly handles:
    - Single turbine visit bleeding into neighbors → keeps only the close one
    - Multi-turbine sortie (heli visits A then B) → keeps both (both < 200 m)
    """
    if not events:
        return []

    from collections import defaultdict
    by_aircraft = defaultdict(list)
    for ev in events:
        by_aircraft[ev["icao24"]].append(ev)

    result = []
    for icao24, ac_events in by_aircraft.items():
        ac_events.sort(key=lambda e: e["visit_start"])

        # Cluster: events overlap if their time windows intersect
        clusters = []
        current_cluster = [ac_events[0]]
        cluster_end = ac_events[0]["visit_end"]

        for ev in ac_events[1:]:
            if ev["visit_start"] <= cluster_end:
                current_cluster.append(ev)
                cluster_end = max(cluster_end, ev["visit_end"])
            else:
                clusters.append(current_cluster)
                current_cluster = [ev]
                cluster_end = ev["visit_end"]
        clusters.append(current_cluster)

        # From each cluster: keep close-approach events, discard spatial bleed
        for cluster in clusters:
            close = [e for e in cluster
                     if e["min_distance_m"] is not None
                     and e["min_distance_m"] <= CLOSE_APPROACH_M]
            if close:
                result.extend(close)
            else:
                # No confirmed close approach — keep best scoring as candidate
                best = max(cluster, key=lambda e: e["score"])
                result.append(best)

    return result


# Helicopter transit time extraction
# `haversine_m` imported from pipeline_common


# OurAirports dataset (loaded once)

_AIRPORTS_CSV = Path("/mnt/e/data_lake/ourairports_airports.csv")
_AIRPORTS_FALLBACK = Path(__file__).resolve().parents[2] / "data" / "sample" / "ourairports_airports.csv"
_AIRPORT_TYPES = {"heliport", "small_airport", "medium_airport", "large_airport"}

GROUND_ALT_M = 100      # baro altitude ceiling for airport positions (generous
                         # — baro can read -50 to +30 m on ground due to calibration)
GROUND_RADIUS_M = 2000   # max distance to an airport to count as landed

_airports_df: pd.DataFrame | None = None


def _load_airports() -> pd.DataFrame:
    """Load OurAirports CSV (cached after first call)."""
    global _airports_df
    if _airports_df is not None:
        return _airports_df

    # _AIRPORTS_CSV.exists() raises OSError if its drive is offline (e.g. WSL
    # mount of an unplugged external), so guard with try/except and fall back.
    try:
        primary_ok = _AIRPORTS_CSV.exists()
    except OSError:
        primary_ok = False
    path = _AIRPORTS_CSV if primary_ok else _AIRPORTS_FALLBACK
    df = pd.read_csv(path, low_memory=False)
    df = df[df["type"].isin(_AIRPORT_TYPES)].copy()
    df = df.dropna(subset=["latitude_deg", "longitude_deg"])
    # Pre-filter to NE US bounding box (speeds up distance calc)
    df = df[
        (df["latitude_deg"].between(38, 44)) &
        (df["longitude_deg"].between(-75, -69))
    ].reset_index(drop=True)
    _airports_df = df
    return _airports_df


def _nearest_airport_ourairports(lat: float, lon: float,
                                  radius_m: float = GROUND_RADIUS_M) -> str | None:
    """Return name of closest OurAirports entry within radius_m, or None."""
    airports = _load_airports()
    cos_lat = math.cos(math.radians(lat))
    dlat = (airports["latitude_deg"].values - lat) * 111_320
    dlon = (airports["longitude_deg"].values - lon) * 111_320 * cos_lat
    dists = dlat ** 2 + dlon ** 2  # squared metres (avoid sqrt for speed)
    radius_sq = radius_m ** 2
    mask = dists <= radius_sq
    if not mask.any():
        return None
    idx = dists[mask].argmin()
    row = airports.loc[airports.index[mask][idx]]
    name = row.get("name")
    if pd.isna(name) or not name:
        name = row.get("ident", "Unknown")
    return str(name)


def _get_turbine_coords(conn, turbine_code: str):
    """Return (latitude, longitude) for a turbine, or (None, None)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT latitude, longitude FROM wind_turbines
            WHERE turbine_code = %s LIMIT 1
        """, (turbine_code,))
        row = cur.fetchone()
    return row if row else (None, None)


def _is_near_ground(alt, og) -> bool:
    """Check if a position is plausibly at low altitude.

    Generous threshold: baro altitudes on the ground range from ~-60m to +30m
    due to calibration. 100m ceiling catches all ground + initial climb-out.
    Speed is NOT used here — ADS-B speed values are often stale on the ground.
    """
    if og:
        return True
    return alt is not None and float(alt) < GROUND_ALT_M


def detect_helicopter_transit(conn, event: dict) -> dict:
    """Extract transit time from full flight track for one helicopter event.

    Scans stage2_helicopter_tracks for the flight containing this visit.
    Finds positions near known airports (within 2000m, alt < 100m) to
    identify departure and arrival airports.

    Strategy: airport proximity is the primary signal — if a helicopter
    position is within 2000m of a known airport/heliport, it's on the
    ground there. Speed is not used because ADS-B speed values are
    frequently stale at low altitude (OpenSky Trino data artifact).

    Returns dict with keys: departure_airport, return_airport,
    transit_out_min, transit_back_min, airport_distance_km.
    """
    icao24 = event["icao24"]
    visit_start = event["visit_start"]
    visit_end = event["visit_end"]
    flight_date = visit_start.date() if hasattr(visit_start, 'date') else visit_start

    # stage2_helicopter_tracks.flight_date is the Trino fetch-batch tag, not
    # the actual flight day — the ±1d buffer in stage2_helicopter_fetch_tracks.py
    # files Day-N positions under flight_date=N-1 when no Stage 1 hit occurred
    # on Day N. Buffer the partition filter ±1d, then bound by time_utc precisely.
    #
    # Cross-midnight visits: extend the window beyond the nominal day so a visit
    # ending after 00:00 the next day still has its return-airport positions in
    # scope. MAX_TRANSIT_MIN (= 180) caps how far past visit_end we need to look.
    day_start = datetime.combine(flight_date, datetime.min.time(), tzinfo=timezone.utc)
    day_end   = day_start + timedelta(days=1)
    margin    = timedelta(minutes=210)   # MAX_TRANSIT_MIN + 30 min safety
    query_start = min(day_start, visit_start - margin)
    query_end   = max(day_end,   visit_end   + margin)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT time_utc, lat, lon, baro_alt_m, velocity_ms, onground
            FROM stage2_helicopter_tracks
            WHERE icao24 = %s
              AND flight_date BETWEEN %s AND %s
              AND time_utc >= %s AND time_utc < %s
            ORDER BY time_utc
        """, (icao24,
              query_start.date() - timedelta(days=1),
              query_end.date()   + timedelta(days=1),
              query_start, query_end))
        track = cur.fetchall()

    if not track:
        return {}

    # Collect low-altitude positions before and after the visit
    low_before = []   # candidates for departure airport
    low_after = []    # candidates for arrival airport
    for ts, lat, lon, alt, vel, og in track:
        if lat is None or lon is None:
            continue
        if _is_near_ground(alt, og):
            if ts < visit_start:
                low_before.append((ts, lat, lon))
            elif ts > visit_end:
                low_after.append((ts, lat, lon))

    # Find departure: check low positions before visit, latest first
    # Stop at first one near an airport (searching backward from visit)
    takeoff_time = None
    takeoff_lat, takeoff_lon = None, None
    takeoff_airport = None
    for ts, lat, lon in reversed(low_before):
        apt = _nearest_airport_ourairports(lat, lon, GROUND_RADIUS_M)
        if apt is not None:
            takeoff_time, takeoff_lat, takeoff_lon = ts, lat, lon
            takeoff_airport = apt
            break

    # Find arrival: check low positions after visit, earliest first
    landing_time = None
    landing_lat, landing_lon = None, None
    landing_airport = None
    for ts, lat, lon in low_after:
        apt = _nearest_airport_ourairports(lat, lon, GROUND_RADIUS_M)
        if apt is not None:
            landing_time, landing_lat, landing_lon = ts, lat, lon
            landing_airport = apt
            break

    result = {}
    MAX_TRANSIT_MIN = 180  # 3 hours — no realistic heli transit exceeds this

    if takeoff_time and takeoff_lat is not None:
        t_min = round((visit_start - takeoff_time).total_seconds() / 60, 1)
        if 0 < t_min <= MAX_TRANSIT_MIN:
            result["transit_out_min"] = t_min
            result["departure_airport"] = takeoff_airport

            # Airport-to-turbine distance
            t_lat, t_lon = _get_turbine_coords(conn, event["turbine_code"])
            if t_lat is not None:
                result["airport_distance_km"] = round(
                    haversine_m(takeoff_lat, takeoff_lon, t_lat, t_lon) / 1_000, 1)

    if landing_time and landing_lat is not None:
        t_min = round((landing_time - visit_end).total_seconds() / 60, 1)
        if 0 < t_min <= MAX_TRANSIT_MIN:
            result["transit_back_min"] = t_min
            result["return_airport"] = landing_airport

    return result


def save_events(conn, events: list[dict]) -> int:
    """Insert events into stage3_helicopter_events, skipping duplicates."""
    if not events:
        return 0
    with conn.cursor() as cur:
        for e in events:
            cur.execute(_UPSERT_EVENT, {k: e[k] for k in OUTPUT_FIELDS})
    conn.commit()
    return len(events)


# Entry point

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stage 3 — detect helicopter maintenance-candidate events.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --list-projects
  %(prog)s --project Block_Island --output events.csv
  %(prog)s --project Vineyard_Wind --year 2024
  %(prog)s --radius 500 --max-alt 400 --gap 20 --project South_Fork

Notes:
  - baro_alt_m is metres AMSL (OpenSky standard).
  - velocity_ms is metres per second (convert: 1 m/s ≈ 1.94 kt).
  - All records in stage2_helicopter_tracks are helicopters — no type filter needed.
  - A visit ends when the gap between consecutive positions exceeds --gap minutes.
  - classify_visit() in this script defines what counts as a maintenance event.
        """,
    )
    parser.add_argument("--project", "-p", metavar="NAME",
                        help="Filter by project name (e.g., Block_Island). Omit for all.")
    parser.add_argument("--start", metavar="YYYY-MM-DD", default="2024-01-01",
                        help="Start date inclusive (default: 2024-01-01)")
    parser.add_argument("--end",   metavar="YYYY-MM-DD", default="2025-01-01",
                        help="End date exclusive (default: 2025-01-01)")
    parser.add_argument("--year",  metavar="YYYY", type=int,
                        help="Shortcut: process a full calendar year")
    parser.add_argument("--radius",  metavar="METERS",  type=int, default=SEARCH_RADIUS_M,
                        help=f"Horizontal search radius in metres (default: {SEARCH_RADIUS_M})")
    parser.add_argument("--max-alt", metavar="METERS",  type=int, default=MAX_ALT_M,
                        help=f"Maximum altitude in metres AMSL (default: {MAX_ALT_M})")
    parser.add_argument("--gap",     metavar="MINUTES", type=int, default=VISIT_GAP_MINUTES,
                        help=f"Gap in minutes that starts a new visit (default: {VISIT_GAP_MINUTES})")
    parser.add_argument("--output", "-o", metavar="FILE", type=Path, default=DEFAULT_OUTPUT,
                        help=f"Output CSV file (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--no-db", action="store_true",
                        help="Skip writing events to stage3_helicopter_events table")
    parser.add_argument("--list-projects", action="store_true",
                        help="List available project names and exit")

    test_group = parser.add_argument_group("single-candidate test mode")
    test_group.add_argument("--icao24", metavar="HEX",
                            help="Filter to a single aircraft by ICAO24 hex code")
    test_group.add_argument("--limit", metavar="N", type=int,
                            help="Process only the first N months")
    args = parser.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)

    if args.list_projects:
        list_projects(conn)
        conn.close()
        return 0

    if args.year:
        args.start = f"{args.year}-01-01"
        args.end   = f"{args.year + 1}-01-01"

    d_start = date.fromisoformat(args.start)
    d_end   = date.fromisoformat(args.end)
    months  = list(iter_months(d_start, d_end))
    if args.limit:
        months = months[:args.limit]

    if not args.no_db:
        deleted = ensure_table_and_clear(
            conn, "stage3_helicopter_events", _CREATE_EVENTS,
            date_col="visit_start", d_start=d_start, d_end=d_end,
        )
        print(f"  Cleared {deleted:,} stale events in [{d_start}, {d_end}); "
              f"downstream columns (Phase A imputation) preserved.")

    bbox = get_farm_bbox(conn, args.project, buffer_km=args.radius / 1000 + 10)

    print(f"Parameters:")
    print(f"  Search radius : {args.radius} m")
    print(f"  Max altitude  : {args.max_alt} m AMSL")
    print(f"  Visit gap     : {args.gap} min")
    print(f"  Project       : {args.project or 'all'}")
    print(f"  Date range    : {d_start} → {d_end} ({len(months)} months)")
    print(f"  Bbox          : lat [{bbox['bbox_lat_min']:.2f}, {bbox['bbox_lat_max']:.2f}]  "
          f"lon [{bbox['bbox_lon_min']:.2f}, {bbox['bbox_lon_max']:.2f}]")
    print(f"  Output        : {args.output}")
    print()

    all_events  = []
    total_hits  = 0

    for i, (ms, me) in enumerate(months, 1):
        print(f"  [{i:>2}/{len(months)}] {ms.strftime('%Y-%m')} ...", end=" ", flush=True)
        hits = fetch_proximity_hits(conn, ms, me,
                                    args.radius, args.max_alt,
                                    args.project, bbox,
                                    icao24=args.icao24)
        if not hits:
            print("0 hits")
            continue

        visits = aggregate_visits(hits, args.gap)

        # Pre-load turbine coordinates so virtual_gap_fix can compute the
        # great-circle closest-approach point during gaps without a per-visit
        # round-trip to the DB.
        if "_turbine_coords" not in locals():
            with conn.cursor() as _tc_cur:
                _tc_cur.execute(
                    "SELECT turbine_code, latitude, longitude FROM wind_turbines")
                _turbine_coords = {tc: (lat, lon) for tc, lat, lon in _tc_cur.fetchall()}

        events = []
        for v in visits:
            t_lat, t_lon = _turbine_coords.get(v["turbine_code"], (None, None))
            qualifies, score, sub_scores = classify_visit(
                v["_positions"], turbine_lat=t_lat, turbine_lon=t_lon)
            if qualifies:
                v["score"] = score
                v["s_evidence"]      = sub_scores.get("s_evidence")
                v["s_duration"]      = sub_scores.get("s_duration")
                v["n_virtual_fixes"] = sub_scores.get("n_virtual_fixes", 0)
                events.append(v)

        # Deduplicate: merge overlapping events, keep best turbine per cluster
        pre_dedup = len(events)
        events = deduplicate_events(events)
        total_hits += len(hits)

        # Enrich events with transit time data
        transit_count = 0
        for ev in events:
            transit = detect_helicopter_transit(conn, ev)
            if transit:
                ev.update(transit)
                transit_count += 1

        if not args.no_db and events:
            save_events(conn, events)

        transit_info = f" ({transit_count} with transit)" if transit_count else ""
        dedup_info = f" [dedup {pre_dedup}→{len(events)}]" if pre_dedup != len(events) else ""
        print(f"{len(hits):,} hits → {len(visits)} visits → {len(events)} events{dedup_info}{transit_info}")
        all_events.extend(events)

    conn.close()

    print(f"\nDone. {total_hits:,} total hits → {len(all_events)} maintenance-candidate events")

    if not all_events:
        print("No events found. classify_visit() may need thresholds adjusting.")
        return 0

    # Write CSV (strip internal _positions key)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for event in all_events:
            writer.writerow({k: event[k] for k in OUTPUT_FIELDS})

    print(f"Saved: {args.output}")

    by_project = Counter(e["project_name"] for e in all_events)
    print("\nEvents by project:")
    for project, count in sorted(by_project.items()):
        print(f"  {project:<28} {count:>4}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
