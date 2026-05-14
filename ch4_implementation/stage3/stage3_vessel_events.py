#!/usr/bin/env python3
# Stage 3 — detect vessel maintenance-candidate events near offshore wind turbines.
#
# Reads stage2_vessel_tracks, groups positions near each turbine into "visits",
# and classifies each visit into a confidence tier based on speed, proximity,
# and AIS data continuity:
#
#   Tier 1 — SURE:   continuous AIS coverage (max gap ≤ MAX_CONT_GAP_MIN),
#                    vessel stopped or slow (min SOG ≤ SOG_STOP_KT),
#                    close to a specific turbine (min distance ≤ RADIUS_SURE_M),
#                    stay duration ≥ MIN_DURATION_MIN.
#                    Turbine attribution is reliable.
#
#   Tier 2 — LIKELY: vessel was slow / stopped near the wind farm, but one or
#                    more conditions prevent certain turbine attribution:
#                    AIS gap > MAX_CONT_GAP_MIN (vessel disappeared then reappeared),
#                    OR closest approach was 500–2000 m (farm area but not pinned).
#                    Maintenance likely but exact turbine unknown.
#
# Positions in stage2_vessel_tracks are from vessels that already passed Stage 1
# (i.e. were within ~2 km of a turbine on that date), so no global scan is needed.
#
# Output:
# - PostgreSQL table : stage3_vessel_events
# - CSV file         : stage3_vessel_events.csv
#
# Usage:
#   python stage3_vessel_events.py
#   python stage3_vessel_events.py --project Vineyard_Wind
#   python stage3_vessel_events.py --year 2024 --output events.csv
#   python stage3_vessel_events.py --dry-run
#
# See: stage2_vessel_fetch_tracks.py -- Stage 2: produces stage2_vessel_tracks.

import argparse
import csv
import math
import os
import statistics
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras

from pipeline_common import (
    DB_CONFIG, conn, haversine_m,
    peak_ev_top_k_median, s_dur_linear, virtual_gap_fix,
    score_to_tier_bucket,
)

DEFAULT_OUTPUT = Path("/mnt/d/thesis/presentation/stage3_vessel_events.csv")
DEFAULT_YEAR   = 2024

# Detection parameters

## Spatial search radius for candidate positions (metres).
SEARCH_RADIUS_M   = 2000

## Within this radius a stop is attributed to the specific turbine (Tier 1).
## CTVs make gangway contact at <50 m; 100 m covers GPS uncertainty.
RADIUS_SURE_M     = 100

## Maximum SOG (knots) to count as "stopped" for Tier 1 flag_sog.
SOG_STOP_KT       = 0.5

## Maximum SOG (knots) for Tier 2 "slow / working speed".
SOG_SLOW_KT       = 2.0

## Time gap (minutes) between consecutive positions that starts a new visit.
VISIT_GAP_MIN     = 30

## Maximum allowed gap within a Tier-1 visit (continuous coverage threshold).
MAX_CONT_GAP_MIN  = 15

## Minimum visit duration (minutes) to report.
MIN_DURATION_MIN  = 10

# SOV-specific classification

## Minimum duration (minutes) to classify a stop as an SOV station (12 h).
SOV_MIN_DURATION_MIN = 720

## Cumulative time (min) slow (≤SOG_STOP_KT) inside RADIUS_SURE_M to flag dwell.
MIN_DWELL_MIN    = 5.0
## Cumulative time (min) at any speed inside RADIUS_SURE_M for approach-cycle T1.
MIN_APPROACH_MIN = 20.0

## Maximum SOG (knots) for an SOV — DP-held vessels are essentially stationary.
SOV_MAX_SOG_KT   = 0.15

## Maximum distance (m) from any turbine for an SOV station event.
## SOVs don't pin to individual turbines; farm-area proximity is sufficient.
SOV_MAX_DIST_M   = 500

# Port-of-call detection

## Hours to look back in vessel_data_ais before visit_start for departure port.
PORT_LOOKBACK_H    = 48

## Hours to look forward after visit_end for return port.
PORT_RETURN_H      = 36

## Vessel must come within this distance (km) of an osm_ports entry to count.
PORT_RADIUS_KM     = 2.0

## Maximum SOG (knots) while near a port to confirm docking (not just transiting).
PORT_SOG_KT        = 1.0

## Minimum time (minutes) spent near a port to count as a stop.
## 5 min is sufficient — vessels stop transmitting shortly after docking,
## so the AIS trail at the berth is often only a few positions long.
PORT_MIN_DUR_MIN   = 5
MAX_TRANSIT_MIN    = 300   # 5 h cap — no US East Coast CTV transit exceeds this

# Continuous scoring (mirrors helicopter pipeline; 2 channels)
#
# Per-position evidence has 2 channels (proximity + SOG); helicopter has a
# 3rd altitude channel which has no vessel analogue. Visit-level aggregation
# (peak_ev = median(top-K), s_dur, score formula, threshold) is identical to
# helicopter — both pipelines now share `peak_ev_top_k_median` and
# `s_dur_linear` from pipeline_common.

## Distance at which proximity sub-score saturates to zero.
## Tighter than helicopter's 1000 m: vessels on the surface have higher
## positional precision and operationally meaningful proximity is closer.
SCORE_PROX_DENOM_M = 200.0

## SOG at which speed sub-score saturates to zero (knots).
SCORE_SOG_DENOM_KT = 1.5

## Per-position evidence weights (must sum to 1).
SCORE_W_PROX = 0.65
SCORE_W_SOG  = 0.35

## Visit-level aggregation weights (must sum to 1; mirrors helicopter).
SCORE_W_EVIDENCE = 0.80
SCORE_W_DURATION = 0.20
SCORE_DUR_DENOM  = 60.0

## Top-K median peak-evidence (matches helicopter).
SCORE_TOPK_FOR_PEAK = 5

## Score threshold for an event to qualify (mirror helicopter; 0-100).
SCORE_THRESHOLD = 40.0

## High-band threshold for backward-compatible tier=1 derivation.
SCORE_HIGH = 75.0

## Hard pre-filters (mirror helicopter for symmetry).
SCORE_MIN_POSITIONS = 3
SCORE_MIN_DURATION  = 5  # minutes


def _score_vessel_point(distance_m: float | None,
                        sog_kt: float | None) -> float | None:
    """Per-position evidence for the vessel classifier (2-channel:
    proximity + SOG). Returns None if `distance_m` is missing.

    Shared between real fixes and virtual gap-fixes — the only difference
    at the call site is whether the result is multiplied by the virtual
    fix's `confidence` weight before joining the per-position evidence list.
    """
    if distance_m is None:
        return None
    s_prox = min(1.0, max(0.0, 1.0 - float(distance_m) / SCORE_PROX_DENOM_M))
    if sog_kt is not None:
        s_sog = min(1.0, max(0.0, 1.0 - float(sog_kt) / SCORE_SOG_DENOM_KT))
    else:
        s_sog = 0.5  # neutral fallback when SOG missing
    return SCORE_W_PROX * s_prox + SCORE_W_SOG * s_sog


def compute_visit_score(positions: list[dict],
                        turbine_lat: float | None,
                        turbine_lon: float | None) -> dict | None:
    """Score a visit using 2-way per-position co-location evidence + virtual
    gap-fix inference (folded into the same continuous score, replacing the
    old separate Tier-3 INFERRED event class).

    Per real position p:
      s_prox = max(0, 1 - distance_m / SCORE_PROX_DENOM_M)   # 200 m
      s_sog  = max(0, 1 - sog_kt / SCORE_SOG_DENOM_KT)       # 1.5 kt
      ev_p   = SCORE_W_PROX·s_prox + SCORE_W_SOG·s_sog

    For each gap between consecutive positions where the great-circle
    closest-approach to the turbine is meaningful (`virtual_gap_fix`),
    a virtual fix is generated; its evidence is multiplied by the
    confidence weight `1 - gap_min/MAX_GAP` before joining the per-position
    list.

    Aggregation:
      peak_ev = median(top-K) of point_ev (real + virtual)
      s_dur   = min(1, duration_min / SCORE_DUR_DENOM)
      score   = 100 · (W_EVIDENCE·peak_ev + W_DURATION·s_dur)

    Returns a dict with keys {score, s_evidence, s_duration, n_virtual_fixes},
    or None if the visit fails the hard pre-filters. Caller checks
    `score >= SCORE_THRESHOLD` to decide whether the visit qualifies.

    Position dicts may use either 'lat'/'lon' (vessel pipeline) or
    'latitude'/'longitude' (helicopter / Stage 2 schema). We normalise.
    """
    if len(positions) < SCORE_MIN_POSITIONS:
        return None

    first, last = positions[0], positions[-1]
    duration_min = (last["time"] - first["time"]).total_seconds() / 60
    if duration_min < SCORE_MIN_DURATION:
        return None

    def _coords(p: dict) -> tuple[float | None, float | None]:
        return (p.get("lat", p.get("latitude")),
                p.get("lon", p.get("longitude")))

    def _sog(p: dict) -> float | None:
        return p.get("sog_kt", p.get("sog"))

    point_ev: list[float] = []

    # Real fixes
    for p in positions:
        ev = _score_vessel_point(p.get("distance_m"), _sog(p))
        if ev is not None:
            point_ev.append(ev)

    # Virtual gap-fixes (only if turbine coords supplied)
    n_virtual = 0
    if turbine_lat is not None and turbine_lon is not None:
        for prev_pos, next_pos in zip(positions[:-1], positions[1:]):
            plat, plon = _coords(prev_pos)
            nlat, nlon = _coords(next_pos)
            if None in (plat, plon, nlat, nlon):
                continue
            prev = {"lat": plat, "lon": plon, "time_utc": prev_pos["time"],
                    "sog_kt": _sog(prev_pos)}
            nxt  = {"lat": nlat, "lon": nlon, "time_utc": next_pos["time"],
                    "sog_kt": _sog(next_pos)}
            vfix = virtual_gap_fix(prev, nxt, turbine_lat, turbine_lon)
            if vfix is None:
                continue
            ev = _score_vessel_point(vfix["distance_m"], vfix.get("sog_kt"))
            if ev is None:
                continue
            point_ev.append(ev * vfix["confidence"])
            n_virtual += 1

    if not point_ev:
        return None

    peak_evidence = peak_ev_top_k_median(point_ev, k=SCORE_TOPK_FOR_PEAK)
    s_dur = s_dur_linear(duration_min, denom=SCORE_DUR_DENOM)

    raw_score = (SCORE_W_EVIDENCE * peak_evidence + SCORE_W_DURATION * s_dur) * 100
    score = round(min(100.0, max(0.0, raw_score)), 1)

    return {
        "score":           score,
        "s_evidence":      peak_evidence,
        "s_duration":      s_dur,
        "n_virtual_fixes": n_virtual,
    }

# Output schema

OUTPUT_FIELDS = [
    "vessel_name", "vessel_type", "vessel_category",
    "project_name", "turbine_code", "turbine_name",
    "visit_start", "visit_end", "duration_minutes",
    "n_positions",
    "min_distance_m", "mean_distance_m",
    "min_sog_kt", "median_sog_kt",
    "max_gap_minutes",
    "tier", "tier_reason",
    "score", "s_evidence", "s_duration", "n_virtual_fixes",
    "flag_duration", "flag_proximity", "flag_sog", "flag_continuity",
    "flag_dwell", "flag_proximity_extended",
    "operation_type",
    "departure_port", "departure_time", "transit_out_min",
    "return_port",    "return_time",    "transit_back_min",
    "transit_dist_km",
]

_ALTER_ADD_FLAGS = """
ALTER TABLE stage3_vessel_events
    ADD COLUMN IF NOT EXISTS flag_duration             BOOLEAN,
    ADD COLUMN IF NOT EXISTS flag_proximity            BOOLEAN,
    ADD COLUMN IF NOT EXISTS flag_sog                  BOOLEAN,
    ADD COLUMN IF NOT EXISTS flag_continuity           BOOLEAN,
    ADD COLUMN IF NOT EXISTS flag_dwell                BOOLEAN,
    ADD COLUMN IF NOT EXISTS flag_proximity_extended   BOOLEAN;
"""

_ALTER_ADD_OPTYPE = """
ALTER TABLE stage3_vessel_events
    ADD COLUMN IF NOT EXISTS operation_type TEXT;
"""

_ALTER_ADD_PORT_COLS = """
ALTER TABLE stage3_vessel_events
    ADD COLUMN IF NOT EXISTS departure_port   TEXT,
    ADD COLUMN IF NOT EXISTS departure_time   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS transit_out_min  REAL,
    ADD COLUMN IF NOT EXISTS return_port      TEXT,
    ADD COLUMN IF NOT EXISTS return_time      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS transit_back_min REAL,
    ADD COLUMN IF NOT EXISTS transit_dist_km  REAL;
"""

_ALTER_ADD_SCORE = """
ALTER TABLE stage3_vessel_events
    ADD COLUMN IF NOT EXISTS score             REAL,
    ADD COLUMN IF NOT EXISTS s_evidence        REAL,
    ADD COLUMN IF NOT EXISTS s_duration        REAL,
    ADD COLUMN IF NOT EXISTS n_virtual_fixes   INTEGER DEFAULT 0;
"""

_CREATE_UPSERT_IDX = """
CREATE UNIQUE INDEX IF NOT EXISTS stage3_vessel_events_upsert_idx
    ON stage3_vessel_events (mms_id, project_name, visit_start, COALESCE(turbine_code, ''));
"""

_MIGRATE_UPSERT_IDX = """
ALTER TABLE stage3_vessel_events
    DROP CONSTRAINT IF EXISTS stage3_vessel_events_mms_id_project_name_visit_start_key;
DROP INDEX IF EXISTS stage3_vessel_events_upsert_idx;
"""

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS stage3_vessel_events (
    id               SERIAL,
    mms_id           TEXT             NOT NULL,
    vessel_name      TEXT,
    vessel_type      SMALLINT,
    vessel_category  TEXT,
    project_name     TEXT             NOT NULL,
    turbine_code     TEXT,
    turbine_name     TEXT,
    visit_start      TIMESTAMPTZ      NOT NULL,
    visit_end        TIMESTAMPTZ      NOT NULL,
    duration_minutes REAL             NOT NULL,
    n_positions      INTEGER          NOT NULL,
    min_distance_m   REAL             NOT NULL,
    mean_distance_m  REAL             NOT NULL,
    min_sog_kt       REAL,
    median_sog_kt    REAL,
    max_gap_minutes  REAL             NOT NULL,
    tier             SMALLINT         NOT NULL,
    tier_reason      TEXT             NOT NULL,
    score            REAL,
    s_evidence       REAL,
    s_duration       REAL,
    n_virtual_fixes  INTEGER          DEFAULT 0,
    flag_duration              BOOLEAN,
    flag_proximity             BOOLEAN,
    flag_sog                   BOOLEAN,
    flag_continuity            BOOLEAN,
    flag_dwell                 BOOLEAN,
    flag_proximity_extended    BOOLEAN,
    operation_type             TEXT,
    departure_port   TEXT,
    departure_time   TIMESTAMPTZ,
    transit_out_min  REAL,
    return_port      TEXT,
    return_time      TIMESTAMPTZ,
    transit_back_min REAL,
    transit_dist_km  REAL
);
"""

_ALTER_ADD_CATEGORY = """
ALTER TABLE stage3_vessel_events
    ADD COLUMN IF NOT EXISTS vessel_category TEXT;
"""

_UPSERT_EVENT = """
INSERT INTO stage3_vessel_events
    (mms_id, vessel_name, vessel_type, vessel_category,
     project_name, turbine_code, turbine_name,
     visit_start, visit_end, duration_minutes, n_positions,
     min_distance_m, mean_distance_m, min_sog_kt, median_sog_kt,
     max_gap_minutes, tier, tier_reason,
     score, s_evidence, s_duration, n_virtual_fixes,
     flag_duration, flag_proximity, flag_sog, flag_continuity,
     flag_dwell, flag_proximity_extended,
     operation_type,
     departure_port, departure_time, transit_out_min,
     return_port,    return_time,    transit_back_min,
     transit_dist_km)
VALUES %s
ON CONFLICT (mms_id, project_name, visit_start, COALESCE(turbine_code, ''))
DO UPDATE SET vessel_category  = EXCLUDED.vessel_category,
              tier                       = EXCLUDED.tier,
              tier_reason                = EXCLUDED.tier_reason,
              score                      = EXCLUDED.score,
              s_evidence                 = EXCLUDED.s_evidence,
              s_duration                 = EXCLUDED.s_duration,
              n_virtual_fixes            = EXCLUDED.n_virtual_fixes,
              flag_duration              = EXCLUDED.flag_duration,
              flag_proximity             = EXCLUDED.flag_proximity,
              flag_sog                   = EXCLUDED.flag_sog,
              flag_continuity            = EXCLUDED.flag_continuity,
              flag_dwell                 = EXCLUDED.flag_dwell,
              flag_proximity_extended    = EXCLUDED.flag_proximity_extended,
              operation_type             = EXCLUDED.operation_type,
              departure_port   = EXCLUDED.departure_port,
              departure_time   = EXCLUDED.departure_time,
              transit_out_min  = EXCLUDED.transit_out_min,
              return_port      = EXCLUDED.return_port,
              return_time      = EXCLUDED.return_time,
              transit_back_min = EXCLUDED.transit_back_min,
              transit_dist_km  = EXCLUDED.transit_dist_km;
"""


# Vessel category classification

def categorise_vessel(vessel_type: int | None,
                      duration_min: float,
                      min_sog: float | None) -> str:
    """
    Classify operational vessel category from AIS vessel_type code and
    behavioural metrics (duration + min SOG).

    Returns one of: "Support", "CTV", "Tug", "Fishing", "Recreational", "Other".

    AIS type codes (ITU-R M.1371 subset relevant here):
      30       Fishing
      31/32/52 Tug
      33       Dredger
      36/37    Sailing / pleasure craft
      40/49    High-speed craft  (most CTVs self-report here)
      50–57    Port service vessels (pilot, SAR, tug …)
      60–69    Passenger
      70–79    Cargo / general cargo
      80–89    Tanker
      90       Other types  (SOVs typically land here)
    """
    vt = vessel_type or 0
    if vt == 30:            return "Fishing"
    if vt in (31, 32, 52):  return "Tug"
    if vt in (36, 37):      return "Recreational"
    if 40 <= vt <= 49:      return "CTV"
    if vt == 90:
        # Support-vessel fingerprint: type 90 + very long stay + essentially stationary (DP).
        # Catches SOVs, heavy-lift/installation, and construction support vessels —
        # indistinguishable from AIS data alone without vessel registry cross-reference.
        sog_ok = min_sog is not None and min_sog <= 0.15
        dur_ok = duration_min >= 720   # 12+ hours
        if sog_ok and dur_ok:
            return "Support"
    if duration_min >= 60:  return "CTV"   # long-stay unknown type → CTV heuristic
    return "Other"


# Helpers

def _cumul_min(times: list, gap_cap_s: float = 300.0) -> float:
    """Sum of inter-observation intervals ≤ gap_cap_s, in minutes.
    Ignores re-entry gaps larger than the cap so only continuous presence counts."""
    if len(times) < 2:
        return 0.0
    total = 0.0
    for i in range(len(times) - 1):
        gap = (times[i + 1] - times[i]).total_seconds()
        if gap <= gap_cap_s:
            total += gap
    return total / 60.0


# `haversine_m` imported from pipeline_common (was: local `_haversine_m`)


# find_gap_candidates() and its Tier-3 INFERRED event class were removed in
# the continuous-score refactor. The closest-approach geometry now lives in
# pipeline_common.virtual_gap_fix() and contributes to the score directly,
# so gap inference is unified with real-fix evidence inside classify_visit().
# The local _segment_point_distance_m helper went with it — its replacement
# pipeline_common.segment_point_distance_m has the same signature.


# Land-proximity port detection fallback

# Module-level cache: OSM port locations bucketed into 0.1° grid cells (~11 km).
# Used to pre-filter slow positions before the expensive osm_land EXISTS scan;
# loaded lazily on first call, reused for the lifetime of the process.
_PORT_CELLS = None


def _load_port_cells(conn):
    global _PORT_CELLS
    if _PORT_CELLS is None:
        cells = set()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT latitude, longitude FROM osm_ports "
                "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
            )
            for plat, plon in cur.fetchall():
                cells.add((round(plat * 10), round(plon * 10)))
        _PORT_CELLS = cells
    return _PORT_CELLS


def _has_port_within_11km(lat: float, lon: float, port_cells: set) -> bool:
    lat_b = round(lat * 10)
    lon_b = round(lon * 10)
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            if (lat_b + dy, lon_b + dx) in port_cells:
                return True
    return False


def _nearest_port_name(conn, lat: float, lon: float, radius_km: float = 5.0):
    """Return name of closest named OSM port within radius_km, or None."""
    pad = radius_km / 111.0
    with conn.cursor() as cur:
        cur.execute("""
            SELECT name FROM osm_ports
            WHERE name IS NOT NULL
              AND latitude  BETWEEN %s AND %s
              AND longitude BETWEEN %s AND %s
            ORDER BY (latitude - %s)^2 + (longitude - %s)^2
            LIMIT 1
        """, (lat - pad, lat + pad, lon - pad, lon + pad, lat, lon))
        row = cur.fetchone()
    return row[0] if row else None


def _find_coastal_stop(conn, rows, pick_last: bool, turbine_lat, turbine_lon):
    """Fallback port detection: find slow positions near land using osm_land.

    Used when no named OSM port is found. Identifies positions within 500 m
    of any land polygon at SOG <= PORT_SOG_KT, then applies the same dwell
    threshold and clustering as the primary port detection.

    Returns (port_name, stop_time, dist_km) or None.
    """
    slow = [(ts, lat, lon) for ts, lat, lon, sog in rows
            if sog is not None and sog <= PORT_SOG_KT]
    if not slow:
        return None

    # Cheap pre-filter: drop slow positions with no OSM port within ~11 km.
    # Vessels at offshore turbines (~35 km out) get filtered here, avoiding
    # the heavy osm_land coastline scan downstream.
    port_cells = _load_port_cells(conn)
    slow = [(ts, lat, lon) for ts, lat, lon in slow
            if _has_port_within_11km(lat, lon, port_cells)]
    if not slow:
        return None

    # Batch: which slow positions are within 500 m of land?
    PAD = 0.005  # ~500 m in degrees — bbox pre-filter for GIST index
    near_land = []
    with conn.cursor() as cur:
        for ts, lat, lon in slow:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM osm_land
                    WHERE geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
                      AND ST_DWithin(geometry::geography,
                          ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, 500)
                )
            """, (lon - PAD, lat - PAD, lon + PAD, lat + PAD, lon, lat))
            if cur.fetchone()[0]:
                near_land.append((ts, lat, lon))

    if not near_land:
        return None

    near_land.sort(key=lambda x: x[0])

    # Cumulative dwell (gap-tolerant <= 60 min)
    dwell = sum(
        (near_land[j + 1][0] - near_land[j][0]).total_seconds() / 60
        for j in range(len(near_land) - 1)
        if (near_land[j + 1][0] - near_land[j][0]).total_seconds() / 60 <= 60
    )
    if dwell < PORT_MIN_DUR_MIN:
        return None

    # Cluster by 60-min gaps
    clusters = [[near_land[0]]]
    for j in range(1, len(near_land)):
        gap = (near_land[j][0] - near_land[j - 1][0]).total_seconds() / 60
        if gap > 60:
            clusters.append([near_land[j]])
        else:
            clusters[-1].append(near_land[j])

    if pick_last:
        stop_ts, stop_lat, stop_lon = clusters[-1][-1]
    else:
        stop_ts, stop_lat, stop_lon = clusters[0][0]

    port_name = _nearest_port_name(conn, stop_lat, stop_lon, radius_km=5.0)
    if not port_name:
        port_name = f"coastal stop ({stop_lat:.3f}, {stop_lon:.3f})"

    dist_km = haversine_m(stop_lat, stop_lon, turbine_lat, turbine_lon) / 1_000
    return (port_name, stop_ts, dist_km)


# Per-process AIS cache keyed by mms_id. Holds (window_start, window_end, rows)
# so repeat _scan_window calls for the same vessel within an overlapping window
# slice from memory instead of re-querying TimescaleDB compressed chunks.
# Each detect_port_calls() invocation issues 2 _scan_window calls; a hit with
# multiple visits multiplies that — caching collapses 6+ SQL queries to 1.
_AIS_WINDOW_CACHE: dict = {}


def _fetch_ais_window_cached(conn, mms_id: str, start_dt, end_dt):
    cached = _AIS_WINDOW_CACHE.get(mms_id)
    if cached is None or start_dt < cached[0] or end_dt > cached[1]:
        # Cache miss or out-of-range — refetch a wider window so adjacent
        # _scan_window calls (departure + return) hit the cache.
        wide_start = start_dt - timedelta(hours=24)
        wide_end   = end_dt   + timedelta(hours=24)
        if cached is not None:
            wide_start = min(wide_start, cached[0])
            wide_end   = max(wide_end,   cached[1])
        with conn.cursor() as cur:
            cur.execute("""
                SELECT time, latitude, longitude, speed_over_ground
                FROM vessel_data_ais
                WHERE mms_id = %s
                  AND time >= %s AND time < %s
                  AND latitude IS NOT NULL AND longitude IS NOT NULL
                ORDER BY time
            """, (mms_id, wide_start, wide_end))
            all_rows = cur.fetchall()
        _AIS_WINDOW_CACHE[mms_id] = (wide_start, wide_end, all_rows)
        cached = _AIS_WINDOW_CACHE[mms_id]

    _, _, all_rows = cached
    return [r for r in all_rows if start_dt <= r[0] < end_dt]


def detect_port_calls(conn, mms_id: str,
                      visit_start, visit_end,
                      turbine_lat: float, turbine_lon: float) -> dict:
    """
    Detect departure and return port for one maintenance_visit event.

    Queries vessel_data_ais with a wide time window (48 h before / 24 h after),
    computes the track bounding box, fetches all named osm_ports within it, then
    finds ports the vessel was within PORT_RADIUS_KM of at SOG ≤ PORT_SOG_KT for
    at least PORT_MIN_DUR_MIN minutes.

    Returns: dict with keys: departure_port, departure_time, transit_out_min,
                            return_port, return_time, transit_back_min,
                            transit_dist_km  (all may be None if not found).
    """
    from datetime import timezone as _tz

    def _scan_window(start_dt, end_dt, pick_last: bool):
        """Fetch AIS positions in window (cached), find port stops; return best match."""
        rows = _fetch_ais_window_cached(conn, mms_id, start_dt, end_dt)
        if not rows:
            return None

        lats = [r[1] for r in rows]
        lons = [r[2] for r in rows]
        bbox_pad = 0.1   # degrees ~ 8 km padding
        min_lat, max_lat = min(lats) - bbox_pad, max(lats) + bbox_pad
        min_lon, max_lon = min(lons) - bbox_pad, max(lons) + bbox_pad

        # Fetch candidate ports within bbox
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, latitude, longitude
                FROM osm_ports
                WHERE name IS NOT NULL
                  AND latitude  BETWEEN %s AND %s
                  AND longitude BETWEEN %s AND %s
            """, (min_lat, max_lat, min_lon, max_lon))
            ports = cur.fetchall()

        if not ports:
            return None

        # For each port, find vessel positions within PORT_RADIUS_KM at low SOG
        M_PER_DEG = 111_320.0
        best = None   # (port_name, stop_time, dist_km_to_turbine)

        for pname, plat, plon in ports:
            cos_lat = math.cos(math.radians((plat + turbine_lat) / 2))
            nearby = []
            for ts, lat, lon, sog in rows:
                if sog is not None and sog > PORT_SOG_KT:
                    continue
                dy = (lat - plat) * M_PER_DEG
                dx = (lon - plon) * M_PER_DEG * cos_lat
                dist_m = math.sqrt(dx * dx + dy * dy)
                if dist_m <= PORT_RADIUS_KM * 1_000:
                    nearby.append((ts, dist_m))

            if not nearby:
                continue

            # Require minimum cumulative dwell time (gap-tolerant: ≤ 60 min gap)
            nearby.sort(key=lambda x: x[0])
            dwell = 0.0
            for j in range(len(nearby) - 1):
                gap = (nearby[j + 1][0] - nearby[j][0]).total_seconds() / 60
                if gap <= 60:
                    dwell += gap
            if dwell < PORT_MIN_DUR_MIN:
                continue

            # Cluster nearby timestamps by 60-min gaps to separate
            # distinct port visits (e.g. evening docking vs morning departure)
            clusters = [[nearby[0]]]
            for j in range(1, len(nearby)):
                gap = (nearby[j][0] - nearby[j - 1][0]).total_seconds() / 60
                if gap > 60:
                    clusters.append([nearby[j]])
                else:
                    clusters[-1].append(nearby[j])

            # Departure (pick_last): last cluster's last timestamp
            # Return (not pick_last): first cluster's first timestamp
            if pick_last:
                stop_time = clusters[-1][-1][0]
            else:
                stop_time = clusters[0][0][0]

            dist_km = haversine_m(plat, plon, turbine_lat, turbine_lon) / 1_000
            best = (pname, stop_time, dist_km)

        # Fallback: if no named port found, try land-proximity detection
        if best is None:
            best = _find_coastal_stop(conn, rows, pick_last,
                                      turbine_lat, turbine_lon)

        return best

    result = {
        "departure_port": None, "departure_time": None, "transit_out_min": None,
        "return_port":    None, "return_time":    None, "transit_back_min": None,
        "transit_dist_km": None,
    }

    # Outbound: look back up to PORT_LOOKBACK_H before visit_start
    out_start = visit_start - timedelta(hours=PORT_LOOKBACK_H)
    dep = _scan_window(out_start, visit_start, pick_last=True)
    if dep:
        pname, dep_time, dist_km = dep
        result["departure_port"]  = pname
        result["departure_time"]  = dep_time
        transit = round((visit_start - dep_time).total_seconds() / 60, 1)
        result["transit_out_min"] = transit if transit <= MAX_TRANSIT_MIN else None
        result["transit_dist_km"] = round(dist_km, 2)

    # Return: look forward up to PORT_RETURN_H after visit_end
    ret_end = visit_end + timedelta(hours=PORT_RETURN_H)
    ret = _scan_window(visit_end, ret_end, pick_last=False)
    if ret:
        pname, ret_time, _ = ret
        transit = round((ret_time - visit_end).total_seconds() / 60, 1)
        result["return_port"]      = pname
        result["return_time"]      = ret_time
        result["transit_back_min"] = transit if transit <= MAX_TRANSIT_MIN else None

    return result


def load_turbines(conn, project_name=None):
    """Return list of turbine dicts for the given project (or all)."""
    sql = "SELECT turbine_code, turbine_name, project_name, latitude, longitude FROM wind_turbines"
    params = ()
    if project_name:
        sql += " WHERE project_name = %s"
        params = (project_name,)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [
            {"code": r[0], "name": r[1], "project": r[2], "lat": r[3], "lon": r[4]}
            for r in cur.fetchall()
        ]


def load_stage1_hits(conn, year: int, mmsi: str | None = None,
                     single_date: str | None = None,
                     limit: int | None = None):
    """Return (mms_id, vessel_date) pairs from Stage 1 hits for the year."""
    conditions = ["EXTRACT(YEAR FROM vessel_date) = %s"]
    params: list = [year]
    if mmsi:
        conditions.append("mms_id = %s")
        params.append(mmsi)
    if single_date:
        conditions.append("vessel_date = %s")
        params.append(single_date)
    limit_clause = f" LIMIT {limit}" if limit else ""
    sql = (f"SELECT DISTINCT mms_id, vessel_date "
           f"FROM stage1_vessel_hits "
           f"WHERE {' AND '.join(conditions)} "
           f"ORDER BY vessel_date, mms_id{limit_clause}")
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fetch_near_turbine_positions(conn, mms_id: str, vessel_date: date,
                                  turbines: list, radius_m: float):
    """
    Fetch all stage2 positions for this vessel ±1 day, filtered to within
           radius_m of at least one turbine in the list.

    Returns list of dicts: time_utc, lat, lon, sog, vessel_name, vessel_type,
                           nearest_turbine_code, nearest_turbine_project, distance_m.
    """
    start_dt = datetime(vessel_date.year, vessel_date.month, vessel_date.day,
                        tzinfo=timezone.utc) - timedelta(days=1)
    end_dt   = start_dt + timedelta(days=3)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT time_utc, latitude, longitude, speed_over_ground,
                   vessel_name, vessel_type
            FROM stage2_vessel_tracks
            WHERE mms_id = %s
              AND time_utc >= %s AND time_utc < %s
              AND latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY time_utc
            """,
            (mms_id, start_dt, end_dt),
        )
        rows = cur.fetchall()

    if not rows:
        return []

    results = []
    for time_utc, lat, lon, sog, vessel_name, vessel_type in rows:
        # find nearest turbine
        best_dist = float("inf")
        best_t    = None
        for t in turbines:
            d = haversine_m(lat, lon, t["lat"], t["lon"])
            if d < best_dist:
                best_dist = d
                best_t    = t

        if best_dist <= radius_m:
            results.append({
                "time":     time_utc,
                "lat":      lat,
                "lon":      lon,
                "sog":      sog,          # knots (may be None)
                "vessel_name": vessel_name,
                "vessel_type": vessel_type,
                "turbine":  best_t,
                "distance_m": best_dist,
            })

    return results


def segment_visits(positions: list, gap_minutes: float) -> list[list]:
    """
    Split a time-sorted position list into visit segments.

    A new segment starts when the gap between consecutive positions exceeds
    gap_minutes.

    positions: List of position dicts (must have 'time' key).
    gap_minutes: Gap threshold in minutes.
    Returns: List of segments (each a list of position dicts).
    """
    if not positions:
        return []
    segments = [[positions[0]]]
    for pos in positions[1:]:
        gap = (pos["time"] - segments[-1][-1]["time"]).total_seconds() / 60
        if gap > gap_minutes:
            segments.append([])
        segments[-1].append(pos)
    return segments


def classify_visit(positions: list, turbines: list | None = None) -> list[dict] | None:
    """
    Compute the continuous evidence score for a visit segment and
           emit one or more event records.

    Tier (0, 1, 2) is derived from the score: tier=1 for score>=75,
    tier=2 for 40<=score<75, tier=0 for score<40 (sub-threshold). All
    qualifying-by-pre-filter visits are emitted regardless of score band so
    the threshold remains tunable post-hoc; downstream queries apply the
    threshold (Stage 4: `WHERE tier IN (1, 2)`). The structural branching
    (Support cluster / CTV cluster / farm-level) is preserved for
    *event-emission shape* — which turbines to attribute the visit to.

    positions: Time-sorted list of position dicts from segment_visits().
    turbines: Optional full turbine list, for cluster expansion.
    Returns: List of event dicts, or None if the visit fails the
                      hard pre-filters (n_positions < 3 or duration < 5 min).
    """
    if len(positions) < SCORE_MIN_POSITIONS:
        return None

    t_start  = positions[0]["time"]
    t_end    = positions[-1]["time"]
    duration = (t_end - t_start).total_seconds() / 60

    if duration < SCORE_MIN_DURATION:
        return None

    distances = [p["distance_m"] for p in positions]
    sogs      = [p["sog"] for p in positions if p["sog"] is not None]

    min_dist  = min(distances)
    mean_dist = statistics.mean(distances)
    min_sog   = min(sogs)   if sogs else None
    med_sog   = statistics.median(sogs) if sogs else None

    # largest gap between consecutive positions (minutes)
    gaps = [
        (positions[i + 1]["time"] - positions[i]["time"]).total_seconds() / 60
        for i in range(len(positions) - 1)
    ]
    max_gap = max(gaps) if gaps else 0.0

    # turbine attribution — use the turbine seen at closest approach
    closest_pos  = min(positions, key=lambda p: p["distance_m"])
    turbine      = closest_pos["turbine"]
    vessel_name  = next((p["vessel_name"] for p in positions if p["vessel_name"]), None)
    vessel_type  = next((p["vessel_type"] for p in positions if p["vessel_type"]), None)

    # Explicit per-criterion flags
    flag_duration    = duration    >= MIN_DURATION_MIN          # always True here
    flag_proximity   = min_dist    <= RADIUS_SURE_M
    flag_sog         = (min_sog is not None) and (min_sog <= SOG_STOP_KT)
    flag_continuity  = max_gap     <= MAX_CONT_GAP_MIN
    is_slow          = (min_sog is not None) and (min_sog <= SOG_SLOW_KT)

    # Joint flags: dwell (slow inside 100m) and approach cycle (any speed)
    _slow_close = sorted(
        [p for p in positions
         if p["distance_m"] <= RADIUS_SURE_M
         and p.get("sog") is not None and p["sog"] <= SOG_STOP_KT],
        key=lambda p: p["time"],
    )
    _dwell_min = _cumul_min([p["time"] for p in _slow_close])
    flag_dwell = _dwell_min >= MIN_DWELL_MIN

    _close = sorted(
        [p for p in positions if p["distance_m"] <= RADIUS_SURE_M],
        key=lambda p: p["time"],
    )
    _approach_min = _cumul_min([p["time"] for p in _close])
    flag_proximity_extended = _approach_min >= MIN_APPROACH_MIN

    category = categorise_vessel(vessel_type, duration, min_sog)

    # Continuous score against the closest-approach turbine
    # The score replaces the legacy tier if/elif cascade. Real fixes plus
    # virtual gap-fixes (synthesised at the great-circle closest-approach
    # point of any sufficiently long AIS gap) are aggregated via the same
    # top-K=5 median peak-evidence used in the helicopter pipeline.
    score_result = compute_visit_score(positions, turbine["lat"], turbine["lon"])
    if score_result is None:
        return None
    score          = score_result["score"]
    s_evidence     = score_result["s_evidence"]
    s_duration_val = score_result["s_duration"]
    n_virtual      = score_result["n_virtual_fixes"]

    # Emit ALL qualifying-by-pre-filter visits with their score; tier = 0 for
    # sub-threshold events. The score threshold is applied at downstream
    # query time (Stage 4 uses `WHERE tier IN (1, 2)`), which keeps the
    # threshold tunable post-hoc without re-running the pipeline.
    score_tier = score_to_tier_bucket(score, high=SCORE_HIGH, mid=SCORE_THRESHOLD)
    base_tier_reason = (
        f"score={score:.1f} (peak_ev={s_evidence:.3f}, dur_norm={s_duration_val:.2f})"
    )
    if n_virtual > 0:
        base_tier_reason += f", {n_virtual} virtual gap-fix(es)"

    # Build base event fields shared across all turbine attributions
    base = {
        "vessel_name":      vessel_name,
        "vessel_type":      vessel_type,
        "vessel_category":  category,
        "project_name":     turbine["project"],
        "visit_start":      t_start,
        "visit_end":        t_end,
        "duration_minutes": round(duration, 1),
        "n_positions":      len(positions),
        "min_distance_m":   round(min_dist, 1),
        "mean_distance_m":  round(mean_dist, 1),
        "min_sog_kt":       round(min_sog, 2) if min_sog is not None else None,
        "median_sog_kt":    round(med_sog, 2) if med_sog is not None else None,
        "max_gap_minutes":  round(max_gap, 1),
        "score":            round(score, 2),
        "s_evidence":       round(s_evidence, 4),
        "s_duration":       round(s_duration_val, 4),
        "n_virtual_fixes":  n_virtual,
        "flag_duration":             flag_duration,
        "flag_proximity":            flag_proximity,
        "flag_sog":                  flag_sog,
        "flag_continuity":           flag_continuity,
        "flag_dwell":                flag_dwell,
        "flag_proximity_extended":   flag_proximity_extended,
    }

    # Support station: farm-level, with optional turbine attribution
    # Catches SOVs, heavy-lift/installation vessels, and construction support —
    # all share the same AIS signature (type 90, long DP station, near-zero SOG).
    # When the vessel is within RADIUS_SURE_M of specific turbines, attribute
    # to those turbines; otherwise fall back to farm-level (turbine_code=None).
    # Tier is now derived from the continuous score; the legacy SOV gate
    # (flag_sov_proximity AND flag_sov_sog) is implicit in the score
    # (proximity component decays at 200m, sog at 1.5 kt).
    if category == "Support":
        tier        = score_tier
        tier_reason = f"support station: {base_tier_reason}"
        # Find all turbines within RADIUS_SURE_M — support vessel may be
        # DP-anchored adjacent to a cluster of specific turbines.
        near: dict[str, dict] = {}
        for p in positions:
            if p["distance_m"] <= RADIUS_SURE_M:
                near[p["turbine"]["code"]] = p["turbine"]
        if turbines:
            for t in turbines:
                if t["code"] in near:
                    continue
                dlat = RADIUS_SURE_M / 111_000
                dlon = RADIUS_SURE_M / (111_000 * math.cos(math.radians(t["lat"])))
                for p in positions:
                    if (abs(p["lat"] - t["lat"]) <= dlat and
                            abs(p["lon"] - t["lon"]) <= dlon and
                            haversine_m(p["lat"], p["lon"], t["lat"], t["lon"]) <= RADIUS_SURE_M):
                        near[t["code"]] = t
                        break
        if near:
            events = []
            for tc, td in near.items():
                t_min_dist = min(haversine_m(p["lat"], p["lon"], td["lat"], td["lon"])
                                 for p in positions)
                events.append({**base,
                               "project_name":   td["project"],
                               "turbine_code":   tc,
                               "turbine_name":   td.get("name"),
                               "min_distance_m": round(t_min_dist, 1),
                               "flag_proximity": True,
                               "tier":           tier,
                               "tier_reason":    tier_reason,
                               "operation_type": "support_station"})
            return events
        return [{**base, "turbine_code": None, "turbine_name": None,
                 "tier": tier, "tier_reason": tier_reason,
                 "operation_type": "support_station"}]

    # CTV / other: turbine-specific maintenance
    # Tier comes from the continuous score; flag_dwell / flag_proximity_extended
    # are kept as descriptive metadata in the output but no longer prescriptive.
    elif min_dist <= RADIUS_SURE_M or flag_dwell or flag_proximity_extended:
        tier         = score_tier
        descriptors  = []
        if flag_dwell:                  descriptors.append(f"dwell {_dwell_min:.0f} min")
        if flag_proximity_extended:     descriptors.append(f"approach {_approach_min:.0f} min")
        if not flag_continuity:         descriptors.append(f"AIS gap {max_gap:.0f} min")
        prefix = "; ".join(descriptors) if descriptors else f"min_dist {min_dist:.0f} m"
        tier_reason = f"{prefix}; {base_tier_reason}"
        # Find ALL turbines within RADIUS_SURE_M — vessel may be near a cluster
        near: dict[str, dict] = {}
        for p in positions:
            if p["distance_m"] <= RADIUS_SURE_M:
                near[p["turbine"]["code"]] = p["turbine"]
        if turbines:
            for t in turbines:
                if t["code"] in near:
                    continue
                dlat = RADIUS_SURE_M / 111_000
                dlon = RADIUS_SURE_M / (111_000 * math.cos(math.radians(t["lat"])))
                for p in positions:
                    if (abs(p["lat"] - t["lat"]) <= dlat and
                            abs(p["lon"] - t["lon"]) <= dlon and
                            haversine_m(p["lat"], p["lon"], t["lat"], t["lon"]) <= RADIUS_SURE_M):
                        near[t["code"]] = t
                        break
        if not near:
            near[turbine["code"]] = turbine  # fallback: always include closest
        events = []
        for tc, td in near.items():
            t_min_dist = min(haversine_m(p["lat"], p["lon"], td["lat"], td["lon"])
                             for p in positions)
            events.append({**base,
                           "project_name":   td["project"],
                           "turbine_code":   tc,
                           "turbine_name":   td.get("name"),
                           "min_distance_m": round(t_min_dist, 1),
                           "flag_proximity": True,
                           "tier":           tier,
                           "tier_reason":    tier_reason,
                           "operation_type": "maintenance_visit"})
        return events

    else:
        # Score passed threshold but vessel never came within RADIUS_SURE_M —
        # emit a single farm-level event with no turbine attribution.
        tier        = score_tier
        reasons     = []
        if not flag_continuity: reasons.append(f"AIS gap {max_gap:.0f} min")
        reasons.append(f"min_dist {min_dist:.0f} m")
        tier_reason = "; ".join(reasons) + f"; {base_tier_reason}"
        return [{**base, "turbine_code": None, "turbine_name": None,
                 "tier": tier, "tier_reason": tier_reason,
                 "operation_type": "maintenance_visit"}]


# Entry point

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stage 3 — detect vessel maintenance events near wind turbines.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Confidence tiers:
  Tier 1 SURE   — stopped close to specific turbine, continuous AIS coverage
  Tier 2 LIKELY — slow near farm, but data gaps OR outside 500 m turbine zone

Examples:
  %(prog)s
  %(prog)s --project Vineyard_Wind
  %(prog)s --year 2024 --output events.csv
  %(prog)s --dry-run
        """,
    )
    parser.add_argument("--year",    type=int, default=DEFAULT_YEAR, metavar="YYYY")
    parser.add_argument("--project", help="Filter to one wind farm project name")
    parser.add_argument("--output",  default=str(DEFAULT_OUTPUT), help="Output CSV path")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show candidate count without writing results")

    test_group = parser.add_argument_group("single-candidate test mode")
    test_group.add_argument("--mmsi", metavar="ID",
                            help="Filter to a single vessel by MMSI")
    test_group.add_argument("--single-date", metavar="YYYY-MM-DD",
                            help="Only classify events for this date")
    test_group.add_argument("--limit", metavar="N", type=int,
                            help="Process only the first N (mms_id, date) pairs")
    args = parser.parse_args()

    conn     = psycopg2.connect(**DB_CONFIG)
    turbines = load_turbines(conn, args.project)

    if not turbines:
        print(f"No turbines found{' for project ' + args.project if args.project else ''}.")
        conn.close()
        return 1

    project_names = sorted({t["project"] for t in turbines})
    print(f"Turbines loaded : {len(turbines)} in {len(project_names)} project(s): "
          f"{', '.join(project_names)}")

    # Stage 1 hits give us the (mms_id, date) pairs to process — avoids full table scan
    hits = load_stage1_hits(conn, args.year, mmsi=args.mmsi,
                            single_date=args.single_date, limit=args.limit)
    print(f"Stage 1 hits    : {len(hits):,} (mms_id, date) pairs for {args.year}")

    if args.dry_run:
        print("DRY RUN — exiting without processing.")
        conn.close()
        return 0

    # Ensure output table exists and migrate pre-existing tables
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE)
        cur.execute(_MIGRATE_UPSERT_IDX)   # drop old single-column index if present
        cur.execute(_CREATE_UPSERT_IDX)
        cur.execute(_ALTER_ADD_CATEGORY)
        cur.execute(_ALTER_ADD_FLAGS)
        cur.execute(_ALTER_ADD_OPTYPE)
        cur.execute(_ALTER_ADD_PORT_COLS)
        cur.execute(_ALTER_ADD_SCORE)
    conn.commit()

    all_events: list[dict] = []
    n_processed = 0

    import time as _profile_time
    _DBG = bool(os.environ.get("STAGE3_DEBUG_TIMING"))
    for mms_id, vessel_date in hits:
        _t0 = _profile_time.time()
        positions = fetch_near_turbine_positions(
            conn, mms_id, vessel_date, turbines, SEARCH_RADIUS_M
        )
        _t_fetch = _profile_time.time() - _t0
        if not positions:
            continue

        _t0 = _profile_time.time()
        visits = segment_visits(positions, VISIT_GAP_MIN)
        _t_seg = _profile_time.time() - _t0
        _t_classify = 0.0
        _t_port = 0.0
        for visit in visits:
            _t0 = _profile_time.time()
            events = classify_visit(visit, turbines)
            _t_classify += _profile_time.time() - _t0
            if not events:
                continue
            # Port detection: call once per segment, propagate to all events
            port_info = None
            if any(e.get("operation_type") == "maintenance_visit" for e in events):
                closest = min(visit, key=lambda p: p["distance_m"])
                _t0 = _profile_time.time()
                port_info = detect_port_calls(
                    conn, mms_id,
                    events[0]["visit_start"], events[0]["visit_end"],
                    closest["turbine"]["lat"], closest["turbine"]["lon"],
                )
                _t_port += _profile_time.time() - _t0
            for event in events:
                event["mms_id"] = mms_id
                if port_info and event.get("operation_type") == "maintenance_visit":
                    event.update(port_info)
                all_events.append(event)
        if _DBG:
            print(f"  TIMING mms={mms_id} pos={len(positions)} vis={len(visits)} "
                  f"fetch={_t_fetch:.2f}s seg={_t_seg:.2f}s "
                  f"classify={_t_classify:.2f}s port={_t_port:.2f}s",
                  flush=True)

        # Tier-3 inferred-from-gap path is gone — gap inference now flows
        # through compute_visit_score via virtual gap-fixes.

        n_processed += 1
        if n_processed % 100 == 0:
            pct = 100 * n_processed / len(hits)
            t1  = sum(1 for e in all_events if e["tier"] == 1)
            t2  = sum(1 for e in all_events if e["tier"] == 2)
            print(f"  [{n_processed:>5}/{len(hits)}] {pct:.0f}%  "
                  f"events so far: {t1} Tier-1  {t2} Tier-2", flush=True)

    conn.close()

    # Summary

    t1_events    = [e for e in all_events if e["tier"] == 1]
    t2_events    = [e for e in all_events if e["tier"] == 2]
    sov_events   = [e for e in all_events if e.get("operation_type") == "support_station"]
    maint_events = [e for e in all_events if e.get("operation_type") == "maintenance_visit"]
    n_with_vfix  = sum(1 for e in all_events if (e.get("n_virtual_fixes") or 0) > 0)

    print(f"\n{'═' * 62}")
    print(f"  Year             : {args.year}")
    print(f"  Hits processed   : {n_processed:,}")
    print(f"  Tier 1 (score≥75): {len(t1_events):,} events")
    print(f"  Tier 2 (40≤s<75) : {len(t2_events):,} events")
    print(f"  Events using virtual gap-fix: {n_with_vfix:,}")
    print(f"  -- by type -------------------------------------")
    print(f"  maintenance_visit: {len(maint_events):,}  |  support_station: {len(sov_events):,}")
    if t1_events:
        per_turbine: dict = defaultdict(int)
        for e in t1_events:
            per_turbine[f"{e['project_name']} / {e['turbine_code']}"] += 1
        top5 = sorted(per_turbine.items(), key=lambda x: -x[1])[:5]
        print(f"\n  Top turbines (Tier 1):")
        for k, v in top5:
            print(f"    {k:40s}  {v:4d} events")
    print(f"{'═' * 62}\n")

    if not all_events:
        print("No qualifying events found.")
        return 0

    # Write CSV

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["mms_id"] + OUTPUT_FIELDS)
        w.writeheader()
        for e in sorted(all_events, key=lambda x: (x["visit_start"], x["mms_id"])):
            w.writerow({k: e.get(k) for k in ["mms_id"] + OUTPUT_FIELDS})
    print(f"CSV written  : {out_path}")

    # Write DB

    # Deduplicate on (mms_id, project_name, visit_start): multi-day SOV stays
    # appear in multiple stage1 hit windows; keep the version with most positions.
    seen_keys: dict = {}
    for e in all_events:
        key = (e["mms_id"], e["project_name"], e["visit_start"], e.get("turbine_code") or "")
        if key not in seen_keys or e["n_positions"] > seen_keys[key]["n_positions"]:
            seen_keys[key] = e
    unique_events = list(seen_keys.values())

    conn2 = psycopg2.connect(**DB_CONFIG)
    tuples = [
        (
            e["mms_id"], e["vessel_name"], e["vessel_type"], e["vessel_category"],
            e["project_name"], e["turbine_code"], e["turbine_name"],
            e["visit_start"], e["visit_end"], e["duration_minutes"],
            e["n_positions"], e["min_distance_m"], e["mean_distance_m"],
            e["min_sog_kt"], e["median_sog_kt"], e["max_gap_minutes"],
            e["tier"], e["tier_reason"],
            e.get("score"), e.get("s_evidence"), e.get("s_duration"),
            e.get("n_virtual_fixes") or 0,
            e.get("flag_duration"), e.get("flag_proximity"),
            e.get("flag_sog"),      e.get("flag_continuity"),
            e.get("flag_dwell"),    e.get("flag_proximity_extended"),
            e.get("operation_type"),
            e.get("departure_port"), e.get("departure_time"), e.get("transit_out_min"),
            e.get("return_port"),    e.get("return_time"),    e.get("transit_back_min"),
            e.get("transit_dist_km"),
        )
        for e in unique_events
    ]
    with conn2.cursor() as cur:
        psycopg2.extras.execute_values(cur, _UPSERT_EVENT, tuples)
    conn2.commit()
    conn2.close()
    print(f"DB table     : stage3_vessel_events  ({len(tuples)} rows inserted)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
