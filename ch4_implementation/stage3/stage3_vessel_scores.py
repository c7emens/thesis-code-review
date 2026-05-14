#!/usr/bin/env python3
# DEPRECATED — superseded by the continuous-score classification inside stage3_vessel_events.py.
#
# This script is no longer wired into the pipeline. Its 3-channel continuous
# scoring (peak_evidence × continuity × duration) was the prototype that
# informed the unified architecture: stage3_vessel_events.py now carries
# `score`, `s_evidence`, `s_duration`, and `n_virtual_fixes` columns directly,
# making this companion table redundant.
#
# Kept on disk for historical reference; do not run.
#
# Migration path: read `stage3_vessel_events.{score, s_evidence, s_duration,
# n_virtual_fixes}` instead of `stage3_vessel_scores`. See pipeline_common.py
# for shared scoring primitives, and stage3_vessel_events.py for the
# integrated implementation.
# ---------------------------------------------------------------------------
#
# Original docstring:
#
# Parallel to stage3_vessel_events.py (tier-based), this script assigns a
# continuous confidence score (0–100) to every vessel approach within 200 m
# of a turbine.  No binary thresholds.  The score combines a per-point
# co-location evidence (proximity + SOG + heading-toward-turbine) with
# continuity (median AIS gap) and a log-scaled duration.
#
#   score = 0.70 × peak_evidence + 0.20 × s_continuity + 0.10 × s_duration
#
#   peak_evidence = median of top-5 per-position values, where
#       point_ev = 0.50·s_prox + 0.35·s_sog + 0.15·s_heading
#       s_prox    = max(0, 1 − dist_m / 200)
#       s_sog     = max(0, 1 − sog_kt / 1.5)
#       s_heading = max(0, 1 − |COG − bearing_to_turbine| / 180)   (when SOG > 0.5 kt)
#
#   s_continuity = max(0, 1 − median_gap_min / 30)
#   s_duration   = min(1, log(1 + duration_min) / log(1 + 60))
#
# Score guide (comparable to tier system):
#   ≥ 75  — high confidence   (~Tier 1)
#   40–74 — moderate          (~Tier 2)
#   < 40  — proximity only / low evidence
#
# Output:
# - PostgreSQL table : stage3_vessel_scores
# - CSV file         : /mnt/d/thesis/presentation/stage3_vessel_scores.csv
#
# Usage:
#   python stage3_vessel_scores.py
#   python stage3_vessel_scores.py --project Block_Island
#   python stage3_vessel_scores.py --mmsi 219699000 --limit 5
#   python stage3_vessel_scores.py --dry-run

import argparse
import csv
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras

# Database

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "windfarm",
    "user":     "thesis",
    "password": "thesis2026",
}

DEFAULT_OUTPUT = Path("/mnt/d/thesis/presentation/stage3_vessel_scores.csv")
DEFAULT_YEAR   = 2024

# Detection parameters

## Same candidate search radius as stage3 (positions pre-filtered here).
SEARCH_RADIUS_M   = 2000

## Positions within this radius of a turbine contribute to SOG/gap/duration.
SCORE_WINDOW_M    = 500

## Beyond this distance s_proximity = 0 — no row emitted.
SCORE_PROX_MAX_M  = 200

## Time gap (min) that splits one visit into two separate events.
VISIT_GAP_MIN     = 30

# Score weights and normalisation

W_EVIDENCE   = 0.70   # peak per-point evidence (proximity + SOG + heading)
W_CONTINUITY = 0.20
W_DURATION   = 0.10

# Per-point evidence component weights (within evidence sub-score)
EV_W_PROX    = 0.50
EV_W_SOG     = 0.35
EV_W_HEADING = 0.15

PROX_DENOM = 200.0   # distance (m) at which per-point proximity → 0
SOG_DENOM  = 1.5     # SOG (kt) at which per-point SOG score → 0  (was 2.0)
GAP_DENOM  = 30.0    # MEDIAN gap (min) at which s_continuity → 0  (was max gap / 60)
DUR_DENOM  = 60.0    # used for log-scale: log(1+dur)/log(1+DUR_DENOM)

# Heading is meaningless at near-zero SOG — use neutral fallback
HEADING_MIN_SOG_KT  = 0.5
HEADING_NEUTRAL     = 0.5

# Top-K aggregation: median of top-K per-position evidence values
TOPK_FOR_PEAK = 5

SCORE_HIGH = 60   # ≥ this: high confidence (~Tier 1 equivalent)
SCORE_MED  = 35   # ≥ this: moderate confidence; < this: low

# SQL

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS stage3_vessel_scores (
    id              SERIAL PRIMARY KEY,
    mms_id          TEXT NOT NULL,
    vessel_name     TEXT,
    vessel_type     SMALLINT,
    project_name    TEXT NOT NULL,
    turbine_code    TEXT NOT NULL,
    turbine_name    TEXT,
    visit_start     TIMESTAMPTZ NOT NULL,
    visit_end       TIMESTAMPTZ NOT NULL,
    score           REAL NOT NULL,
    s_evidence      REAL NOT NULL,
    s_continuity    REAL NOT NULL,
    s_duration      REAL NOT NULL,
    min_distance_m  REAL NOT NULL,
    min_sog_kt      REAL,
    max_gap_min     REAL NOT NULL,
    duration_min    REAL NOT NULL,
    n_positions     INTEGER NOT NULL
);
"""

_CREATE_UPSERT_IDX = """
CREATE UNIQUE INDEX IF NOT EXISTS stage3_vessel_scores_upsert_idx
    ON stage3_vessel_scores (mms_id, turbine_code, visit_start);
"""

_UPSERT = """
INSERT INTO stage3_vessel_scores
    (mms_id, vessel_name, vessel_type, project_name, turbine_code, turbine_name,
     visit_start, visit_end, score, s_evidence, s_continuity, s_duration,
     min_distance_m, min_sog_kt, max_gap_min, duration_min, n_positions)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (mms_id, turbine_code, visit_start) DO UPDATE SET
    score          = EXCLUDED.score,
    s_evidence     = EXCLUDED.s_evidence,
    s_continuity   = EXCLUDED.s_continuity,
    s_duration     = EXCLUDED.s_duration,
    min_distance_m = EXCLUDED.min_distance_m,
    min_sog_kt     = EXCLUDED.min_sog_kt,
    max_gap_min    = EXCLUDED.max_gap_min,
    duration_min   = EXCLUDED.duration_min,
    n_positions    = EXCLUDED.n_positions,
    visit_end      = EXCLUDED.visit_end,
    vessel_name    = EXCLUDED.vessel_name
"""

_CSV_FIELDS = [
    "mms_id", "vessel_name", "vessel_type", "project_name", "turbine_code",
    "turbine_name", "visit_start", "visit_end", "score",
    "s_evidence", "s_continuity", "s_duration",
    "min_distance_m", "min_sog_kt", "max_gap_min", "duration_min", "n_positions",
]


# Geometry helpers

def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance in metres between two WGS-84 points."""
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def _bearing_deg(lat1, lon1, lat2, lon2) -> float:
    """Initial great-circle bearing (degrees, 0-360) from p1 toward p2."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlam = math.radians(lon2 - lon1)
    y = math.sin(dlam) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlam)
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


def _bearing_delta(a: float, b: float) -> float:
    """Absolute angular delta between two bearings (degrees, 0-180)."""
    d = abs(a - b) % 360.0
    return d if d <= 180.0 else 360.0 - d


# DB helpers

def load_turbines(conn, project_name=None):
    sql = ("SELECT turbine_code, turbine_name, project_name, latitude, longitude "
           "FROM wind_turbines")
    params = ()
    if project_name:
        sql += " WHERE project_name = %s"
        params = (project_name,)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [{"code": r[0], "name": r[1], "project": r[2],
                 "lat": float(r[3]), "lon": float(r[4])}
                for r in cur.fetchall()]


def load_stage1_hits(conn, year, mmsi=None, single_date=None, limit=None):
    conds  = ["EXTRACT(YEAR FROM vessel_date) = %s"]
    params = [year]
    if mmsi:
        conds.append("mms_id = %s")
        params.append(mmsi)
    if single_date:
        conds.append("vessel_date = %s")
        params.append(single_date)
    lim = f" LIMIT {limit}" if limit else ""
    sql = (f"SELECT DISTINCT mms_id, vessel_date FROM stage1_vessel_hits "
           f"WHERE {' AND '.join(conds)} ORDER BY vessel_date, mms_id{lim}")
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fetch_positions(conn, mms_id, vessel_date, bbox=None):
    """Fetch stage2_vessel_tracks for the vessel covering ±1 / +2 days.

    bbox: optional (lat_min, lat_max, lon_min, lon_max) to pre-filter by area.
    When --project is specified, pass the project turbine bbox to avoid loading
    positions from unrelated wind farms.
    """
    start = (datetime(vessel_date.year, vessel_date.month, vessel_date.day,
                      tzinfo=timezone.utc) - timedelta(days=1))
    end   = start + timedelta(days=3)
    if bbox:
        lat_min, lat_max, lon_min, lon_max = bbox
        with conn.cursor() as cur:
            cur.execute("""
                SELECT time_utc, latitude, longitude, speed_over_ground,
                       course_over_ground, vessel_name, vessel_type
                FROM stage2_vessel_tracks
                WHERE mms_id = %s AND time_utc >= %s AND time_utc < %s
                  AND latitude  BETWEEN %s AND %s
                  AND longitude BETWEEN %s AND %s
                  AND latitude IS NOT NULL AND longitude IS NOT NULL
                ORDER BY time_utc
            """, (mms_id, start, end, lat_min, lat_max, lon_min, lon_max))
            return [{"time": r[0], "lat": float(r[1]), "lon": float(r[2]),
                     "sog":  float(r[3]) if r[3] is not None else None,
                     "cog":  float(r[4]) if r[4] is not None else None,
                     "vessel_name": r[5], "vessel_type": r[6]}
                    for r in cur.fetchall()]
    with conn.cursor() as cur:
        cur.execute("""
            SELECT time_utc, latitude, longitude, speed_over_ground,
                   course_over_ground, vessel_name, vessel_type
            FROM stage2_vessel_tracks
            WHERE mms_id = %s AND time_utc >= %s AND time_utc < %s
              AND latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY time_utc
        """, (mms_id, start, end))
        return [{"time": r[0], "lat": float(r[1]), "lon": float(r[2]),
                 "sog":  float(r[3]) if r[3] is not None else None,
                 "cog":  float(r[4]) if r[4] is not None else None,
                 "vessel_name": r[5], "vessel_type": r[6]}
                for r in cur.fetchall()]


# Visit segmentation

def _segment(positions, gap_min):
    """Split time-sorted positions into segments on gaps > gap_min."""
    if not positions:
        return []
    segs = [[positions[0]]]
    for p in positions[1:]:
        if (p["time"] - segs[-1][-1]["time"]).total_seconds() / 60 > gap_min:
            segs.append([])
        segs[-1].append(p)
    return segs


# Scoring

def _score_segment(seg, turbine) -> dict | None:
    """
    Compute confidence score for one visit segment against one turbine.

    Returns event dict or None if vessel never within SCORE_PROX_MAX_M.
    """
    tlat, tlon = turbine["lat"], turbine["lon"]

    # Bounding-box pre-filter (fast rejection before haversine)
    dlat = SCORE_PROX_MAX_M / 111_000 * 1.05
    dlon = SCORE_PROX_MAX_M / (111_000 * math.cos(math.radians(tlat))) * 1.05
    if not any(abs(p["lat"] - tlat) <= dlat and abs(p["lon"] - tlon) <= dlon
               for p in seg):
        return None

    # Full distances for all positions in segment
    dists = [_haversine_m(p["lat"], p["lon"], tlat, tlon) for p in seg]
    min_dist = min(dists)

    if min_dist > SCORE_PROX_MAX_M:
        return None   # closest approach was ≥ SCORE_PROX_MAX_M

    # Working window: positions within SCORE_WINDOW_M — store with their distances
    work_with_dist = sorted(
        [(seg[i], dists[i]) for i, d in enumerate(dists) if d <= SCORE_WINDOW_M],
        key=lambda pd: pd[0]["time"],
    )
    work       = [pd[0] for pd in work_with_dist]
    work_dists = [pd[1] for pd in work_with_dist]

    import statistics

    if work:
        sogs     = [p["sog"] for p in work if p["sog"] is not None]
        min_sog  = min(sogs) if sogs else None
        times    = [p["time"] for p in work]
        duration = (times[-1] - times[0]).total_seconds() / 60
        gaps_min = [(work[j + 1]["time"] - work[j]["time"]).total_seconds() / 60
                    for j in range(len(work) - 1)]
        # Median gap is robust to a single overnight AIS dropout that would
        # otherwise destroy continuity for legitimately long SOV stations.
        median_gap = statistics.median(gaps_min) if gaps_min else 0.0
        max_gap    = max(gaps_min) if gaps_min else 0.0
        n_pos    = len(work)
        t_start, t_end = times[0], times[-1]
    else:
        min_sog  = None
        duration = 0.0
        median_gap = 0.0
        max_gap  = 0.0
        n_pos    = 0
        t_start  = seg[0]["time"]
        t_end    = seg[-1]["time"]

    # Per-point joint evidence: proximity + SOG + heading-toward-turbine.
    # Heading uses COG vs. bearing-to-turbine; falls back to neutral at low SOG.
    point_ev = []
    for p, d_p in zip(work, work_dists):
        s_prox = max(0.0, 1.0 - d_p / PROX_DENOM)
        s_sog  = max(0.0, 1.0 - p["sog"] / SOG_DENOM) if p["sog"] is not None else 0.0
        if (p["cog"] is not None and p["sog"] is not None
                and p["sog"] > HEADING_MIN_SOG_KT):
            tgt_bearing = _bearing_deg(p["lat"], p["lon"], tlat, tlon)
            delta = _bearing_delta(p["cog"], tgt_bearing)
            s_heading = max(0.0, 1.0 - delta / 180.0)
        else:
            s_heading = HEADING_NEUTRAL
        point_ev.append(EV_W_PROX * s_prox + EV_W_SOG * s_sog + EV_W_HEADING * s_heading)

    point_ev.sort(reverse=True)
    if point_ev:
        top_k = point_ev[:min(TOPK_FOR_PEAK, len(point_ev))]
        peak_evidence = statistics.median(top_k)
    else:
        peak_evidence = 0.0

    # Continuity: median gap (not max) — overnight AIS dropouts don't kill long stations
    s_cont = max(0.0, 1.0 - median_gap / GAP_DENOM)

    # Duration: log-scale instead of linear — compresses 2h-vs-4h while
    # preserving 10min-vs-30min discriminating power.
    s_dur = min(1.0, math.log1p(duration) / math.log1p(DUR_DENOM)) if duration > 0 else 0.0

    raw = (W_EVIDENCE * peak_evidence + W_CONTINUITY * s_cont + W_DURATION * s_dur) * 100
    score = round(min(100.0, max(0.0, raw)), 1)

    return {
        "visit_start":    t_start,
        "visit_end":      t_end,
        "score":          score,
        "s_evidence":     round(peak_evidence, 3),
        "s_continuity":   round(s_cont, 3),
        "s_duration":     round(s_dur, 3),
        "min_distance_m": round(min_dist, 1),
        "min_sog_kt":     round(min_sog, 2) if min_sog is not None else None,
        "max_gap_min":    round(max_gap, 1),
        "duration_min":   round(duration, 1),
        "n_positions":    n_pos,
    }


# Main

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stage 3 — continuous confidence scoring pipeline.")
    parser.add_argument("--year",        type=int, default=DEFAULT_YEAR)
    parser.add_argument("--project",     help="Limit to one wind farm project")
    parser.add_argument("--output",      default=str(DEFAULT_OUTPUT))
    parser.add_argument("--dry-run",     action="store_true")
    parser.add_argument("--mmsi",        metavar="ID", help="Single vessel MMSI")
    parser.add_argument("--single-date", metavar="YYYY-MM-DD")
    parser.add_argument("--limit",       type=int, metavar="N")
    args = parser.parse_args()

    conn     = psycopg2.connect(**DB_CONFIG)
    turbines = load_turbines(conn, args.project)
    if not turbines:
        print(f"No turbines found{' for ' + args.project if args.project else ''}.")
        conn.close()
        return 1

    project_names = sorted({t["project"] for t in turbines})
    print(f"Turbines : {len(turbines)} in {len(project_names)} project(s): "
          f"{', '.join(project_names)}")

    hits = load_stage1_hits(conn, args.year,
                            mmsi=args.mmsi,
                            single_date=args.single_date,
                            limit=args.limit)
    print(f"Stage 1 hits : {len(hits):,} (mms_id, date) pairs for {args.year}")

    # Spatial pre-filter bbox for fetch_positions — avoids loading positions from
    # unrelated wind farms when --project is specified.
    _pad_deg = (SEARCH_RADIUS_M + SCORE_PROX_MAX_M) / 111_000 + 0.05
    fetch_bbox = (
        min(t["lat"] for t in turbines) - _pad_deg,
        max(t["lat"] for t in turbines) + _pad_deg,
        min(t["lon"] for t in turbines) - _pad_deg,
        max(t["lon"] for t in turbines) + _pad_deg,
    ) if args.project else None

    if args.dry_run:
        conn.close()
        return 0

    # Create output table
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE)
        cur.execute(_CREATE_UPSERT_IDX)
    conn.commit()

    all_events: list[dict] = []
    seen: set[tuple] = set()   # (mms_id, turbine_code, visit_start) — dedup across hits

    for i, (mms_id, vessel_date) in enumerate(hits, 1):
        positions = fetch_positions(conn, mms_id, vessel_date, bbox=fetch_bbox)
        if not positions:
            continue

        vessel_name = next((p["vessel_name"] for p in positions if p["vessel_name"]), None)
        vessel_type = next((p["vessel_type"] for p in positions if p["vessel_type"]), None)

        # Bounding box of positions → limit turbine candidates
        lats = [p["lat"] for p in positions]
        lons = [p["lon"] for p in positions]
        pad  = SCORE_PROX_MAX_M / 111_000 + 0.001
        candidate_turbines = [
            t for t in turbines
            if (min(lats) - pad) <= t["lat"] <= (max(lats) + pad)
            and (min(lons) - pad) <= t["lon"] <= (max(lons) + pad)
        ]
        if not candidate_turbines:
            continue

        # Segment all positions by time gap once (shared across turbines)
        segs = _segment(positions, VISIT_GAP_MIN)

        for t in candidate_turbines:
            for seg in segs:
                result = _score_segment(seg, t)
                if result is None:
                    continue

                key = (mms_id, t["code"], result["visit_start"])
                if key in seen:
                    continue
                seen.add(key)

                all_events.append({
                    "mms_id":       mms_id,
                    "vessel_name":  vessel_name,
                    "vessel_type":  vessel_type,
                    "project_name": t["project"],
                    "turbine_code": t["code"],
                    "turbine_name": t["name"],
                    **result,
                })

        if i % 500 == 0:
            pct = 100 * i / len(hits)
            print(f"  [{i:>5}/{len(hits)}] {pct:.0f}%  events so far: {len(all_events):,}",
                  flush=True)

    conn.close()

    # Summary

    total = len(all_events)
    print(f"\nTotal scored events : {total:,}")
    if not total:
        print("No events generated — check thresholds or data coverage.")
        return 0

    scores = [e["score"] for e in all_events]
    high   = sum(1 for s in scores if s >= SCORE_HIGH)
    mid    = sum(1 for s in scores if SCORE_MED <= s < SCORE_HIGH)
    low    = sum(1 for s in scores if s < SCORE_MED)
    print(f"  score ≥ {SCORE_HIGH}  (high, ~Tier 1): {high:>6,}  ({100*high//total}%)")
    print(f"  score {SCORE_MED}–{SCORE_HIGH-1} (moderate, ~T2): {mid:>6,}  ({100*mid//total}%)")
    print(f"  score < {SCORE_MED}  (low proximity): {low:>6,}  ({100*low//total}%)")

    # Per-project breakdown
    by_proj: dict[str, list] = defaultdict(list)
    for e in all_events:
        by_proj[e["project_name"]].append(e["score"])
    print(f"\nPer-project (≥{SCORE_HIGH} / {SCORE_MED}–{SCORE_HIGH-1} / <{SCORE_MED}):")
    for proj in sorted(by_proj):
        ss = by_proj[proj]
        h = sum(1 for s in ss if s >= SCORE_HIGH)
        m = sum(1 for s in ss if SCORE_MED <= s < SCORE_HIGH)
        l = sum(1 for s in ss if s < SCORE_MED)
        print(f"  {proj:<20} {h:>5} / {m:>5} / {l:>5}  (total {len(ss):,})")

    # CSV output

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sorted_events = sorted(all_events,
                           key=lambda e: (e["project_name"], e["turbine_code"],
                                          e["visit_start"]))
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(sorted_events)
    print(f"\nCSV saved → {out_path}  ({out_path.stat().st_size / 1e3:.0f} KB)")

    # DB upsert

    conn2 = psycopg2.connect(**DB_CONFIG)
    with conn2.cursor() as cur:
        psycopg2.extras.execute_batch(cur, _UPSERT, [
            (e["mms_id"], e["vessel_name"], e["vessel_type"],
             e["project_name"], e["turbine_code"], e["turbine_name"],
             e["visit_start"], e["visit_end"],
             e["score"], e["s_evidence"],
             e["s_continuity"], e["s_duration"],
             e["min_distance_m"], e["min_sog_kt"],
             e["max_gap_min"], e["duration_min"], e["n_positions"])
            for e in all_events
        ], page_size=500)
    conn2.commit()
    conn2.close()
    print(f"DB table stage3_vessel_scores: {total:,} rows upserted.")
    return 0


if __name__ == "__main__":
    exit(main())
