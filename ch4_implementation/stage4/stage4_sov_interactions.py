#!/usr/bin/env python3
# Stage 4 — detect cross-modal interactions with SOVs (Service Operation Vessels).
#
# Detects two interaction types:
#   1. Helicopter hoists — helicopters hovering near an SOV for crew/cargo transfer
#   2. CTV dockings — crew transfer vessels coming alongside an SOV for walk-to-work
#
# Uses SOV station periods from stage3_vessel_events (operation_type='support_station')
# as anchor points, then searches helicopter and CTV tracks for nearby activity.
#
# Strategy:
#   1. Load SOV station events from stage3_vessel_events.
#   2. For each SOV station, compute centroid position from stage2_vessel_tracks.
#   3. Query stage2_helicopter_tracks within 2 km of the SOV during its station period.
#   4. Query stage2_vessel_tracks for CTVs within 500 m of the SOV.
#   5. Segment nearby positions into visits, score each, classify as hoist/docking.
#
# Usage:
#   python stage4_sov_interactions.py --sov-only          # list SOV stations
#   python stage4_sov_interactions.py --year 2024         # full detection
#   python stage4_sov_interactions.py --sov-name "SEA INSTALLER" --year 2024

import argparse
import csv
import math
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from itertools import groupby
from pathlib import Path

import psycopg2
import psycopg2.extras

from pipeline_common import (
    DB_CONFIG, conn,
    haversine_m,
    ensure_table_and_clear,
)

DEFAULT_OUTPUT = Path("/mnt/d/thesis/presentation/stage4_sov_interactions.csv")

# Detection parameters

# Helicopter-to-SOV
HELI_SEARCH_RADIUS_M = 2000   # spatial search envelope around SOV centroid
HELI_MAX_ALT_M       = 300    # lower ceiling than turbine visits (no nacelle)
HELI_SPEED_DENOM_MS  = 20.0   # hover speed threshold (stricter than turbine 30 m/s)
HELI_ALT_DENOM_M     = 300.0  # altitude denominator
HELI_PROX_DENOM_M    = 1000.0 # proximity denominator
HELI_DUR_DENOM       = 30.0   # hoists saturate at 30 min (shorter than turbine 60)
HELI_MIN_POSITIONS   = 3
HELI_MIN_DURATION    = 1.0    # minutes — hoists can be quick
HELI_VISIT_GAP_MIN   = 30
MISSING_SPEED_DEFAULT = 0.7

# CTV-to-SOV
CTV_SEARCH_RADIUS_M  = 500    # CTVs dock alongside, smaller envelope
CTV_DOCK_RADIUS_M    = 100    # "alongside" threshold
CTV_DOCK_SOG_KT      = 0.5    # max SOG while docked
CTV_SOG_DENOM_KT     = 2.0    # SOG denominator for scoring
CTV_PROX_DENOM_M     = 500.0
CTV_DUR_DENOM        = 30.0   # dwell saturates at 30 min
CTV_CONT_DENOM       = 15.0   # continuity denominator
CTV_MIN_POSITIONS    = 3
CTV_MIN_DURATION     = 3.0    # minutes
CTV_VISIT_GAP_MIN    = 30

# Per-point weights (helicopter, 3-way co-location)
W_PROX     = 0.45
W_ALT      = 0.30
W_SPEED    = 0.25

# Final score weights
W_EVIDENCE = 0.80
W_DURATION = 0.20

# CTV score weights
CTV_W_PROX  = 0.35
CTV_W_SOG   = 0.30
CTV_W_DWELL = 0.20
CTV_W_CONT  = 0.15

# Classification threshold
SCORE_THRESHOLD = 40

# DDL

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS stage4_sov_interactions (
    sov_mmsi          TEXT             NOT NULL,
    sov_name          TEXT,
    sov_lat           DOUBLE PRECISION,
    sov_lon           DOUBLE PRECISION,
    sov_station_start TIMESTAMPTZ      NOT NULL,
    sov_station_end   TIMESTAMPTZ      NOT NULL,
    asset_type        TEXT             NOT NULL,
    asset_id          TEXT             NOT NULL,
    asset_name        TEXT,
    interaction_type  TEXT             NOT NULL,
    interaction_start TIMESTAMPTZ      NOT NULL,
    interaction_end   TIMESTAMPTZ      NOT NULL,
    duration_minutes  REAL             NOT NULL,
    min_distance_m    REAL,
    min_alt_m         REAL,
    min_sog_kt        REAL,
    score             REAL,
    project_name      TEXT,
    PRIMARY KEY (asset_type, asset_id, interaction_start, sov_mmsi)
);
"""

_UPSERT = """
INSERT INTO stage4_sov_interactions
    (sov_mmsi, sov_name, sov_lat, sov_lon,
     sov_station_start, sov_station_end,
     asset_type, asset_id, asset_name,
     interaction_type, interaction_start, interaction_end,
     duration_minutes, min_distance_m, min_alt_m, min_sog_kt,
     score, project_name)
VALUES
    (%(sov_mmsi)s, %(sov_name)s, %(sov_lat)s, %(sov_lon)s,
     %(sov_station_start)s, %(sov_station_end)s,
     %(asset_type)s, %(asset_id)s, %(asset_name)s,
     %(interaction_type)s, %(interaction_start)s, %(interaction_end)s,
     %(duration_minutes)s, %(min_distance_m)s, %(min_alt_m)s, %(min_sog_kt)s,
     %(score)s, %(project_name)s)
ON CONFLICT (asset_type, asset_id, interaction_start, sov_mmsi) DO NOTHING;
"""

OUTPUT_FIELDS = [
    "sov_mmsi", "sov_name", "sov_lat", "sov_lon",
    "sov_station_start", "sov_station_end",
    "asset_type", "asset_id", "asset_name",
    "interaction_type", "interaction_start", "interaction_end",
    "duration_minutes", "min_distance_m", "min_alt_m", "min_sog_kt",
    "score", "project_name",
]


# Haversine
# `haversine_m` imported from pipeline_common


# SOV station loading

def load_sov_stations(conn, year: int, sov_name: str | None = None,
                      project: str | None = None) -> list[dict]:
    """Load SOV station periods from stage3_vessel_events."""
    query = """
        SELECT mms_id, vessel_name, project_name,
               visit_start, visit_end, duration_minutes
        FROM stage3_vessel_events
        WHERE operation_type = 'support_station'
          AND tier IN (1, 2)
          AND visit_start >= %(start)s
          AND visit_start <  %(end)s
    """
    params = {
        "start": f"{year}-01-01",
        "end":   f"{year + 1}-01-01",
    }
    if sov_name:
        query += " AND vessel_name ILIKE %(sov_name)s"
        params["sov_name"] = f"%{sov_name}%"
    if project:
        query += " AND project_name = %(project)s"
        params["project"] = project

    query += " ORDER BY visit_start"

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query, params)
        return [dict(r) for r in cur.fetchall()]


def compute_sov_centroid(conn, mmsi: str, start, end) -> tuple[float, float] | None:
    """Compute centroid position of SOV during station period (DP-held positions only)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT AVG(latitude), AVG(longitude)
            FROM stage2_vessel_tracks
            WHERE mms_id = %s
              AND time_utc BETWEEN %s AND %s
              AND speed_over_ground <= 0.5
        """, (mmsi, start, end))
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0]), float(row[1])
    return None


# Helicopter interaction detection

def find_helicopter_positions(conn, sov_lat: float, sov_lon: float,
                              start, end) -> list[dict]:
    """Query helicopter track positions near SOV during station period.

    Chunks long periods into weekly queries for efficient partition access.
    """
    dlat = HELI_SEARCH_RADIUS_M / 111_000
    dlon = HELI_SEARCH_RADIUS_M / (111_000 * math.cos(math.radians(sov_lat)))

    CHUNK_DAYS = 7
    positions = []
    chunk_start = start.date() - timedelta(days=1)
    chunk_end_limit = end.date() + timedelta(days=1)

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        while chunk_start <= chunk_end_limit:
            chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), chunk_end_limit)
            cur.execute("""
                SELECT icao24, time_utc, lat, lon, baro_alt_m, velocity_ms
                FROM stage2_helicopter_tracks
                WHERE flight_date BETWEEN %s AND %s
                  AND lat BETWEEN %s AND %s
                  AND lon BETWEEN %s AND %s
                  AND baro_alt_m IS NOT NULL
                  AND baro_alt_m <= %s
                ORDER BY icao24, time_utc
            """, (
                chunk_start, chunk_end,
                sov_lat - dlat, sov_lat + dlat,
                sov_lon - dlon, sov_lon + dlon,
                HELI_MAX_ALT_M,
            ))
            for r in cur.fetchall():
                d = dict(r)
                d["distance_m"] = haversine_m(d["lat"], d["lon"], sov_lat, sov_lon)
                if d["distance_m"] <= HELI_SEARCH_RADIUS_M:
                    if start <= d["time_utc"] <= end:
                        positions.append(d)
            chunk_start = chunk_end + timedelta(days=1)

    positions.sort(key=lambda p: (p["icao24"], p["time_utc"]))
    return positions


def segment_visits(positions: list[dict], id_key: str,
                   gap_minutes: int) -> list[list[dict]]:
    """Segment positions into visits by (id_key) with time-gap splitting."""
    if not positions:
        return []
    visits = []
    gap = timedelta(minutes=gap_minutes)

    for _id, group in groupby(positions, key=lambda p: p[id_key]):
        buf = []
        for p in group:
            if buf and p["time_utc"] - buf[-1]["time_utc"] > gap:
                visits.append(buf)
                buf = []
            buf.append(p)
        if buf:
            visits.append(buf)

    return visits


def classify_heli_interaction(positions: list[dict]) -> tuple[str, float]:
    """Score a helicopter visit near SOV using 3-way per-point co-location.

    Returns (interaction_type, score).
    """
    if len(positions) < HELI_MIN_POSITIONS:
        return "flyby", 0.0

    first, last = positions[0], positions[-1]
    duration_min = (last["time_utc"] - first["time_utc"]).total_seconds() / 60
    if duration_min < HELI_MIN_DURATION:
        return "flyby", 0.0

    point_ev = []
    for p in positions:
        if p["distance_m"] is None or p["baro_alt_m"] is None:
            continue
        s_prox = min(1.0, max(0.0, 1.0 - p["distance_m"] / HELI_PROX_DENOM_M))
        s_alt  = min(1.0, max(0.0, 1.0 - p["baro_alt_m"] / HELI_ALT_DENOM_M))
        if p["velocity_ms"] is not None:
            s_spd = min(1.0, max(0.0, 1.0 - p["velocity_ms"] / HELI_SPEED_DENOM_MS))
        else:
            s_spd = MISSING_SPEED_DEFAULT
        point_ev.append(W_PROX * s_prox + W_ALT * s_alt + W_SPEED * s_spd)

    if not point_ev:
        return "flyby", 0.0

    point_ev.sort(reverse=True)
    peak_evidence = sum(point_ev[:3]) / min(3, len(point_ev))

    s_dur = min(1.0, duration_min / HELI_DUR_DENOM)

    score = round((W_EVIDENCE * peak_evidence + W_DURATION * s_dur) * 100, 1)

    itype = "hoist" if score >= SCORE_THRESHOLD else "flyby"
    return itype, score


# CTV interaction detection

def find_ctv_positions(conn, sov_mmsi: str, sov_lat: float, sov_lon: float,
                       start, end) -> list[dict]:
    """Query CTV track positions near SOV during station period.

    Chunks long periods into weekly queries to avoid scanning too many
    daily partitions in stage2_vessel_tracks at once.
    """
    dlat = CTV_SEARCH_RADIUS_M / 111_000
    dlon = CTV_SEARCH_RADIUS_M / (111_000 * math.cos(math.radians(sov_lat)))

    CHUNK_DAYS = 7
    positions = []
    chunk_start = start.date() - timedelta(days=1)
    chunk_end_limit = end.date() + timedelta(days=1)

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        while chunk_start <= chunk_end_limit:
            chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), chunk_end_limit)
            cur.execute("""
                SELECT mms_id, time_utc, latitude AS lat, longitude AS lon,
                       speed_over_ground AS sog_kt, vessel_name, vessel_type
                FROM stage2_vessel_tracks
                WHERE vessel_date BETWEEN %s AND %s
                  AND mms_id != %s
                  AND latitude BETWEEN %s AND %s
                  AND longitude BETWEEN %s AND %s
                ORDER BY mms_id, time_utc
            """, (
                chunk_start, chunk_end,
                sov_mmsi,
                sov_lat - dlat, sov_lat + dlat,
                sov_lon - dlon, sov_lon + dlon,
            ))
            for r in cur.fetchall():
                d = dict(r)
                d["distance_m"] = haversine_m(d["lat"], d["lon"], sov_lat, sov_lon)
                if d["distance_m"] <= CTV_SEARCH_RADIUS_M:
                    if start <= d["time_utc"] <= end:
                        positions.append(d)
            chunk_start = chunk_end + timedelta(days=1)

    # Re-sort by mms_id, time_utc since chunks may interleave
    positions.sort(key=lambda p: (p["mms_id"], p["time_utc"]))
    return positions


def classify_ctv_interaction(positions: list[dict]) -> tuple[str, float]:
    """Score a CTV visit near SOV.

    Returns (interaction_type, score).
    """
    if len(positions) < CTV_MIN_POSITIONS:
        return "passing", 0.0

    first, last = positions[0], positions[-1]
    duration_min = (last["time_utc"] - first["time_utc"]).total_seconds() / 60
    if duration_min < CTV_MIN_DURATION:
        return "passing", 0.0

    min_dist = min(p["distance_m"] for p in positions)
    sogs = [p["sog_kt"] for p in positions if p["sog_kt"] is not None]
    min_sog = min(sogs) if sogs else None

    # Dwell time: cumulative time within dock radius at low SOG
    dwell_min = 0.0
    for i in range(1, len(positions)):
        p = positions[i]
        pp = positions[i - 1]
        if (p["distance_m"] <= CTV_DOCK_RADIUS_M and
                p["sog_kt"] is not None and p["sog_kt"] <= CTV_DOCK_SOG_KT):
            dt = (p["time_utc"] - pp["time_utc"]).total_seconds() / 60
            if dt <= CTV_VISIT_GAP_MIN:
                dwell_min += dt

    # Max gap
    max_gap = 0.0
    for i in range(1, len(positions)):
        gap = (positions[i]["time_utc"] - positions[i - 1]["time_utc"]).total_seconds() / 60
        if gap > max_gap:
            max_gap = gap

    # Score components
    s_prox  = min(1.0, max(0.0, 1.0 - min_dist / CTV_PROX_DENOM_M))
    s_sog   = min(1.0, max(0.0, 1.0 - (min_sog / CTV_SOG_DENOM_KT))) if min_sog is not None else 0.5
    s_dwell = min(1.0, dwell_min / CTV_DUR_DENOM)
    s_cont  = min(1.0, max(0.0, 1.0 - max_gap / CTV_CONT_DENOM))

    score = round(
        (CTV_W_PROX * s_prox + CTV_W_SOG * s_sog +
         CTV_W_DWELL * s_dwell + CTV_W_CONT * s_cont) * 100, 1)

    itype = "docking" if score >= SCORE_THRESHOLD else "passing"
    return itype, score


# Main

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stage 4 — detect helicopter hoists and CTV dockings at SOVs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--year", type=int, default=2024, help="Calendar year (default: 2024)")
    parser.add_argument("--project", metavar="NAME", help="Filter by project")
    parser.add_argument("--sov-name", metavar="NAME", help="Filter to a specific SOV by name")
    parser.add_argument("--output", "-o", type=Path, default=DEFAULT_OUTPUT, help="Output CSV")
    parser.add_argument("--sov-only", action="store_true", help="List SOV stations and exit")
    parser.add_argument("--no-db", action="store_true", help="Skip writing to database")
    parser.add_argument("--heli-only", action="store_true", help="Only detect helicopter interactions")
    parser.add_argument("--ctv-only", action="store_true", help="Only detect CTV interactions")
    args = parser.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)

    print("Loading SOV stations...")
    stations = load_sov_stations(conn, args.year, args.sov_name, args.project)
    print(f"  {len(stations)} SOV station periods found")

    if not stations:
        print("No SOV stations found. Check stage3_vessel_events.")
        conn.close()
        return 1

    if args.sov_only:
        # Summarize by vessel
        by_vessel = defaultdict(lambda: {"count": 0, "days": 0.0, "projects": set()})
        for s in stations:
            v = by_vessel[s["vessel_name"]]
            v["count"] += 1
            v["days"] += s["duration_minutes"] / 60 / 24
            v["projects"].add(s["project_name"])

        print(f"\n{'SOV Station Summary':-^60}")
        print(f"  {'Vessel':<25} {'Stations':>8} {'Days':>8}  Projects")
        for name, info in sorted(by_vessel.items(), key=lambda x: -x[1]["days"]):
            projects = ", ".join(sorted(info["projects"]))
            print(f"  {name:<25} {info['count']:>8} {info['days']:>8.0f}  {projects}")
        conn.close()
        return 0

    if not args.no_db:
        year_start = date(args.year, 1, 1)
        year_end   = date(args.year + 1, 1, 1)
        deleted = ensure_table_and_clear(
            conn, "stage4_sov_interactions", _CREATE_TABLE,
            date_col="interaction_start", d_start=year_start, d_end=year_end,
        )
        print(f"  Cleared {deleted:,} stale interactions in "
              f"[{year_start}, {year_end}); table preserved.")

    all_interactions = []
    total_heli = 0
    total_ctv  = 0

    # Merge consecutive station periods for same vessel to avoid redundant queries
    # (SOVs often have multiple tier-1/2 events spanning one continuous stay)
    merged_stations = _merge_sov_stations(stations)
    print(f"  Merged into {len(merged_stations)} unique station periods")
    print()

    for i, sov in enumerate(merged_stations, 1):
        mmsi = sov["mms_id"]
        name = sov["vessel_name"]
        start = sov["visit_start"]
        end   = sov["visit_end"]
        dur_days = (end - start).total_seconds() / 86400

        print(f"  [{i:>3}/{len(merged_stations)}] {name:<22} "
              f"{start.strftime('%Y-%m-%d')}→{end.strftime('%Y-%m-%d')} "
              f"({dur_days:.0f}d)", end=" ", flush=True)

        # Compute centroid
        centroid = compute_sov_centroid(conn, mmsi, start, end)
        if centroid is None:
            print("— no centroid (skipped)")
            continue
        sov_lat, sov_lon = centroid

        n_heli = 0
        n_ctv  = 0

        # Helicopter interactions
        if not args.ctv_only:
            heli_pos = find_helicopter_positions(conn, sov_lat, sov_lon, start, end)
            if heli_pos:
                visits = segment_visits(heli_pos, "icao24", HELI_VISIT_GAP_MIN)
                for visit in visits:
                    itype, score = classify_heli_interaction(visit)
                    if score < 10:  # skip trivial flybys
                        continue
                    first, last = visit[0], visit[-1]
                    alts = [p["baro_alt_m"] for p in visit if p["baro_alt_m"] is not None]
                    interaction = {
                        "sov_mmsi": mmsi,
                        "sov_name": name,
                        "sov_lat": round(sov_lat, 6),
                        "sov_lon": round(sov_lon, 6),
                        "sov_station_start": start,
                        "sov_station_end": end,
                        "asset_type": "helicopter",
                        "asset_id": first["icao24"],
                        "asset_name": None,
                        "interaction_type": itype,
                        "interaction_start": first["time_utc"],
                        "interaction_end": last["time_utc"],
                        "duration_minutes": round(
                            (last["time_utc"] - first["time_utc"]).total_seconds() / 60, 1),
                        "min_distance_m": round(min(p["distance_m"] for p in visit), 1),
                        "min_alt_m": round(min(alts), 1) if alts else None,
                        "min_sog_kt": None,
                        "score": score,
                        "project_name": sov.get("project_name"),
                    }
                    all_interactions.append(interaction)
                    n_heli += 1

        # CTV interactions
        if not args.heli_only:
            ctv_pos = find_ctv_positions(conn, mmsi, sov_lat, sov_lon, start, end)
            if ctv_pos:
                visits = segment_visits(ctv_pos, "mms_id", CTV_VISIT_GAP_MIN)
                for visit in visits:
                    itype, score = classify_ctv_interaction(visit)
                    if score < 10:
                        continue
                    first, last = visit[0], visit[-1]
                    sogs = [p["sog_kt"] for p in visit if p["sog_kt"] is not None]
                    interaction = {
                        "sov_mmsi": mmsi,
                        "sov_name": name,
                        "sov_lat": round(sov_lat, 6),
                        "sov_lon": round(sov_lon, 6),
                        "sov_station_start": start,
                        "sov_station_end": end,
                        "asset_type": "ctv",
                        "asset_id": first["mms_id"],
                        "asset_name": first.get("vessel_name"),
                        "interaction_type": itype,
                        "interaction_start": first["time_utc"],
                        "interaction_end": last["time_utc"],
                        "duration_minutes": round(
                            (last["time_utc"] - first["time_utc"]).total_seconds() / 60, 1),
                        "min_distance_m": round(min(p["distance_m"] for p in visit), 1),
                        "min_alt_m": None,
                        "min_sog_kt": round(min(sogs), 2) if sogs else None,
                        "score": score,
                        "project_name": sov.get("project_name"),
                    }
                    all_interactions.append(interaction)
                    n_ctv += 1

        total_heli += n_heli
        total_ctv  += n_ctv
        parts = []
        if n_heli:
            parts.append(f"{n_heli} heli")
        if n_ctv:
            parts.append(f"{n_ctv} ctv")
        print(f"→ {', '.join(parts) if parts else 'no interactions'}")

    # Save to database
    if not args.no_db and all_interactions:
        with conn.cursor() as cur:
            for e in all_interactions:
                cur.execute(_UPSERT, {k: e[k] for k in OUTPUT_FIELDS})
        conn.commit()

    conn.close()

    # Write CSV
    if all_interactions:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
            writer.writeheader()
            for e in all_interactions:
                writer.writerow({k: e[k] for k in OUTPUT_FIELDS})
        print(f"\nSaved: {args.output}")

    # Summary
    heli_hoists  = sum(1 for e in all_interactions
                       if e["asset_type"] == "helicopter" and e["interaction_type"] == "hoist")
    heli_flybys  = sum(1 for e in all_interactions
                       if e["asset_type"] == "helicopter" and e["interaction_type"] == "flyby")
    ctv_dockings = sum(1 for e in all_interactions
                       if e["asset_type"] == "ctv" and e["interaction_type"] == "docking")
    ctv_passing  = sum(1 for e in all_interactions
                       if e["asset_type"] == "ctv" and e["interaction_type"] == "passing")

    print(f"\nDone. {len(all_interactions)} total interactions")
    print(f"  Helicopter: {heli_hoists} hoists, {heli_flybys} flybys")
    print(f"  CTV:        {ctv_dockings} dockings, {ctv_passing} passing")

    # Breakdown by SOV
    by_sov = defaultdict(lambda: {"hoist": 0, "flyby": 0, "docking": 0, "passing": 0})
    for e in all_interactions:
        by_sov[e["sov_name"]][e["interaction_type"]] += 1

    print(f"\n{'Interactions by SOV':-^60}")
    print(f"  {'Vessel':<22} {'Hoists':>6} {'Flybys':>7} {'Docks':>6} {'Pass':>6}")
    for name, counts in sorted(by_sov.items(), key=lambda x: -(x[1]["hoist"] + x[1]["docking"])):
        print(f"  {name:<22} {counts['hoist']:>6} {counts['flyby']:>7} "
              f"{counts['docking']:>6} {counts['passing']:>6}")

    return 0


def _merge_sov_stations(stations: list[dict]) -> list[dict]:
    """Merge overlapping/adjacent station periods for the same vessel.

    SOVs on multi-day DP holds often produce multiple stage3 events
    (from day-by-day processing). Merge them to avoid redundant queries.
    """
    if not stations:
        return []

    by_vessel = defaultdict(list)
    for s in stations:
        by_vessel[s["mms_id"]].append(s)

    merged = []
    for mmsi, events in by_vessel.items():
        events.sort(key=lambda e: e["visit_start"])
        current = dict(events[0])
        for e in events[1:]:
            # Merge if overlap or gap <= 6 hours
            if e["visit_start"] <= current["visit_end"] + timedelta(hours=6):
                current["visit_end"] = max(current["visit_end"], e["visit_end"])
                current["duration_minutes"] = (
                    (current["visit_end"] - current["visit_start"]).total_seconds() / 60
                )
            else:
                merged.append(current)
                current = dict(e)
        merged.append(current)

    merged.sort(key=lambda s: s["visit_start"])
    return merged


if __name__ == "__main__":
    raise SystemExit(main())
