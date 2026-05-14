#!/usr/bin/env python3
"""
Recompute vessel transit times using full-track port call detection.

Instead of a 48h window, scans ALL slow positions per vessel to build a
complete port call timeline, then assigns departure/return ports to each
event based on the nearest port call in time.

Usage:
    python scripts/recompute_vessel_transit.py [--dry-run]
"""

import argparse
import time
from collections import defaultdict
from datetime import timedelta

import numpy as np
import psycopg2

DB = dict(host="localhost", port=5432, dbname="windfarm",
          user="thesis", password="thesis2026")

PORT_RADIUS_DEG = 0.018    # ~2 km bbox match (matches existing pipeline)
PORT_SOG_KT = 1            # slow speed threshold (matches existing: PORT_SOG_KT = 1.0)
PORT_CALL_GAP_H = 2        # hours gap to split port calls
MIN_PORT_DWELL_MIN = 5     # minimum dwell to count as a real port call
MAX_TRANSIT_MIN = 300       # 5-hour sanity cap


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km."""
    import math
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2)**2
    return R * 2 * math.asin(math.sqrt(a))


def load_ports(conn):
    """Load NE US ports into numpy arrays."""
    cur = conn.cursor()
    cur.execute("""
        SELECT name, latitude, longitude FROM osm_ports
        WHERE latitude BETWEEN 38 AND 44 AND longitude BETWEEN -75 AND -69
        AND name IS NOT NULL
    """)
    ports = cur.fetchall()
    return (
        [p[0] for p in ports],
        np.array([p[1] for p in ports], dtype=np.float64),
        np.array([p[2] for p in ports], dtype=np.float64),
    )


def find_port_calls(times, lats, lons, port_names, port_lats, port_lons):
    """Find all port calls from a vessel's slow positions.

    Returns list of dicts: {name, start_time, end_time, lat, lon}
    """
    n = len(times)
    if n == 0:
        return []

    # Vectorized: check each port against all positions
    near_port_mask = np.zeros(n, dtype=bool)
    near_port_idx = np.full(n, -1, dtype=int)

    for i, (plat, plon) in enumerate(zip(port_lats, port_lons)):
        mask = (np.abs(lats - plat) < PORT_RADIUS_DEG) & \
               (np.abs(lons - plon) < PORT_RADIUS_DEG)
        new_matches = mask & ~near_port_mask
        near_port_mask |= mask
        near_port_idx[new_matches] = i

    # Extract near-port positions
    indices = np.where(near_port_mask)[0]
    if len(indices) == 0:
        return []

    # Cluster into port calls by time gaps
    port_calls = []
    cluster_start = 0
    for j in range(1, len(indices)):
        gap_h = (times[indices[j]] - times[indices[j - 1]]).total_seconds() / 3600
        if gap_h > PORT_CALL_GAP_H:
            # Close current cluster
            _add_call(port_calls, indices, cluster_start, j, times, lats, lons,
                      near_port_idx, port_names)
            cluster_start = j
    # Last cluster
    _add_call(port_calls, indices, cluster_start, len(indices), times, lats, lons,
              near_port_idx, port_names)

    return port_calls


def _add_call(port_calls, indices, start, end, times, lats, lons,
              near_port_idx, port_names):
    """Add a port call if dwell exceeds minimum."""
    idx_slice = indices[start:end]
    t_start = times[idx_slice[0]]
    t_end = times[idx_slice[-1]]
    dwell_min = (t_end - t_start).total_seconds() / 60

    if dwell_min < MIN_PORT_DWELL_MIN:
        return

    # Most common port in this cluster
    port_ids = near_port_idx[idx_slice]
    unique, counts = np.unique(port_ids[port_ids >= 0], return_counts=True)
    if len(unique) == 0:
        return
    best_port_idx = unique[counts.argmax()]

    port_calls.append({
        "name": port_names[best_port_idx],
        "start_time": t_start,
        "end_time": t_end,
        "lat": float(lats[idx_slice[-1]]),
        "lon": float(lons[idx_slice[-1]]),
    })


def assign_transit(events, port_calls, turbine_coords):
    """Assign departure/return ports to events from port call timeline.

    For each event:
    - Departure: latest port call ending before event start
    - Return: earliest port call starting after event end
    """
    # Sort port calls by end time
    calls_sorted = sorted(port_calls, key=lambda c: c["end_time"])

    results = {}
    for ev in events:
        ev_id = ev["id"]
        ev_start = ev["visit_start"]
        ev_end = ev["visit_end"]

        # Find departure: latest port call ending before visit start
        dep = None
        for c in reversed(calls_sorted):
            if c["end_time"] < ev_start:
                dep = c
                break

        # Find return: earliest port call starting after visit end
        ret = None
        for c in calls_sorted:
            if c["start_time"] > ev_end:
                ret = c
                break

        result = {"id": ev_id}

        if dep:
            transit_min = (ev_start - dep["end_time"]).total_seconds() / 60
            if 0 < transit_min <= MAX_TRANSIT_MIN:
                result["departure_port"] = dep["name"]
                result["departure_time"] = dep["end_time"]
                result["transit_out_min"] = round(transit_min, 1)
                # Distance
                t_coords = turbine_coords.get(ev["turbine_code"])
                if t_coords:
                    result["transit_dist_km"] = round(
                        haversine_km(dep["lat"], dep["lon"],
                                     t_coords[0], t_coords[1]), 1)

        if ret:
            transit_min = (ret["start_time"] - ev_end).total_seconds() / 60
            if 0 < transit_min <= MAX_TRANSIT_MIN:
                result["return_port"] = ret["name"]
                result["return_time"] = ret["start_time"]
                result["transit_back_min"] = round(transit_min, 1)

        results[ev_id] = result

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Print results without updating DB")
    args = parser.parse_args()

    conn = psycopg2.connect(**DB)
    cur = conn.cursor()

    # Load ports
    port_names, port_lats, port_lons = load_ports(conn)
    print(f"Loaded {len(port_names)} ports")

    # Load turbine coordinates
    cur.execute("SELECT turbine_code, latitude, longitude FROM wind_turbines")
    turbine_coords = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

    # Get all vessels with tier 1/2 maintenance events
    cur.execute("""
        SELECT DISTINCT mms_id FROM stage3_vessel_events
        WHERE operation_type IS NOT NULL AND tier IN (1, 2)
    """)
    vessel_ids = [r[0] for r in cur.fetchall()]
    print(f"Processing {len(vessel_ids)} vessels")

    # Get all events grouped by vessel
    cur.execute("""
        SELECT id, mms_id, visit_start, visit_end, turbine_code
        FROM stage3_vessel_events
        WHERE operation_type IS NOT NULL AND tier IN (1, 2)
        ORDER BY mms_id, visit_start
    """)
    events_by_vessel = defaultdict(list)
    for row in cur.fetchall():
        events_by_vessel[row[1]].append({
            "id": row[0], "visit_start": row[2],
            "visit_end": row[3], "turbine_code": row[4],
        })

    # Process each vessel
    t0 = time.time()
    total_events = 0
    total_assigned_dep = 0
    total_assigned_ret = 0
    all_results = {}

    for i, mmsi in enumerate(vessel_ids):
        # Fetch slow positions from FULL AIS table (includes port positions)
        cur.execute("""
            SELECT time, latitude, longitude
            FROM vessel_data_ais
            WHERE mms_id = %s AND speed_over_ground <= %s
              AND time >= '2024-01-01' AND time < '2025-01-01'
              AND latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY time
        """, (mmsi, PORT_SOG_KT))
        rows = cur.fetchall()

        if not rows:
            continue

        times = [r[0] for r in rows]
        lats = np.array([r[1] for r in rows], dtype=np.float64)
        lons = np.array([r[2] for r in rows], dtype=np.float64)

        # Find port calls
        port_calls = find_port_calls(times, lats, lons,
                                     port_names, port_lats, port_lons)

        # Assign transit to events
        events = events_by_vessel.get(mmsi, [])
        if events and port_calls:
            results = assign_transit(events, port_calls, turbine_coords)
            all_results.update(results)

            for r in results.values():
                if "departure_port" in r:
                    total_assigned_dep += 1
                if "return_port" in r:
                    total_assigned_ret += 1

        total_events += len(events)

        if (i + 1) % 50 == 0:
            elapsed = time.time() - t0
            print(f"  [{i+1}/{len(vessel_ids)}] {elapsed:.0f}s  "
                  f"dep={total_assigned_dep} ret={total_assigned_ret}")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")
    print(f"Events: {total_events}")
    print(f"Assigned departure port: {total_assigned_dep} "
          f"({total_assigned_dep/total_events*100:.1f}%)")
    print(f"Assigned return port:    {total_assigned_ret} "
          f"({total_assigned_ret/total_events*100:.1f}%)")

    if args.dry_run:
        # Show sample results
        samples = [(eid, r) for eid, r in list(all_results.items())[:20]
                   if "departure_port" in r]
        print(f"\nSample assignments (first {len(samples)}):")
        for eid, r in samples[:10]:
            print(f"  event {eid}: dep={r.get('departure_port', '-')} "
                  f"t_out={r.get('transit_out_min', '-')} "
                  f"ret={r.get('return_port', '-')} "
                  f"t_back={r.get('transit_back_min', '-')}")
    else:
        # Update database
        print("\nUpdating database...")
        update_count = 0
        for eid, r in all_results.items():
            if "departure_port" not in r and "return_port" not in r:
                continue
            cur.execute("""
                UPDATE stage3_vessel_events SET
                    departure_port = COALESCE(%s, departure_port),
                    departure_time = COALESCE(%s, departure_time),
                    transit_out_min = COALESCE(%s, transit_out_min),
                    return_port = COALESCE(%s, return_port),
                    return_time = COALESCE(%s, return_time),
                    transit_back_min = COALESCE(%s, transit_back_min),
                    transit_dist_km = COALESCE(%s, transit_dist_km)
                WHERE id = %s
            """, (
                r.get("departure_port"),
                r.get("departure_time"),
                r.get("transit_out_min"),
                r.get("return_port"),
                r.get("return_time"),
                r.get("transit_back_min"),
                r.get("transit_dist_km"),
                eid,
            ))
            update_count += 1

        conn.commit()
        print(f"Updated {update_count} events")

    conn.close()


if __name__ == "__main__":
    main()
