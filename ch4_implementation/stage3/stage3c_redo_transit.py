#!/usr/bin/env python3
# Re-run helicopter transit detection on existing Stage 3 events.
#
# Used after fixing the flight_date partition bug in detect_helicopter_transit
# (stage3_helicopter_events.py:618). Re-classifies airports for every existing
# event without re-running the full Stage 3 pipeline.
#
# Updates stage3_helicopter_events in place. Phase A (stage3b_airport_imputation.py)
# should be re-run AFTER this so the inferred fields reflect the new ground truth.

import sys
import psycopg2
import psycopg2.extras

sys.path.insert(0, "/mnt/d/thesis/scripts")
from stage3_helicopter_events import detect_helicopter_transit, DB_CONFIG


_FETCH = """
SELECT icao24, project_name, turbine_code, visit_start, visit_end,
       departure_airport AS old_dep, return_airport AS old_ret
FROM stage3_helicopter_events
ORDER BY visit_start
"""

_UPDATE = """
UPDATE stage3_helicopter_events
SET departure_airport   = COALESCE(%(departure_airport)s, departure_airport),
    return_airport      = COALESCE(%(return_airport)s,    return_airport),
    transit_out_min     = COALESCE(%(transit_out_min)s,     transit_out_min),
    transit_back_min    = COALESCE(%(transit_back_min)s,    transit_back_min),
    airport_distance_km = COALESCE(%(airport_distance_km)s, airport_distance_km)
WHERE icao24 = %(icao24)s
  AND turbine_code = %(turbine_code)s
  AND visit_start = %(visit_start)s
"""


def main() -> int:
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_FETCH)
        events = cur.fetchall()
    print(f"Loaded {len(events)} events.")

    new_dep_count = 0
    new_ret_count = 0
    lost_dep_count = 0
    lost_ret_count = 0
    unchanged = 0

    for i, ev in enumerate(events, 1):
        if i % 100 == 0:
            print(f"  {i}/{len(events)} ...")

        result = detect_helicopter_transit(conn, ev) or {}
        new_dep = result.get("departure_airport")
        new_ret = result.get("return_airport")

        if new_dep and not ev["old_dep"]:
            new_dep_count += 1
        if new_ret and not ev["old_ret"]:
            new_ret_count += 1
        if ev["old_dep"] and not new_dep:
            lost_dep_count += 1
        if ev["old_ret"] and not new_ret:
            lost_ret_count += 1
        if (ev["old_dep"] == new_dep) and (ev["old_ret"] == new_ret):
            unchanged += 1

        with conn.cursor() as cur:
            cur.execute(_UPDATE, {
                "icao24":       ev["icao24"],
                "turbine_code": ev["turbine_code"],
                "visit_start":  ev["visit_start"],
                "departure_airport":   new_dep,
                "return_airport":      new_ret,
                "transit_out_min":     result.get("transit_out_min"),
                "transit_back_min":    result.get("transit_back_min"),
                "airport_distance_km": result.get("airport_distance_km"),
            })
        conn.commit()

    conn.close()

    print()
    print(f"Newly detected departures : +{new_dep_count}")
    print(f"Newly detected returns    : +{new_ret_count}")
    print(f"Lost departures           : -{lost_dep_count}  (sanity check — should be 0)")
    print(f"Lost returns              : -{lost_ret_count}  (sanity check — should be 0)")
    print(f"Unchanged                 : {unchanged}/{len(events)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
