#!/usr/bin/env python3
# Phase A airport imputation — fill missing departure/return airports on
#        confirmed helicopter events using same-aircraft prior detections as templates.
#
# Reads stage3_helicopter_events. For each event with NULL departure_airport or
# return_airport, finds confirmed events from the SAME icao24 (templates) and
# applies a matching rule to attribute the most-likely airport.
#
# Imputed values are written to NEW columns so the original detection fields are
# preserved verbatim:
#   - departure_airport_inferred TEXT
#   - return_airport_inferred    TEXT
#   - inference_method           TEXT
#   - inference_confidence       REAL  (0..1)
#
# WARNING: re-running stage3_helicopter_events.py drops and re-creates
# stage3_helicopter_events, which wipes these inferred columns. Run this script
# AFTER each base Stage 3 run.
#
# Output:
# - PostgreSQL columns added to stage3_helicopter_events (idempotent ALTER TABLE)
# - Stdout summary of imputation coverage
#
# Usage:
#   python stage3b_airport_imputation.py
#   python stage3b_airport_imputation.py --dry-run

import argparse
import sys
from collections import defaultdict, Counter
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras


DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "windfarm",
    "user":     "thesis",
    "password": "thesis2026",
}


_ALTER_TABLE = """
ALTER TABLE stage3_helicopter_events
    ADD COLUMN IF NOT EXISTS departure_airport_inferred TEXT,
    ADD COLUMN IF NOT EXISTS return_airport_inferred    TEXT,
    ADD COLUMN IF NOT EXISTS inference_method           TEXT,
    ADD COLUMN IF NOT EXISTS inference_confidence       REAL;
"""

_FETCH_ALL = """
SELECT icao24, project_name, turbine_code, visit_start, visit_end,
       departure_airport, return_airport,
       transit_out_min, transit_back_min, airport_distance_km
FROM stage3_helicopter_events
ORDER BY icao24, visit_start;
"""

_UPDATE_INFERRED = """
UPDATE stage3_helicopter_events
SET departure_airport_inferred = %(dep)s,
    return_airport_inferred    = %(ret)s,
    inference_method           = %(method)s,
    inference_confidence       = %(confidence)s
WHERE icao24 = %(icao24)s
  AND turbine_code = %(turbine_code)s
  AND visit_start = %(visit_start)s;
"""


def build_templates(events: list[dict]) -> dict[str, list[dict]]:
    """Group confirmed events (those with at least one airport detected) by icao24.

    Returns: {icao24: [template_event, ...]} — each template has at least one of
    departure_airport or return_airport set, and these are the prior knowledge
    the matcher draws from.
    """
    templates = defaultdict(list)
    for e in events:
        if e["departure_airport"] is not None or e["return_airport"] is not None:
            templates[e["icao24"]].append(e)
    return templates


def match_template(event: dict, templates: list[dict]) -> dict | None:
    """Decide which airport(s) to attribute to an event with missing airport(s).

    Args:
        event:     The target event with NULL departure_airport and/or return_airport.
                   Keys available: icao24, project_name, turbine_code, visit_start,
                   visit_end, departure_airport (may be None), return_airport (may be
                   None), transit_out_min, transit_back_min, airport_distance_km.
        templates: List of confirmed events for the SAME icao24 (already filtered
                   by caller). Each template has at least one airport set.

    Returns:
        A dict with keys {dep, ret, method, confidence} describing the inferred
        airports, or None if no inference can be made.
            - dep, ret:     str airport names (or None for whichever still cannot
                            be inferred). Do NOT overwrite a non-NULL field on
                            the input event.
            - method:       str describing which rule fired (free-form, used in
                            audit trail and the methodology section).
            - confidence:   float in [0, 1].

    """
    CONFIDENCE_FLOOR = 0.5
    DAYS_WINDOW = 30
    HOUR_WINDOW = 2
    # Minimum number of templates with a non-null airport value before any
    # inference is allowed. Without this floor, a single template would yield
    # confidence = 1.0 and trivially clear CONFIDENCE_FLOOR. With ≥3 templates
    # and a 50% agreement floor, the worst-case admitted inference is 2-of-3
    # agreement — a defensible minimum evidence bar.
    MIN_EVIDENCE_COUNT = 3

    ev_date = event["visit_start"].date() if hasattr(event["visit_start"], "date") else event["visit_start"]
    ev_hour = event["visit_start"].hour
    ev_project = event["project_name"]

    def in_day_window(t):
        d = t.date() if hasattr(t, "date") else t
        return abs((d - ev_date).days) <= DAYS_WINDOW

    def in_hour_window(t):
        h = t.hour
        return min((h - ev_hour) % 24, (ev_hour - h) % 24) <= HOUR_WINDOW

    # Tiers progress from strongest to weakest evidence. The fallback tier
    # (any same-icao24 template, regardless of farm) was removed: in this
    # dataset it fired almost exclusively for South_Fork events on aircraft
    # that have NO confirmed South_Fork airport. The rule then mode-picks
    # the helicopter's most-frequent airport from OTHER farms — which is
    # Martha's Vineyard for all three affected aircraft, even though
    # Republic Airport (Long Island) is geographically closer to South_Fork.
    # Dropping the tier marks that data gap honestly as "no_attribution"
    # rather than filling it with cross-farm mode imputation.
    tiers = [
        ("strict_same_farm_30d_2h",
         [t for t in templates if t["project_name"] == ev_project
          and in_day_window(t["visit_start"]) and in_hour_window(t["visit_start"])]),
        ("strict_same_farm_30d",
         [t for t in templates if t["project_name"] == ev_project
          and in_day_window(t["visit_start"])]),
        ("same_farm_any_time",
         [t for t in templates if t["project_name"] == ev_project]),
    ]

    def pick_airport(field, candidates):
        with_field = [t[field] for t in candidates if t[field] is not None]
        if len(with_field) < MIN_EVIDENCE_COUNT:
            return None, 0.0
        counts = Counter(with_field)
        airport, n = counts.most_common(1)[0]
        return airport, n / len(with_field)

    needs_dep = event["departure_airport"] is None
    needs_ret = event["return_airport"] is None

    for method, candidates in tiers:
        if not candidates:
            continue
        dep, dep_conf = pick_airport("departure_airport", candidates) if needs_dep else (None, 0.0)
        ret, ret_conf = pick_airport("return_airport",    candidates) if needs_ret else (None, 0.0)

        accepted_dep = dep if dep_conf >= CONFIDENCE_FLOOR else None
        accepted_ret = ret if ret_conf >= CONFIDENCE_FLOOR else None
        if not accepted_dep and not accepted_ret:
            continue

        confs = [c for c, kept in [(dep_conf, accepted_dep), (ret_conf, accepted_ret)] if kept]
        return {
            "dep": accepted_dep,
            "ret": accepted_ret,
            "method": method,
            "confidence": round(sum(confs) / len(confs), 3),
        }

    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Phase A — impute missing departure/return airports on Stage 3 events."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute imputations and print summary, but do not write to DB.")
    args = parser.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)

    print("Adding inference columns (idempotent) ...")
    with conn.cursor() as cur:
        cur.execute(_ALTER_TABLE)
        # Reset all inferred fields. The script writes fresh inferences below;
        # any event the new rule declines will correctly land in NULL state
        # rather than retaining stale values from a previous tier configuration.
        cur.execute("""
            UPDATE stage3_helicopter_events
               SET departure_airport_inferred = NULL,
                   return_airport_inferred    = NULL,
                   inference_method           = NULL,
                   inference_confidence       = NULL
        """)
    conn.commit()

    print("Loading events ...")
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_FETCH_ALL)
        events = cur.fetchall()

    templates_by_icao = build_templates(events)
    n_helis_with_templates = len(templates_by_icao)
    n_total_templates = sum(len(v) for v in templates_by_icao.values())

    print(f"Loaded {len(events)} events.")
    print(f"Template index: {n_total_templates} confirmed events across "
          f"{n_helis_with_templates} aircraft.")
    print()

    inferred_dep = 0
    inferred_ret = 0
    skipped_no_template = 0
    skipped_rule_returned_none = 0
    method_counter = Counter()
    updates = []

    for ev in events:
        if ev["departure_airport"] and ev["return_airport"]:
            continue  # nothing to infer

        templates = templates_by_icao.get(ev["icao24"], [])
        if not templates:
            skipped_no_template += 1
            continue

        result = match_template(ev, templates)
        if result is None:
            skipped_rule_returned_none += 1
            continue

        # Never overwrite a confirmed value
        new_dep = result.get("dep") if ev["departure_airport"] is None else None
        new_ret = result.get("ret") if ev["return_airport"] is None else None
        if not new_dep and not new_ret:
            skipped_rule_returned_none += 1
            continue

        if new_dep:
            inferred_dep += 1
        if new_ret:
            inferred_ret += 1
        method_counter[result.get("method", "unspecified")] += 1

        updates.append({
            "icao24":       ev["icao24"],
            "turbine_code": ev["turbine_code"],
            "visit_start":  ev["visit_start"],
            "dep":          new_dep,
            "ret":          new_ret,
            "method":       result.get("method"),
            "confidence":   result.get("confidence"),
        })

    print(f"Imputation summary:")
    print(f"  Events with both fields already filled : "
          f"{sum(1 for e in events if e['departure_airport'] and e['return_airport'])}")
    print(f"  Events with no aircraft templates       : {skipped_no_template}")
    print(f"  Events the rule declined                : {skipped_rule_returned_none}")
    print(f"  Departure airports inferred             : {inferred_dep}")
    print(f"  Return airports inferred                : {inferred_ret}")
    print(f"  Method breakdown                        : {dict(method_counter)}")
    print()

    if args.dry_run:
        print("Dry run — no rows written.")
        conn.close()
        return 0

    print(f"Writing {len(updates)} rows ...")
    with conn.cursor() as cur:
        for u in updates:
            cur.execute(_UPDATE_INFERRED, u)
    conn.commit()
    conn.close()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
