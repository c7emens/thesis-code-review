#!/usr/bin/env python3
# Leave-one-out cross-validation of the airport imputation rule.
#
# For each event with BOTH departure_airport and return_airport confirmed,
# blank one side, run match_template() against the remaining templates, and
# check whether the rule recovers the true value.

import sys
from collections import Counter

import psycopg2
import psycopg2.extras

sys.path.insert(0, "/mnt/d/thesis/scripts")
from stage3b_airport_imputation import build_templates, match_template, DB_CONFIG, _FETCH_ALL


def loo_validate(events, field):
    """For every event where `field` is non-null, blank it, infer it via
    match_template against templates that EXCLUDE this event (leave-one-out),
    and compare predicted vs ground truth. `field` is 'departure_airport' or
    'return_airport'."""
    truths = [e for e in events if e[field] is not None]
    print(f"\nLOO on {field}: {len(truths)} ground-truth events")

    correct_by_method = Counter()
    total_by_method   = Counter()
    abstained         = 0
    wrong_examples    = []

    for target in truths:
        # Build template index excluding this exact event
        others = [e for e in events
                  if not (e["icao24"] == target["icao24"]
                          and e["turbine_code"] == target["turbine_code"]
                          and e["visit_start"] == target["visit_start"])]
        templates = build_templates(others).get(target["icao24"], [])
        if not templates:
            abstained += 1
            continue

        # Make a "missing" copy: blank the field under test
        probe = dict(target)
        probe[field] = None

        result = match_template(probe, templates)
        if result is None:
            abstained += 1
            continue

        predicted = result.get("dep") if field == "departure_airport" else result.get("ret")
        if predicted is None:
            abstained += 1
            continue

        method = result["method"]
        total_by_method[method] += 1
        if predicted == target[field]:
            correct_by_method[method] += 1
        elif len(wrong_examples) < 5:
            wrong_examples.append({
                "icao24": target["icao24"],
                "project": target["project_name"],
                "true": target[field],
                "pred": predicted,
                "method": method,
                "conf": result.get("confidence"),
            })

    total = sum(total_by_method.values())
    correct = sum(correct_by_method.values())
    print(f"  Predictions made : {total} ({abstained} abstained)")
    print(f"  Correct          : {correct} ({100.0*correct/total:.1f}%)" if total else "  No predictions")
    print(f"  By method:")
    for m in sorted(total_by_method, key=lambda x: -total_by_method[x]):
        n = total_by_method[m]
        c = correct_by_method[m]
        print(f"    {m:<28} : {c}/{n} = {100.0*c/n:.1f}%")
    if wrong_examples:
        print(f"  Sample wrong predictions:")
        for w in wrong_examples:
            print(f"    {w['icao24']} @ {w['project']:<16} true={w['true']!r:<32} "
                  f"pred={w['pred']!r:<32} method={w['method']} conf={w['conf']}")


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--table", default="stage3_helicopter_events",
                   help="Source table to validate against (e.g. stage3_helicopter_events_pre_c1).")
    args = p.parse_args()

    sql = _FETCH_ALL.replace("FROM stage3_helicopter_events", f"FROM {args.table}")
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        events = cur.fetchall()
    conn.close()

    print(f"Loaded {len(events)} events from {args.table}.")
    loo_validate(events, "departure_airport")
    loo_validate(events, "return_airport")


if __name__ == "__main__":
    main()
