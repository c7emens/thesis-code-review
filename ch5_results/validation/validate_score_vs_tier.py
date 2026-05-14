#!/usr/bin/env python3
# Validate the continuous-score classifier against the legacy tier classifier.
#
# Reads `stage3_vessel_events_legacy` (snapshot of pre-refactor tier-rule events)
# and the live `stage3_vessel_events` (continuous-score events), matches by
# (mms_id, project_name, visit_start, turbine_code), and emits:
#   - confusion matrix: legacy_tier × new_tier (and a "missing" column for
#     legacy events that didn't survive the new pipeline)
#   - per-band score histograms within each legacy tier
#   - the acceptance criteria from the refactor plan:
#       * ≥ 90 % of old Tier-1 should land at score ≥ 75 (new Tier-1)
#       * ≥ 80 % of old Tier-2 should land at score ≥ 40 (new Tier-1 ∪ Tier-2)
#       * old Tier-3: ≥ 50 % should land at score ≥ 40 (the rest fall below
#         threshold — acceptable since Tier-3 was always lower-confidence)
#
# This script is intended for the ch5 validation subsection.
#
# Usage:
#   python validate_score_vs_tier.py
#   python validate_score_vs_tier.py --tex /tmp/score_vs_tier.tex

import argparse
import sys
from collections import defaultdict

import psycopg2

from pipeline_common import DB_CONFIG


def load_pairs(conn) -> tuple[dict, dict]:
    """Return (legacy_by_key, new_by_key) — events keyed by the upsert tuple."""
    legacy: dict[tuple, dict] = {}
    new:    dict[tuple, dict] = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT mms_id, project_name, visit_start, COALESCE(turbine_code, ''),
                   tier, score
            FROM stage3_vessel_events_legacy
        """)
        for row in cur.fetchall():
            mms, prj, vs, tc, t, s = row
            legacy[(mms, prj, vs, tc)] = {"tier": t, "score": s}

        cur.execute("""
            SELECT mms_id, project_name, visit_start, COALESCE(turbine_code, ''),
                   tier, score
            FROM stage3_vessel_events
        """)
        for row in cur.fetchall():
            mms, prj, vs, tc, t, s = row
            new[(mms, prj, vs, tc)] = {"tier": t, "score": s}
    return legacy, new


def build_confusion(legacy: dict, new: dict) -> dict:
    """Return confusion[(legacy_tier, new_tier_or_none)] = count.

    legacy_tier is one of {1, 2, 3}; new_tier is one of {0, 1, 2} for
    events present in the post-refactor table (0 = sub-threshold but
    still emitted with a score) or None for events that no longer
    qualify even by hard pre-filters (n_positions < 3 or duration < 5
    min) or that the run hasn't yet covered.
    """
    cm: dict[tuple, int] = defaultdict(int)
    for key, le in legacy.items():
        ne = new.get(key)
        nt = ne["tier"] if ne is not None else None
        cm[(le["tier"], nt)] += 1
    return cm


def acceptance_criteria(cm: dict) -> list[tuple[str, float, float, bool]]:
    """Compute the three acceptance criteria from the refactor plan.

    Returns list of (label, observed_pct, target_pct, passed).
    """
    def total_in_legacy_tier(t: int) -> int:
        return sum(n for (lt, _), n in cm.items() if lt == t)

    def in_legacy_tier_landing_at_or_above(legacy_t: int, new_min: int) -> int:
        # Score-banded tiers: 1 (high), 2 (moderate), 0 (sub-threshold).
        # "Landing at or above new_min" = nt is in [1, new_min] — that is,
        # at least as confident as the bound. Tier 0 (sub-threshold) is
        # excluded because score < 40.
        return sum(n for (lt, nt), n in cm.items()
                   if lt == legacy_t and nt is not None and 1 <= nt <= new_min)

    rows = []

    # Criterion 1: ≥90% of legacy Tier-1 → new Tier-1 (score ≥ 75)
    n1 = total_in_legacy_tier(1)
    if n1 > 0:
        n1_into_t1 = in_legacy_tier_landing_at_or_above(1, 1)
        pct = 100 * n1_into_t1 / n1
        rows.append(("Legacy Tier-1 → new Tier-1 (score ≥ 75)", pct, 90.0, pct >= 90.0))

    # Criterion 2: ≥80% of legacy Tier-2 → score ≥ 40 (new Tier-1 ∪ Tier-2)
    n2 = total_in_legacy_tier(2)
    if n2 > 0:
        n2_into_emitted = in_legacy_tier_landing_at_or_above(2, 2)
        pct = 100 * n2_into_emitted / n2
        rows.append(("Legacy Tier-2 → emitted (score ≥ 40)", pct, 80.0, pct >= 80.0))

    # Criterion 3: ≥50% of legacy Tier-3 → score ≥ 40
    n3 = total_in_legacy_tier(3)
    if n3 > 0:
        n3_into_emitted = in_legacy_tier_landing_at_or_above(3, 2)
        pct = 100 * n3_into_emitted / n3
        rows.append(("Legacy Tier-3 → emitted (score ≥ 40)", pct, 50.0, pct >= 50.0))

    return rows


def render_text(cm: dict, criteria: list, n_legacy: int, n_new: int) -> str:
    out = []
    out.append("-" * 64)
    out.append("Score vs Tier validation — confusion matrix")
    out.append("-" * 64)
    out.append(f"Legacy events (snapshot): {n_legacy:,}")
    out.append(f"New    events (live):      {n_new:,}")
    out.append("")
    out.append(f"{'':12s}{'new=1':>10s}{'new=2':>10s}{'new=0':>10s}{'dropped':>10s}{'total':>10s}")
    out.append("-" * 66)
    for legacy_t in (1, 2, 3):
        total = sum(n for (lt, _), n in cm.items() if lt == legacy_t)
        if total == 0:
            continue
        n_t1   = sum(n for (lt, nt), n in cm.items() if lt == legacy_t and nt == 1)
        n_t2   = sum(n for (lt, nt), n in cm.items() if lt == legacy_t and nt == 2)
        n_t0   = sum(n for (lt, nt), n in cm.items() if lt == legacy_t and nt == 0)
        n_drop = sum(n for (lt, nt), n in cm.items() if lt == legacy_t and nt is None)
        out.append(f"{f'old Tier-{legacy_t}':12s}{n_t1:>10,}{n_t2:>10,}{n_t0:>10,}{n_drop:>10,}{total:>10,}")
    out.append("-" * 66)
    out.append("")
    out.append("Acceptance criteria (Phase-3 refactor plan):")
    for label, pct, target, ok in criteria:
        flag = "✓ PASS" if ok else "✗ FAIL"
        out.append(f"  [{flag}] {label}: {pct:.1f}% (target ≥{target:.0f}%)")
    out.append("")
    return "\n".join(out)


def render_tex(cm: dict, criteria: list) -> str:
    """Emit a LaTeX longtable for the ch5 validation subsection."""
    out = ["\\begin{table}[htbp]\\centering",
           "\\caption{Confusion matrix of legacy tier vs continuous-score tier "
           "after the Phase~3 refactor. Counts of events keyed by "
           "$(\\texttt{mms\\_id}, \\texttt{project}, \\texttt{visit\\_start}, "
           "\\texttt{turbine\\_code})$. New Tier~0 = sub-threshold "
           "but emitted; Dropped = visit no longer qualifies by hard "
           "pre-filters (n\\_positions $<\\!3$ or duration $<\\!5$~min).}",
           "\\label{tab:score_vs_tier}",
           "\\begin{tabular}{lrrrrr}",
           "\\toprule",
           "\\textbf{Legacy tier} & \\textbf{New Tier 1} & \\textbf{New Tier 2} & "
           "\\textbf{New Tier 0} & \\textbf{Dropped} & \\textbf{Total} \\\\",
           "\\midrule"]
    for legacy_t in (1, 2, 3):
        total = sum(n for (lt, _), n in cm.items() if lt == legacy_t)
        if total == 0:
            continue
        n_t1   = sum(n for (lt, nt), n in cm.items() if lt == legacy_t and nt == 1)
        n_t2   = sum(n for (lt, nt), n in cm.items() if lt == legacy_t and nt == 2)
        n_t0   = sum(n for (lt, nt), n in cm.items() if lt == legacy_t and nt == 0)
        n_drop = sum(n for (lt, nt), n in cm.items() if lt == legacy_t and nt is None)
        out.append(f"Tier {legacy_t} & {n_t1:,} & {n_t2:,} & {n_t0:,} & {n_drop:,} & {total:,} \\\\")
    out.extend(["\\bottomrule", "\\end{tabular}", "\\end{table}", "",
               "\\paragraph{Acceptance criteria.}",
               "\\begin{itemize}"])
    for label, pct, target, ok in criteria:
        flag = "\\checkmark" if ok else "$\\times$"
        # Sanitise Unicode so pdflatex compiles without --shell-escape or fontspec
        ascii_label = label.replace("→", "$\\to$").replace("≥", "$\\geq$")
        out.append(f"  \\item {flag}\\ {ascii_label}: {pct:.1f}\\% (target $\\geq {target:.0f}$\\%).")
    out.append("\\end{itemize}")
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tex", metavar="PATH",
                        help="Write LaTeX confusion-matrix table to this file.")
    args = parser.parse_args()

    with psycopg2.connect(**DB_CONFIG) as conn:
        legacy, new = load_pairs(conn)

    if not legacy:
        print("ERROR: stage3_vessel_events_legacy is empty — nothing to validate against.",
              file=sys.stderr)
        return 1

    cm = build_confusion(legacy, new)
    criteria = acceptance_criteria(cm)
    print(render_text(cm, criteria, len(legacy), len(new)))

    if args.tex:
        from pathlib import Path
        Path(args.tex).write_text(render_tex(cm, criteria) + "\n")
        print(f"  LaTeX table written → {args.tex}")

    failed = [c for c in criteria if not c[3]]
    return 0 if not failed else 2


if __name__ == "__main__":
    raise SystemExit(main())
