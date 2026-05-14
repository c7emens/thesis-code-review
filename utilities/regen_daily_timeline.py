#!/usr/bin/env python3
"""Regenerate b_daily_timeline.pdf with monthly x-axis ticks.

Replaces the auto-generated chart that rendered every day as a tick label
(producing a solid black bar on the x-axis). Pulls the same daily-aggregate
query the eda_thesis.py script uses.
"""
from __future__ import annotations
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2

from pipeline_common import DB_CONFIG

OUT = Path("/mnt/d/thesis/main/figures/eda")
PALETTE = {"vessel": "#1f77b4", "heli": "#d62728", "sov": "#2ca02c"}


def main() -> None:
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("""
        WITH vessel_d AS (
            SELECT DATE_TRUNC('day', visit_start)::date AS day, COUNT(*) AS n
            FROM stage3_vessel_events GROUP BY 1
        ), heli_d AS (
            SELECT DATE_TRUNC('day', visit_start)::date AS day, COUNT(*) AS n
            FROM stage3_helicopter_events GROUP BY 1
        ), sov_d AS (
            SELECT DATE_TRUNC('day', interaction_start)::date AS day, COUNT(*) AS n
            FROM stage4_sov_interactions GROUP BY 1
        ), all_days AS (
            SELECT day FROM vessel_d
            UNION SELECT day FROM heli_d
            UNION SELECT day FROM sov_d
        )
        SELECT a.day,
               COALESCE(v.n, 0) AS vessel_events,
               COALESCE(h.n, 0) AS heli_events,
               COALESCE(s.n, 0) AS sov_interactions
        FROM all_days a
        LEFT JOIN vessel_d v ON v.day = a.day
        LEFT JOIN heli_d   h ON h.day = a.day
        LEFT JOIN sov_d    s ON s.day = a.day
        ORDER BY a.day
    """, conn)
    conn.close()

    df["day"] = pd.to_datetime(df["day"])

    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(8.5, 3.8))
    ax.plot(df["day"], df["vessel_events"], color=PALETTE["vessel"],
            linewidth=1.0, label="Vessel events")
    ax.plot(df["day"], df["heli_events"], color=PALETTE["heli"],
            linewidth=1.0, label="Helicopter events")
    ax.plot(df["day"], df["sov_interactions"], color=PALETTE["sov"],
            linewidth=1.0, label="SOV interactions")

    # Monthly major ticks; quarterly minor labels for dense windows
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 4, 7, 10]))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center")

    ax.set_xlabel("Day")
    ax.set_ylabel("Daily event count")
    ax.set_title("Daily event activity across modalities (2024–2025)")
    ax.legend(loc="upper right", fontsize=9, framealpha=0.92)
    ax.margins(x=0.01)

    fig.tight_layout()
    fig.savefig(OUT / "b_daily_timeline.pdf", bbox_inches="tight")
    fig.savefig(OUT / "b_daily_timeline.png",
                bbox_inches="tight", dpi=160)
    plt.close(fig)
    print(f"Wrote {OUT / 'b_daily_timeline.pdf'} ({len(df)} days)")


if __name__ == "__main__":
    main()
