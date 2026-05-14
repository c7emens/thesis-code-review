#!/usr/bin/env python3
"""Direct vessel-vs-helicopter weather sensitivity comparison.

Generates a 2-panel figure (wind speed, wave height) showing the daily-aggregate
distribution of each weather variable, split by which modality was active that
day at the four US East Coast offshore wind farms.

Visibility is omitted because the 2024 ICOADS coverage in the AOI is < 0.1 % of
observations (109 / 199 744) — too sparse to support distributional inference.

Output: /mnt/d/thesis/main/figures/eda/d_modality_weather_comparison.{pdf,png}
"""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import psycopg2

from pipeline_common import DB_CONFIG

OUT_DIR = Path("/mnt/d/thesis/main/figures/eda")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# AOI bounding box (matches study-area extent used elsewhere)
LON_MIN, LON_MAX = -71.7, -70.0
LAT_MIN, LAT_MAX =  40.5,  41.5

YEAR_START = "2024-01-01"
YEAR_END   = "2025-01-01"

CATEGORIES = [
    ("vessel_active", "Vessel-active days",   "#1f77b4"),
    ("vessel_idle",   "Vessel-idle days",     "#a8c7e3"),
    ("heli_active",   "Helicopter-active days",   "#d62728"),
    ("heli_idle",     "Helicopter-idle days",     "#f4a3a4"),
]


def _fetch_daily_weather(conn) -> pd.DataFrame:
    """Daily-aggregate AOI weather. Wind in tenths-of-m/s → divide by 10."""
    q = """
    SELECT date_trunc('day', time)::date AS day,
           AVG(wind_speed)  / 10.0 AS wind_mps,
           AVG(wave_height)        AS wave_m
    FROM weather_observations
    WHERE time >= %s AND time < %s
      AND latitude  BETWEEN %s AND %s
      AND longitude BETWEEN %s AND %s
    GROUP BY 1 ORDER BY 1
    """
    return pd.read_sql(q, conn, params=(
        YEAR_START, YEAR_END, LAT_MIN, LAT_MAX, LON_MIN, LON_MAX))


def _fetch_daily_activity(conn) -> pd.DataFrame:
    """For each day, flag whether vessels and/or helicopters were active."""
    q = """
    SELECT day,
           bool_or(modality = 'vessel') AS vessel_active,
           bool_or(modality = 'heli')   AS heli_active
    FROM (
        SELECT visit_start::date AS day, 'vessel' AS modality
        FROM stage3_vessel_events
        WHERE EXTRACT(YEAR FROM visit_start) = 2024 AND score >= 40
        UNION ALL
        SELECT visit_start::date AS day, 'heli' AS modality
        FROM stage3_helicopter_events
        WHERE EXTRACT(YEAR FROM visit_start) = 2024 AND score >= 40
    ) u
    GROUP BY day
    """
    return pd.read_sql(q, conn)


def main() -> None:
    conn = psycopg2.connect(**DB_CONFIG)
    weather = _fetch_daily_weather(conn)
    activity = _fetch_daily_activity(conn)
    conn.close()

    # Left-join activity into the calendar of weather days; missing → idle
    df = weather.merge(activity, on="day", how="left")
    df["vessel_active"] = df["vessel_active"].fillna(False)
    df["heli_active"]   = df["heli_active"].fillna(False)

    # Per-modality active/idle subsets
    subsets = {
        "vessel_active": df[df["vessel_active"]],
        "vessel_idle":   df[~df["vessel_active"]],
        "heli_active":   df[df["heli_active"]],
        "heli_idle":     df[~df["heli_active"]],
    }

    plt.style.use("seaborn-v0_8-whitegrid")
    fig, (ax_w, ax_h) = plt.subplots(1, 2, figsize=(8.8, 4.2))

    for ax, var, ylabel, title in (
        (ax_w, "wind_mps", "Daily mean wind speed (m/s)",
            "Wind speed by activity category"),
        (ax_h, "wave_m",   "Daily mean wave height (m)",
            "Wave height by activity category"),
    ):
        boxes, labels, colours = [], [], []
        for key, label, colour in CATEGORIES:
            sub = subsets[key][var].dropna()
            if len(sub) == 0:
                continue
            boxes.append(sub.values)
            labels.append(f"{label}\n(n={len(sub)})")
            colours.append(colour)
        bp = ax.boxplot(boxes, labels=labels, patch_artist=True,
                        showfliers=False, widths=0.55)
        for patch, c in zip(bp["boxes"], colours):
            patch.set_facecolor(c)
            patch.set_alpha(0.75)
        for med in bp["medians"]:
            med.set_color("black")
            med.set_linewidth(1.4)
        ax.set_ylabel(ylabel)
        ax.set_title(title, fontsize=10)
        ax.tick_params(axis="x", labelsize=8)

    fig.suptitle("Modality-split weather sensitivity (2024, US-NE AOI)",
                 fontsize=11, y=1.02)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "d_modality_weather_comparison.pdf", bbox_inches="tight")
    fig.savefig(OUT_DIR / "d_modality_weather_comparison.png",
                bbox_inches="tight", dpi=160)
    plt.close(fig)

    # Quick numeric summary for the prose
    print("\nMedians by category:")
    for key, label, _ in CATEGORIES:
        sub = subsets[key]
        n = len(sub)
        wm = sub["wind_mps"].median()
        hm = sub["wave_m"].median()
        print(f"  {key:14s} n={n:3d}  wind_med={wm:.2f} m/s  wave_med={hm:.2f} m")
    print(f"\nWrote {OUT_DIR / 'd_modality_weather_comparison.pdf'}")


if __name__ == "__main__":
    main()
