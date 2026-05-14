#!/usr/bin/env python3
"""Wave height vs vessel maintenance activity — correlation analysis.

Tests whether daily vessel maintenance event counts at the four US East Coast
offshore wind farms are correlated with daily-mean significant wave height (Hs)
from ICOADS, controlling for the seasonal confound (summer = both calmer seas
AND scheduled-maintenance campaign season).

Output:
  /mnt/d/thesis/main/figures/data_stats/wave_height_vs_activity.{pdf,png}

Three-panel figure:
  (a) Scatter: daily mean Hs vs daily event count, with Pearson r
  (b) Binned dose-response: mean events/day per 0.5 m Hs bin
  (c) Within-month residuals: deconfounded correlation
"""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
from scipy import stats

from pipeline_common import DB_CONFIG

OUT_DIR = Path("/mnt/d/thesis/main/figures/data_stats")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Study-area AOI bounding box (matches existing weather-comparison script)
LON_MIN, LON_MAX = -72.0, -69.5
LAT_MIN, LAT_MAX =  40.0,  42.0

WINDOW_START = "2024-01-01"
WINDOW_END   = "2025-07-01"

# CTV ladder boarding threshold (Dalgic et al. 2014)
HS_CTV_LIMIT = 1.5
# Heavy-duty SOV gangway threshold (Ampelmann)
HS_SOV_LIMIT = 3.5


def fetch_daily_panel() -> pd.DataFrame:
    """Return one row per day with mean Hs and total Tier-1+Tier-2 event count."""
    conn = psycopg2.connect(**DB_CONFIG)
    q = """
    WITH w AS (
      SELECT date_trunc('day', time)::date AS day,
             AVG(wave_height) AS hs,
             COUNT(wave_height) AS n_wave_obs
      FROM weather_observations
      WHERE time >= %s AND time < %s
        AND latitude BETWEEN %s AND %s
        AND longitude BETWEEN %s AND %s
        AND wave_height IS NOT NULL
      GROUP BY 1
    ),
    v AS (
      SELECT visit_start::date AS day,
             COUNT(*) FILTER (WHERE tier = 1) AS n_tier1,
             COUNT(*) FILTER (WHERE tier = 2) AS n_tier2
      FROM stage3_vessel_events
      WHERE visit_start >= %s AND visit_start < %s
        AND tier IN (1, 2)
      GROUP BY 1
    )
    SELECT w.day, w.hs, w.n_wave_obs,
           COALESCE(v.n_tier1, 0) AS n_tier1,
           COALESCE(v.n_tier2, 0) AS n_tier2
    FROM w LEFT JOIN v USING (day)
    ORDER BY 1
    """
    df = pd.read_sql(q, conn, params=(
        WINDOW_START, WINDOW_END, LAT_MIN, LAT_MAX, LON_MIN, LON_MAX,
        WINDOW_START, WINDOW_END))
    conn.close()
    df["day"] = pd.to_datetime(df["day"])
    df["month"] = df["day"].dt.month
    df["n_events"] = df["n_tier1"] + df["n_tier2"]
    return df


def deconfound_within_month(df: pd.DataFrame) -> pd.DataFrame:
    """Remove the seasonal confound from the (Hs, n_events) relationship.

    The raw correlation between daily Hs and daily event count is partly causal
    (rough seas suppress operations) and partly seasonal (summer has both calmer
    seas AND more scheduled maintenance campaigns).  To isolate the weather
    effect, we need to compare days *within the same month*: if Hs causally
    drives activity, then on a rough day within July, activity should still be
    below the July baseline.

    Your task: add a `hs_resid` and `n_events_resid` column to `df` that strip
    out the per-month mean of each variable, leaving the within-month deviation.

    Returns the same DataFrame with two new columns added.
    """
    monthly_hs = df.groupby("month")["hs"].transform("mean")
    monthly_ev = df.groupby("month")["n_events"].transform("mean")
    df = df.copy()
    df["hs_resid"] = df["hs"] - monthly_hs
    df["n_events_resid"] = df["n_events"] - monthly_ev
    # Drop rows from months with <5 observations (residuals are unstable)
    month_counts = df.groupby("month")["day"].transform("count")
    df = df[month_counts >= 5].reset_index(drop=True)
    return df


def main() -> None:
    df = fetch_daily_panel()
    print(f"Loaded {len(df)} days of joined weather+events data")
    print(f"  mean Hs = {df['hs'].mean():.2f} m, "
          f"mean events/day = {df['n_events'].mean():.1f}")

    # Raw correlation (confounded with season)
    r_raw, p_raw = stats.pearsonr(df["hs"], df["n_events"])
    rho_raw, p_rho = stats.spearmanr(df["hs"], df["n_events"])
    print(f"\nRaw correlation:")
    print(f"  Pearson  r = {r_raw:+.3f}  (p = {p_raw:.2e})")
    print(f"  Spearman ρ = {rho_raw:+.3f}  (p = {p_rho:.2e})")

    # Deconfound — student fills this in
    df = deconfound_within_month(df)
    if "hs_resid" in df.columns and "n_events_resid" in df.columns:
        r_dec, p_dec = stats.pearsonr(df["hs_resid"], df["n_events_resid"])
        rho_dec, p_rho_dec = stats.spearmanr(df["hs_resid"], df["n_events_resid"])
        print(f"\nDeconfounded (within-month residuals):")
        print(f"  Pearson  r = {r_dec:+.3f}  (p = {p_dec:.2e})")
        print(f"  Spearman ρ = {rho_dec:+.3f}  (p = {p_rho_dec:.2e})")
    else:
        print("\n[deconfound_within_month] not yet implemented — skipping panel (c)")

    # Plot
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.3))

    # (a) Raw scatter
    ax = axes[0]
    ax.scatter(df["hs"], df["n_events"], s=14, alpha=0.45, color="#1f77b4")
    ax.axvline(HS_CTV_LIMIT, color="grey", ls=":", lw=1, label=f"CTV $H_s$ limit ({HS_CTV_LIMIT} m)")
    ax.axvline(HS_SOV_LIMIT, color="grey", ls="--", lw=1, label=f"SOV heavy $H_s$ limit ({HS_SOV_LIMIT} m)")
    ax.set_xlabel("Daily mean $H_s$ (m)")
    ax.set_ylabel("Daily vessel events (Tier 1+2)")
    ax.set_title(f"(a) Raw: $r = {r_raw:+.3f}$, $\\rho = {rho_raw:+.3f}$")
    ax.legend(fontsize=8, loc="upper right")

    # (b) Binned dose-response
    ax = axes[1]
    bins = [0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 6.0]
    df["hs_bin"] = pd.cut(df["hs"], bins=bins)
    grouped = df.groupby("hs_bin", observed=True)["n_events"].agg(["mean", "sem", "count"])
    centers = [(bins[i] + bins[i + 1]) / 2 for i in range(len(bins) - 1)]
    ax.bar(centers, grouped["mean"], width=[bins[i + 1] - bins[i] for i in range(len(bins) - 1)],
           yerr=grouped["sem"], color="#1f77b4", alpha=0.75, edgecolor="black", capsize=3)
    ax.axvline(HS_CTV_LIMIT, color="grey", ls=":", lw=1)
    ax.axvline(HS_SOV_LIMIT, color="grey", ls="--", lw=1)
    ax.set_xlabel("Daily mean $H_s$ (m)")
    ax.set_ylabel("Mean events / day")
    ax.set_title("(b) Dose-response (binned)")

    # (c) Deconfounded scatter (if implemented)
    ax = axes[2]
    if "hs_resid" in df.columns:
        ax.scatter(df["hs_resid"], df["n_events_resid"], s=14, alpha=0.45, color="#2ca02c")
        ax.axhline(0, color="grey", lw=0.6)
        ax.axvline(0, color="grey", lw=0.6)
        ax.set_xlabel("$H_s$ residual (within-month)")
        ax.set_ylabel("Events residual (within-month)")
        ax.set_title(f"(c) Deconfounded: $r = {r_dec:+.3f}$, $\\rho = {rho_dec:+.3f}$")
    else:
        ax.text(0.5, 0.5, "Deconfounded panel\n(awaiting implementation)",
                ha="center", va="center", transform=ax.transAxes,
                fontsize=11, color="grey")
        ax.set_xticks([]); ax.set_yticks([])

    plt.tight_layout()
    for ext in ("pdf", "png"):
        out = OUT_DIR / f"wave_height_vs_activity.{ext}"
        plt.savefig(out, dpi=160, bbox_inches="tight")
        print(f"wrote {out}")


if __name__ == "__main__":
    main()
