#!/usr/bin/env python3
"""Comprehensive weather-sensitivity analysis: Hs and wind speed against
each modality's maintenance activity, with vessel-type breakdown.

Answers four questions empirically:
  1.  Is daily Hs correlated with vessel maintenance activity?
  2.  Is daily Hs correlated with helicopter maintenance activity?
  3.  Is daily wind speed correlated with each?
  4.  Do different vessel categories (CTV vs Support/SOV) show different
      weather sensitivities --- specifically, does the Hs sensitivity
      gradient match the operational literature (CTV at Hs <= 1.5 m,
      SOV up to Hs <= 3.5 m)?

Outputs:
  /mnt/d/thesis/main/figures/data_stats/weather_sensitivity_grid.{pdf,png}
  /mnt/d/thesis/main/tables/eda/weather_sensitivity_correlations.tex

The grid figure is 2 rows (Hs, wind) x 3 columns (CTV, Support, Helicopter).
Each panel shows the binned dose-response with +-1 SEM error bars, plus the
raw and within-month-deconfounded Pearson r and Spearman rho annotated.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
from scipy import stats

from pipeline_common import DB_CONFIG

FIG_DIR = Path("/mnt/d/thesis/main/figures/data_stats")
TBL_DIR = Path("/mnt/d/thesis/main/tables/eda")
FIG_DIR.mkdir(parents=True, exist_ok=True)
TBL_DIR.mkdir(parents=True, exist_ok=True)

# AOI bounding box
LON_MIN, LON_MAX = -72.0, -69.5
LAT_MIN, LAT_MAX = 40.0, 42.0
WINDOW_START = "2024-01-01"
WINDOW_END = "2025-07-01"

# Operational threshold guides
HS_CTV_LIMIT = 1.5  # Dalgic 2014 ladder boarding
HS_SOV_LIMIT = 3.5  # Ampelmann heavy-duty gangway

# Modality definitions: (label, sql_event_filter, colour)
MODALITIES = [
    ("CTV", "stage3_vessel_events", "tier IN (1,2) AND vessel_category = 'CTV'", "#1f77b4"),
    ("Support", "stage3_vessel_events", "tier IN (1,2) AND vessel_category = 'Support'", "#2ca02c"),
    ("Helicopter", "stage3_helicopter_events", "score >= 40", "#d62728"),
]


def fetch_daily_panel() -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)

    # Weather: daily AOI mean Hs and wind speed
    q_weather = """
    SELECT date_trunc('day', time)::date AS day,
           AVG(wave_height) AS hs,
           AVG(wind_speed) / 10.0 AS wind_mps
    FROM weather_observations
    WHERE time >= %s AND time < %s
      AND latitude BETWEEN %s AND %s
      AND longitude BETWEEN %s AND %s
    GROUP BY 1
    """
    weather = pd.read_sql(q_weather, conn, params=(
        WINDOW_START, WINDOW_END, LAT_MIN, LAT_MAX, LON_MIN, LON_MAX))
    weather["day"] = pd.to_datetime(weather["day"])
    weather["month"] = weather["day"].dt.month

    # Events: one count column per modality
    for label, table, where, _ in MODALITIES:
        q_events = f"""
        SELECT visit_start::date AS day, COUNT(*) AS n_events
        FROM {table}
        WHERE visit_start >= %s AND visit_start < %s AND {where}
        GROUP BY 1
        """
        ev = pd.read_sql(q_events, conn, params=(WINDOW_START, WINDOW_END))
        ev["day"] = pd.to_datetime(ev["day"])
        weather = weather.merge(
            ev.rename(columns={"n_events": f"n_{label}"}),
            on="day", how="left")
        weather[f"n_{label}"] = weather[f"n_{label}"].fillna(0).astype(int)

    conn.close()
    return weather


def correlate_with_deconfound(df: pd.DataFrame, x_col: str, y_col: str) -> dict:
    """Compute raw + within-month-deconfounded correlations for (x, y)."""
    sub = df.dropna(subset=[x_col, y_col]).copy()
    if len(sub) < 30:
        return dict(n=len(sub), r_raw=np.nan, rho_raw=np.nan, p_raw=np.nan,
                    r_dec=np.nan, rho_dec=np.nan, p_dec=np.nan)
    # Raw
    r_raw, p_raw = stats.pearsonr(sub[x_col], sub[y_col])
    rho_raw, _ = stats.spearmanr(sub[x_col], sub[y_col])
    # Deconfound within month: subtract per-month mean
    sub["x_resid"] = sub[x_col] - sub.groupby("month")[x_col].transform("mean")
    sub["y_resid"] = sub[y_col] - sub.groupby("month")[y_col].transform("mean")
    # Drop months with <5 obs
    counts = sub.groupby("month")["day"].transform("count")
    sub = sub[counts >= 5]
    r_dec, p_dec = stats.pearsonr(sub["x_resid"], sub["y_resid"])
    rho_dec, _ = stats.spearmanr(sub["x_resid"], sub["y_resid"])
    return dict(n=len(sub), r_raw=r_raw, rho_raw=rho_raw, p_raw=p_raw,
                r_dec=r_dec, rho_dec=rho_dec, p_dec=p_dec)


def plot_panel(ax, df, x_col, y_col, x_bins, x_label, y_label, title,
               colour, vlines=()):
    """One binned dose-response panel with correlation annotation."""
    sub = df.dropna(subset=[x_col, y_col])
    if len(sub) < 10:
        ax.text(0.5, 0.5, "insufficient data", ha="center", va="center",
                transform=ax.transAxes)
        ax.set_xticks([]); ax.set_yticks([])
        return
    sub = sub.copy()
    sub["bin"] = pd.cut(sub[x_col], bins=x_bins)
    grouped = (sub.groupby("bin", observed=False)[y_col]
                  .agg(["mean", "sem", "count"]))
    centers = [(x_bins[i] + x_bins[i + 1]) / 2 for i in range(len(x_bins) - 1)]
    widths = [x_bins[i + 1] - x_bins[i] for i in range(len(x_bins) - 1)]
    means = grouped["mean"].fillna(0).to_numpy()
    sems = grouped["sem"].fillna(0).to_numpy()
    ax.bar(centers, means, width=widths, yerr=sems,
           color=colour, alpha=0.75, edgecolor="black", capsize=2.5, linewidth=0.5)
    for vx, vstyle, vlabel in vlines:
        ax.axvline(vx, color="grey", ls=vstyle, lw=0.9)
    # Correlation annotation
    cc = correlate_with_deconfound(df, x_col, y_col)
    ann = (f"$r$ = {cc['r_raw']:+.2f}, $\\rho$ = {cc['rho_raw']:+.2f}\n"
           f"within-month $r$ = {cc['r_dec']:+.2f}")
    ax.text(0.97, 0.95, ann, ha="right", va="top", transform=ax.transAxes,
            fontsize=8, bbox=dict(facecolor="white", alpha=0.85,
                                  edgecolor="none", pad=2))
    ax.set_xlabel(x_label, fontsize=9)
    ax.set_ylabel(y_label, fontsize=9)
    ax.set_title(title, fontsize=10)
    ax.tick_params(labelsize=8)


def plot_comparable(ax, df, x_col, x_bins, x_label, vlines=()):
    """Overlay all three cohorts in one panel, normalised to each cohort's
    own calmest-bin rate so the *shape* of decline is directly comparable
    across modalities with vastly different absolute volumes."""
    centers = [(x_bins[i] + x_bins[i + 1]) / 2 for i in range(len(x_bins) - 1)]
    ann_lines = []
    for label, _, _, colour in MODALITIES:
        y_col = f"n_{label}"
        sub = df.dropna(subset=[x_col, y_col]).copy()
        sub["bin"] = pd.cut(sub[x_col], bins=x_bins)
        means = (sub.groupby("bin", observed=False)[y_col]
                    .mean().fillna(0).to_numpy())
        sems = (sub.groupby("bin", observed=False)[y_col]
                   .sem().fillna(0).to_numpy())
        # Normalise to calmest-bin mean (= 1.0); calmest bin = bin 0.
        baseline = means[0] if means[0] > 0 else 1.0
        rel_means = means / baseline
        rel_sems  = sems / baseline
        ax.errorbar(centers, rel_means, yerr=rel_sems,
                    color=colour, marker="o", markersize=6, linewidth=1.8,
                    capsize=3, label=label)
        # Correlation annotation per cohort
        cc = correlate_with_deconfound(df, x_col, y_col)
        ann_lines.append(f"{label}: within-month $r$ = {cc['r_dec']:+.2f}")

    for vx, vstyle, _ in vlines:
        ax.axvline(vx, color="grey", ls=vstyle, lw=0.9)
    ax.axhline(1.0, color="black", lw=0.4, alpha=0.4)
    ax.set_xlabel(x_label, fontsize=10)
    ax.set_ylabel("Relative event rate  (calmest bin = $1.0$)", fontsize=10)
    ax.tick_params(labelsize=9)
    ax.grid(axis="y", linestyle=":", alpha=0.4)
    ax.legend(loc="upper right", framealpha=0.95, fontsize=9)
    # Embed within-month correlations top-left as a compact summary
    ax.text(0.02, 0.98, "\n".join(ann_lines), transform=ax.transAxes,
            ha="left", va="top", fontsize=8,
            bbox=dict(facecolor="white", alpha=0.85, edgecolor="none", pad=3))


def main():
    df = fetch_daily_panel()
    print(f"Loaded {len(df)} days")
    for label, _, _, _ in MODALITIES:
        col = f"n_{label}"
        print(f"  {label}: total={df[col].sum()}, mean/day={df[col].mean():.2f}, "
              f"active days={int((df[col] > 0).sum())}")

    # Bin specifications
    hs_bins = [0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 6.0]
    wind_bins = [0, 2, 4, 6, 8, 10, 12, 16, 22]

    hs_vlines = [(HS_CTV_LIMIT, ":", "CTV"), (HS_SOV_LIMIT, "--", "SOV")]
    wind_vlines = []

    # Primary figures: two single-panel overlays (Hs / wind)
    # Each cohort normalised to its own calmest-bin rate so decline shapes
    # can be visually compared across modalities with very different
    # absolute event volumes.
    fig, ax = plt.subplots(figsize=(9.5, 5.5))
    plot_comparable(ax, df, "hs", hs_bins,
                    "Daily mean $H_s$ (m)", vlines=hs_vlines)
    ax.set_title("Weather sensitivity vs significant wave height", fontsize=12)
    plt.tight_layout()
    for ext in ("pdf", "png"):
        out = FIG_DIR / f"weather_sensitivity_hs.{ext}"
        plt.savefig(out, dpi=160, bbox_inches="tight")
        print(f"wrote {out}")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9.5, 5.5))
    plot_comparable(ax, df, "wind_mps", wind_bins,
                    "Daily mean wind speed (m/s)", vlines=wind_vlines)
    ax.set_title("Weather sensitivity vs surface wind speed", fontsize=12)
    plt.tight_layout()
    for ext in ("pdf", "png"):
        out = FIG_DIR / f"weather_sensitivity_wind.{ext}"
        plt.savefig(out, dpi=160, bbox_inches="tight")
        print(f"wrote {out}")
    plt.close(fig)

    # Supplementary figure: detailed 2×3 absolute view
    # Retained as supporting evidence; not referenced as Figure 5.1 in the
    # body but available for the appendix and for absolute-volume context.
    fig, axes = plt.subplots(2, 3, figsize=(13.5, 7.2))
    for j, (label, _, _, colour) in enumerate(MODALITIES):
        y_col = f"n_{label}"
        ylab = f"{label} events / day"
        plot_panel(axes[0, j], df, "hs", y_col, hs_bins,
                   "Daily mean $H_s$ (m)", ylab,
                   f"{label} vs $H_s$", colour, vlines=hs_vlines)
        plot_panel(axes[1, j], df, "wind_mps", y_col, wind_bins,
                   "Daily mean wind speed (m/s)", ylab,
                   f"{label} vs wind speed", colour, vlines=wind_vlines)
    plt.tight_layout()
    for ext in ("pdf", "png"):
        out = FIG_DIR / f"weather_sensitivity_detail.{ext}"
        plt.savefig(out, dpi=160, bbox_inches="tight")
        print(f"wrote {out}")
    plt.close(fig)

    # Build LaTeX table of correlations
    rows = []
    for label, _, _, _ in MODALITIES:
        y_col = f"n_{label}"
        for x_label, x_col in (("$H_s$", "hs"), ("Wind speed", "wind_mps")):
            cc = correlate_with_deconfound(df, x_col, y_col)
            rows.append({
                "Modality": label,
                "Variable": x_label,
                "n": cc["n"],
                "Pearson r (raw)": f"{cc['r_raw']:+.3f}",
                "Spearman rho (raw)": f"{cc['rho_raw']:+.3f}",
                "Pearson r (within-month)": f"{cc['r_dec']:+.3f}",
                "p (within-month)": f"{cc['p_dec']:.1e}",
            })
    summary = pd.DataFrame(rows)
    print("\n=== Correlation summary ===")
    print(summary.to_string(index=False))

    # Write to LaTeX
    tbl_lines = [
        r"\begin{tabular}{llrrrrr}",
        r"\toprule",
        r"\textbf{Modality} & \textbf{Variable} & $n$ & "
        r"\textbf{$r$ (raw)} & \textbf{$\rho$ (raw)} & "
        r"\textbf{$r$ (w-month)} & \textbf{$p$} \\",
        r"\midrule",
    ]
    for r in rows:
        tbl_lines.append(
            f"{r['Modality']} & {r['Variable']} & {r['n']} & "
            f"{r['Pearson r (raw)']} & {r['Spearman rho (raw)']} & "
            f"{r['Pearson r (within-month)']} & {r['p (within-month)']} \\\\"
        )
    tbl_lines += [r"\bottomrule", r"\end{tabular}"]
    tex_out = TBL_DIR / "weather_sensitivity_correlations.tex"
    tex_out.write_text("\n".join(tbl_lines) + "\n")
    print(f"wrote {tex_out}")


if __name__ == "__main__":
    main()
