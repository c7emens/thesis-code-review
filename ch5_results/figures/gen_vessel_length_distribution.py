#!/usr/bin/env python3
"""Length distribution of vessels with at least one above-threshold maintenance
event in the 2024 calibration year.

Produces:
  main/figures/data_stats/vessel_length_distribution.{pdf,png}

Reproduces the per-vessel length histogram split into two cohorts:
  - CTV-class AIS type codes (HSC, cargo, tug, other, passenger, etc.)
  - Other AIS type codes (recreational craft, small fishing, unclassified)

A vertical guide marks the 65 ft = 19.8 m threshold of 33 CFR § 164.46
above which commercial vessels in US navigable waters must broadcast
Class A AIS.

For each distinct vessel (by mms_id) the maximum non-null broadcast
vessel_length is used; this filters AIS "unknown" sentinel values
(typically 0 or 1023) and noise from intermittent zero-length frames.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import psycopg2

# Config

DB = dict(host="localhost", port=5432, dbname="windfarm",
          user="thesis", password="thesis2026")

FIG_DIR = Path("/mnt/d/thesis/main/figures/data_stats")
FIG_DIR.mkdir(parents=True, exist_ok=True)

# CTV-class AIS type codes — per Section 5.2.3 / table:vessel_types prose:
#   40, 49 (HSC) | 70 (cargo) | 52 (tug) | 90 (other) | 60 (passenger) |
#   80, 31, 33 (long tail)
CTV_CLASS_TYPES = {40, 49, 70, 52, 90, 60, 80, 31, 33}

# 33 CFR § 164.46: vessels >= 65 ft = 19.8 m must broadcast Class A AIS.
LENGTH_65FT_M = 19.8

# AIS "unknown length" sentinel values to filter.
SENTINEL_LENGTHS = {0, 1023}


# Query

def fetch_vessel_lengths() -> list[tuple[str, int | None, float]]:
    """Return [(mms_id, vessel_type, max_length_m), ...] for distinct vessels
    with at least one above-threshold event in 2024 calibration year."""
    sql = """
    WITH eligible AS (
        SELECT DISTINCT mms_id
        FROM stage3_vessel_events
        WHERE tier IN (1, 2)
          AND visit_start >= '2024-01-01'
          AND visit_start <  '2025-01-01'
    )
    SELECT v.mms_id,
           MAX(v.vessel_type)   AS vessel_type,
           MAX(v.vessel_length) AS max_length
    FROM vessel_data_ais v
    JOIN eligible e ON v.mms_id = e.mms_id
    WHERE v.vessel_length IS NOT NULL
      AND v.vessel_length > 0
      AND v.vessel_length < 1023
      AND v.time >= '2024-01-01'
      AND v.time <  '2025-01-01'
    GROUP BY v.mms_id
    """
    with psycopg2.connect(**DB) as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


# Plot

def plot_distribution(rows: list[tuple[str, int | None, float]],
                       out_stem: str = "vessel_length_distribution") -> None:
    ctv_lengths   = [L for _, t, L in rows if t in CTV_CLASS_TYPES]
    other_lengths = [L for _, t, L in rows if t not in CTV_CLASS_TYPES]
    all_lengths   = [L for _, _, L in rows]

    n_total     = len(all_lengths)
    n_ctv       = len(ctv_lengths)
    n_other     = len(other_lengths)
    n_ctv_ge65  = sum(1 for L in ctv_lengths if L >= LENGTH_65FT_M)
    pct_ctv_ge65 = n_ctv_ge65 / n_ctv * 100 if n_ctv else 0.0
    median_ctv  = float(np.median(ctv_lengths)) if ctv_lengths else 0.0

    print(f"Total distinct vessels with valid length: {n_total}")
    print(f"  CTV-class AIS types:        {n_ctv:>4}  "
          f"median {median_ctv:.1f} m   {pct_ctv_ge65:.1f}% >= 65 ft")
    print(f"  Other AIS types:            {n_other:>4}")

    # Length bins: 0–100 m in 5 m bins covers the useful range
    bins = np.arange(0, 105, 5)

    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.hist([ctv_lengths, other_lengths], bins=bins,
            stacked=True, color=["#1f77b4", "#bbbbbb"], edgecolor="black",
            linewidth=0.4,
            label=[f"CTV-class AIS types  ($n = {n_ctv}$)",
                   f"Other AIS types  ($n = {n_other}$)"])

    ax.axvline(LENGTH_65FT_M, color="#d62728", linestyle="--", linewidth=1.5,
               label=f"65 ft = {LENGTH_65FT_M:.1f} m (Class A AIS threshold)")

    ax.set_xlabel("Vessel length (m)", fontsize=11)
    ax.set_ylabel("Number of distinct vessels", fontsize=11)
    ax.set_title("Length distribution of vessels with maintenance events, "
                 "2024 calibration year", fontsize=12)
    ax.tick_params(labelsize=10)
    ax.grid(axis="y", linestyle=":", alpha=0.5)
    ax.set_xlim(0, 100)

    # Annotate the headline finding inside the plot.
    ax.text(0.97, 0.55,
            f"CTV-class vessels $\\geq 65$ ft:\n"
            f"$\\mathbf{{{pct_ctv_ge65:.1f}\\%}}$  "
            f"({n_ctv_ge65}/{n_ctv})\n"
            f"median length: ${median_ctv:.1f}$ m",
            transform=ax.transAxes, ha="right", va="top", fontsize=10,
            bbox=dict(facecolor="white", alpha=0.9, edgecolor="grey",
                      boxstyle="round,pad=0.4"))

    ax.legend(loc="upper right", framealpha=0.95, fontsize=10)
    plt.tight_layout()
    for ext in ("pdf", "png"):
        out = FIG_DIR / f"{out_stem}.{ext}"
        fig.savefig(out, dpi=160, bbox_inches="tight")
        print(f"wrote {out}")
    plt.close(fig)


# Main

def main():
    rows = fetch_vessel_lengths()
    plot_distribution(rows)


if __name__ == "__main__":
    main()
