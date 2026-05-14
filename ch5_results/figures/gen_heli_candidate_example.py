#!/usr/bin/env python3
"""Static example of a Stage 2 helicopter track used for manual validation.

Reproduces the kind of inspection an analyst performs against the industry
\\emph{Master Flight Report}: pick one high-confidence Stage 3 candidate, plot
its full ADS-B track over the wind-farm turbine cluster, colour by altitude,
highlight low-and-slow points (the maintenance signature), and annotate
arrival/departure times so the trajectory can be cross-checked against the
operator's manifest.

Output: /mnt/d/thesis/main/figures/eda/heli_candidate_example.{pdf,png}

Default candidate: icao24=a92f2d at South Fork on 2024-04-27 (score 99.7).
"""
from __future__ import annotations
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import psycopg2

from pipeline_common import DB_CONFIG

OUT_DIR = Path("/mnt/d/thesis/main/figures/eda")
OUT_DIR.mkdir(parents=True, exist_ok=True)

ICAO  = "a92f2d"
FDATE = "2024-04-27"
PROJECT = "South_Fork"

# Hover-signature thresholds (matches the helicopter Stage 3 saturation denominators)
HOVER_ALT_M    = 100  # m AGL — typical SOV-deck approach
HOVER_VEL_MS   = 10   # m/s — typical hover or slow approach


def main() -> None:
    conn = psycopg2.connect(**DB_CONFIG)
    track = pd.read_sql(
        """
        SELECT time_utc, lat, lon, baro_alt_m, velocity_ms
        FROM stage2_helicopter_tracks
        WHERE icao24 = %s AND flight_date = %s
          AND lat IS NOT NULL AND lon IS NOT NULL
        ORDER BY time_utc
        """,
        conn, params=(ICAO, FDATE),
    )
    turbines = pd.read_sql(
        "SELECT latitude, longitude FROM wind_turbines WHERE project_name = %s",
        conn, params=(PROJECT,),
    )
    conn.close()

    # Filter erroneous negative-altitude readings (ADS-B baro can underflow)
    track = track[track["baro_alt_m"].fillna(0) >= 0].reset_index(drop=True)

    # Tag the hover signature
    track["is_hover"] = (
        (track["baro_alt_m"].fillna(9999) <= HOVER_ALT_M) &
        (track["velocity_ms"].fillna(9999) <= HOVER_VEL_MS)
    )

    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(8.0, 5.4))

    # Track scatter coloured by altitude
    sc = ax.scatter(track["lon"], track["lat"], c=track["baro_alt_m"],
                    cmap="viridis_r", s=8, alpha=0.85, zorder=2,
                    label="ADS-B fix")
    cbar = fig.colorbar(sc, ax=ax, pad=0.015, fraction=0.04)
    cbar.set_label("Barometric altitude (m)", fontsize=9)

    # Highlight hover signatures
    hov = track[track["is_hover"]]
    ax.scatter(hov["lon"], hov["lat"], facecolors="none", edgecolors="#d62728",
               s=70, linewidths=1.0, zorder=3,
               label=f"Hover signature (alt $\\leq$ {HOVER_ALT_M} m, "
                     f"v $\\leq$ {HOVER_VEL_MS} m/s)")

    # Turbines
    ax.scatter(turbines["longitude"], turbines["latitude"],
               marker="^", color="#1f77b4", s=85, edgecolor="white",
               linewidth=0.6, zorder=4,
               label=f"{PROJECT.replace('_', ' ')} turbines (n={len(turbines)})")

    # Annotate first / last fix with timestamps; only show first if widely separated
    if len(track) >= 2:
        first, last = track.iloc[0], track.iloc[-1]
        ax.annotate(f"first fix {first['time_utc']:%H:%M UTC}",
                    xy=(first["lon"], first["lat"]),
                    xytext=(first["lon"] - 0.10, first["lat"] + 0.02),
                    fontsize=8, color="#444",
                    arrowprops=dict(arrowstyle="-", color="#888", lw=0.5))
        ax.annotate(f"last fix {last['time_utc']:%H:%M UTC}",
                    xy=(last["lon"], last["lat"]),
                    xytext=(last["lon"] + 0.02, last["lat"] - 0.05),
                    fontsize=8, color="#444",
                    arrowprops=dict(arrowstyle="-", color="#888", lw=0.5))

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(
        f"Helicopter candidate {ICAO} — {FDATE} — {PROJECT.replace('_', ' ')}\n"
        f"({len(track)} ADS-B fixes, {int(hov.shape[0])} hover-signature; "
        "Stage 3 score = 99.7)",
        fontsize=10,
    )
    ax.legend(loc="lower left", fontsize=8, framealpha=0.92)
    ax.set_aspect("equal", adjustable="box")

    fig.tight_layout()
    fig.savefig(OUT_DIR / "heli_candidate_example.pdf", bbox_inches="tight")
    fig.savefig(OUT_DIR / "heli_candidate_example.png",
                bbox_inches="tight", dpi=160)
    plt.close(fig)
    print(f"Wrote {OUT_DIR / 'heli_candidate_example.pdf'}")
    print(f"Track: {len(track)} fixes, {hov.shape[0]} hover-signature, "
          f"alt {track['baro_alt_m'].min():.0f}-{track['baro_alt_m'].max():.0f} m")


if __name__ == "__main__":
    main()
