#!/usr/bin/env python3
"""Generate study-area overview map: 4 US East Coast offshore wind farms.

Output: /mnt/d/thesis/main/figures/data_stats/study_area_overview.{pdf,png}

Pulls turbine coordinates from `wind_turbines` and a coastline polygon from
`ne_land` (Natural Earth), draws each farm as a labelled cluster.
"""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
from shapely import wkb

from pipeline_common import DB_CONFIG

OUT_DIR = Path("/mnt/d/thesis/main/figures/data_stats")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Plot extent — covers all four farms with a comfortable margin
LON_MIN, LON_MAX = -71.85, -69.95
LAT_MIN, LAT_MAX =  40.85,  41.45

FARM_COLOURS = {
    "Block_Island":    "#1f77b4",
    "South_Fork":      "#ff7f0e",
    "Revolution_Wind": "#2ca02c",
    "Vineyard_Wind":   "#d62728",
}
FARM_LABELS = {
    "Block_Island":    "Block Island",
    "South_Fork":      "South Fork",
    "Revolution_Wind": "Revolution Wind",
    "Vineyard_Wind":   "Vineyard Wind",
}
# Manual label-anchor offsets (deg) so labels don't overlap the cluster
LABEL_OFFSETS = {
    "Block_Island":    (-0.18, -0.03),
    "South_Fork":      ( 0.18, -0.04),
    "Revolution_Wind": (-0.05,  0.10),
    "Vineyard_Wind":   ( 0.00, -0.13),
}


def main() -> None:
    conn = psycopg2.connect(**DB_CONFIG)

    turbines = pd.read_sql(
        "SELECT project_name, latitude, longitude FROM wind_turbines",
        conn,
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ST_AsBinary(geometry) FROM ne_land
            WHERE geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
            """,
            (LON_MIN - 0.5, LAT_MIN - 0.5, LON_MAX + 0.5, LAT_MAX + 0.5),
        )
        land_polys = [wkb.loads(bytes(row[0])) for row in cur.fetchall()]
    conn.close()

    land = gpd.GeoSeries(land_polys, crs="EPSG:4326")

    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(7.0, 4.8))

    land.plot(ax=ax, color="#e8e4d8", edgecolor="#888888", linewidth=0.6, zorder=1)

    for project, sub in turbines.groupby("project_name"):
        ax.scatter(
            sub["longitude"], sub["latitude"],
            c=FARM_COLOURS.get(project, "#555555"),
            s=22, edgecolors="white", linewidth=0.4,
            label=f"{FARM_LABELS.get(project, project)} ({len(sub)})",
            zorder=3,
        )
        cx = sub["longitude"].mean()
        cy = sub["latitude"].mean()
        dx, dy = LABEL_OFFSETS.get(project, (0.05, 0.05))
        ax.annotate(
            FARM_LABELS.get(project, project),
            xy=(cx, cy), xytext=(cx + dx, cy + dy),
            fontsize=9, fontweight="bold",
            color=FARM_COLOURS.get(project, "#000"),
            arrowprops=dict(arrowstyle="-", color="#666", lw=0.5),
            zorder=4,
        )

    ax.set_xlim(LON_MIN, LON_MAX)
    ax.set_ylim(LAT_MIN, LAT_MAX)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("US East Coast offshore wind farms — study area")
    ax.set_aspect("equal", adjustable="box")
    ax.legend(loc="upper right", fontsize=8, framealpha=0.92)

    fig.tight_layout()
    fig.savefig(OUT_DIR / "study_area_overview.pdf", bbox_inches="tight")
    fig.savefig(OUT_DIR / "study_area_overview.png",
                bbox_inches="tight", dpi=160)
    plt.close(fig)

    print(f"Wrote {OUT_DIR / 'study_area_overview.pdf'}")
    print(f"Wrote {OUT_DIR / 'study_area_overview.png'}")


if __name__ == "__main__":
    main()
