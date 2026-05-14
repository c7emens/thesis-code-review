#!/usr/bin/env python3
"""
Generate the O&M detection pipeline diagram for the thesis.

Output: /mnt/d/thesis/presentation/pipeline_diagram.pdf  (and .png)

Layout
------
Left column  : Stage 1 → 2 → 3 pipeline flow (vessel + helicopter)
Right column : Stage 3 classification decision tree
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
import matplotlib.patheffects as pe
from pathlib import Path

OUT_DIR = Path("/mnt/d/thesis/presentation")

# Colour palette
C_STAGE   = "#1e40af"   # dark blue  — pipeline stage boxes
C_DATA    = "#0f766e"   # teal       — data source boxes
C_T1      = "#15803d"   # green      — Tier 1 / sure
C_T2      = "#b45309"   # amber      — Tier 2 / likely
C_SOV     = "#6d28d9"   # purple     — support station
C_DISCARD = "#6b7280"   # grey       — discard / transit
C_DIAMOND = "#374151"   # dark grey  — decision diamonds
C_ARROW   = "#374151"
C_BG      = "#f8fafc"

FONT = "DejaVu Sans"

# Figure setup
fig = plt.figure(figsize=(18, 13), facecolor="white")
fig.text(0.5, 0.965, "Offshore Wind O&M Activity Detection Pipeline",
         ha="center", va="top", fontsize=16, fontweight="bold", fontfamily=FONT,
         color="#111827")

# Two axes side-by-side
ax_pipe = fig.add_axes([0.01, 0.02, 0.42, 0.91])   # left: pipeline
ax_cls  = fig.add_axes([0.46, 0.02, 0.53, 0.91])   # right: classification

for ax in (ax_pipe, ax_cls):
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.set_facecolor(C_BG)


# Helper: draw a rounded box with text
def box(ax, x, y, w, h, text, color, fontsize=9, textcolor="white",
        bold=False, sub=None, style="round,pad=0.02"):
    patch = FancyBboxPatch((x - w/2, y - h/2), w, h,
                           boxstyle=style,
                           linewidth=1.2, edgecolor="white",
                           facecolor=color, zorder=3)
    ax.add_patch(patch)
    weight = "bold" if bold else "normal"
    if sub:
        ax.text(x, y + h * 0.12, text, ha="center", va="center",
                fontsize=fontsize, fontfamily=FONT,
                fontweight=weight, color=textcolor, zorder=4)
        ax.text(x, y - h * 0.22, sub, ha="center", va="center",
                fontsize=fontsize - 1.5, fontfamily=FONT, color=textcolor,
                alpha=0.85, zorder=4, style="italic")
    else:
        ax.text(x, y, text, ha="center", va="center",
                fontsize=fontsize, fontfamily=FONT,
                fontweight=weight, color=textcolor, zorder=4)


def diamond(ax, x, y, w, h, text, color=C_DIAMOND, fontsize=8.5):
    """Draw a decision diamond."""
    pts = [(x, y + h/2), (x + w/2, y), (x, y - h/2), (x - w/2, y)]
    patch = plt.Polygon(pts, closed=True, facecolor=color,
                        edgecolor="white", linewidth=1.2, zorder=3)
    ax.add_patch(patch)
    ax.text(x, y, text, ha="center", va="center", fontsize=fontsize,
            fontfamily=FONT, color="white", fontweight="bold", zorder=4,
            multialignment="center")


def arrow(ax, x0, y0, x1, y1, label=None, label_side="right", color=C_ARROW):
    ax.annotate("", xy=(x1, y1), xytext=(x0, y0),
                arrowprops=dict(arrowstyle="-|>", color=color,
                                lw=1.4, mutation_scale=12),
                zorder=2)
    if label:
        mx, my = (x0 + x1) / 2, (y0 + y1) / 2
        offset = 0.04 if label_side == "right" else -0.04
        ax.text(mx + offset, my, label, ha="center", va="center",
                fontsize=7.5, fontfamily=FONT, color="#374151",
                style="italic", zorder=5,
                bbox=dict(facecolor="white", edgecolor="none", alpha=0.7, pad=1))


def line(ax, x0, y0, x1, y1, color=C_ARROW, lw=1.2, style="-"):
    ax.plot([x0, x1], [y0, y1], color=color, lw=lw, ls=style, zorder=2)


# ══════════════════════════════════════════════════════════════════════════════
# LEFT PANEL — Pipeline flow
# ══════════════════════════════════════════════════════════════════════════════
ax = ax_pipe
ax.text(0.5, 0.97, "Pipeline Stages", ha="center", va="top",
        fontsize=11, fontweight="bold", fontfamily=FONT, color="#1e3a5f")

# Data sources
box(ax, 0.27, 0.87, 0.40, 0.072, "AIS vessel tracks",
    C_DATA, sub="TimescaleDB · NOAA AIS")
box(ax, 0.73, 0.87, 0.40, 0.072, "ADS-B flight tracks",
    C_DATA, sub="OpenSky Network · Trino")

# join arrows
arrow(ax, 0.27, 0.833, 0.27, 0.795)
arrow(ax, 0.73, 0.833, 0.73, 0.795)
line(ax,  0.27, 0.795, 0.50, 0.795)
line(ax,  0.73, 0.795, 0.50, 0.795)
arrow(ax, 0.50, 0.795, 0.50, 0.760)

# Stage 1
box(ax, 0.50, 0.718, 0.72, 0.074,
    "Stage 1 — Tripwire / Proximity Scan",
    C_STAGE, bold=True, fontsize=9.5,
    sub="For each (vessel/flight, date): was it within 2 km of any turbine?")

arrow(ax, 0.50, 0.681, 0.50, 0.645,
      label="candidate (ID, date) pairs")

# Stage 2
box(ax, 0.50, 0.605, 0.72, 0.074,
    "Stage 2 — Full Track Retrieval",
    C_STAGE, bold=True, fontsize=9.5,
    sub="Fetch complete trajectory  ±48 h before / 36 h after visit date")

arrow(ax, 0.50, 0.568, 0.50, 0.530,
      label="position track (lat, lon, time, SOG)")

# Outlier / gap annotation
ax.text(0.97, 0.548, "outlier\nremoval\n(stage 2b)", ha="right", va="center",
        fontsize=7, fontfamily=FONT, color="#6b7280", style="italic")

# Stage 3
box(ax, 0.50, 0.488, 0.72, 0.074,
    "Stage 3 — Visit Classification",
    C_STAGE, bold=True, fontsize=9.5,
    sub="Group positions into visit segments → classify each segment")

arrow(ax, 0.50, 0.451, 0.50, 0.410)

# Outputs
box(ax, 0.27, 0.365, 0.40, 0.072,
    "maintenance_visit",
    C_T1, sub="CTV gangway transfer")
box(ax, 0.73, 0.365, 0.40, 0.072,
    "support_station",
    C_SOV, sub="SOV / heavy-lift DP-anchor")

line(ax, 0.50, 0.410, 0.27, 0.410)
line(ax, 0.50, 0.410, 0.73, 0.410)
arrow(ax, 0.27, 0.410, 0.27, 0.401)
arrow(ax, 0.73, 0.410, 0.73, 0.401)

# Tiers under maintenance_visit
box(ax, 0.18, 0.270, 0.25, 0.062, "Tier 1 — SURE",   C_T1,  fontsize=8.5,
    sub="all 4 flags")
box(ax, 0.47, 0.270, 0.25, 0.062, "Tier 2 — LIKELY", C_T2,  fontsize=8.5,
    sub="partial flags")

arrow(ax, 0.27, 0.329, 0.18, 0.301)
arrow(ax, 0.27, 0.329, 0.47, 0.301)

# Discard
box(ax, 0.50, 0.175, 0.45, 0.058, "Discard (transit / too fast / too far)",
    C_DISCARD, fontsize=8)
ax.text(0.50, 0.133, "→  stored in stage3_vessel_events (PostgreSQL) + CSV",
        ha="center", va="center", fontsize=8, fontfamily=FONT, color="#374151",
        style="italic")

# Flags legend (bottom left)
ax.text(0.04, 0.09, "Tier 1 flags:", ha="left", va="top",
        fontsize=8, fontfamily=FONT, color="#1e3a5f", fontweight="bold")
flags = [
    ("flag_proximity",   "min dist ≤ 100 m"),
    ("flag_sog",         "min SOG ≤ 0.5 kt"),
    ("flag_continuity",  "max AIS gap ≤ 15 min"),
    ("flag_duration",    "stay ≥ 10 min"),
]
for i, (name, desc) in enumerate(flags):
    ax.text(0.06, 0.072 - i * 0.016, f"• {name}: {desc}",
            ha="left", va="top", fontsize=7.5, fontfamily=FONT, color="#374151")


# ══════════════════════════════════════════════════════════════════════════════
# RIGHT PANEL — Stage 3 classification decision tree
# ══════════════════════════════════════════════════════════════════════════════
ax = ax_cls
ax.text(0.5, 0.97, "Stage 3 — Classification Decision Tree",
        ha="center", va="top",
        fontsize=11, fontweight="bold", fontfamily=FONT, color="#1e3a5f")

# Visit segment input
box(ax, 0.5, 0.895, 0.55, 0.060,
    "Visit segment  (positions grouped by 30-min gap)",
    "#374151", fontsize=9)

arrow(ax, 0.5, 0.865, 0.5, 0.833)

# D1: Vessel category
diamond(ax, 0.5, 0.793, 0.52, 0.070,
        "AIS type = 90\n(Support vessel)?", C_DIAMOND)

# YES → right branch (support station)
arrow(ax, 0.76, 0.793, 0.88, 0.793, color=C_SOV)
ax.text(0.812, 0.808, "Yes", ha="center", va="bottom",
        fontsize=8, fontfamily=FONT, color=C_SOV, fontweight="bold")

# NO → down
arrow(ax, 0.5, 0.758, 0.5, 0.718, color=C_T1)
ax.text(0.514, 0.738, "No", ha="left", va="center",
        fontsize=8, fontfamily=FONT, color=C_T1, fontweight="bold")

# Support station branch
diamond(ax, 0.88, 0.700, 0.22, 0.068,
        "SOG ≤ 0.15 kt\nAND dist ≤ 500 m?", C_DIAMOND)
arrow(ax, 0.88, 0.793, 0.88, 0.734, color=C_SOV)

# SOV: No → discard
arrow(ax, 0.99, 0.700, 0.99, 0.620, color=C_DISCARD)
ax.text(1.0, 0.660, "No →\nDiscard", ha="right", va="center",
        fontsize=7.5, fontfamily=FONT, color=C_DISCARD)

# SOV: Yes → turbine attribution check
arrow(ax, 0.88, 0.666, 0.88, 0.598, color=C_SOV)
ax.text(0.894, 0.632, "Yes", ha="left", va="center",
        fontsize=8, fontfamily=FONT, color=C_SOV, fontweight="bold")

diamond(ax, 0.88, 0.555, 0.22, 0.068,
        "Within 100 m of\nspecific turbines?", C_DIAMOND)

# per-turbine
arrow(ax, 0.88, 0.521, 0.88, 0.460, color=C_SOV)
ax.text(0.894, 0.490, "Yes", ha="left", va="center",
        fontsize=8, fontfamily=FONT, color=C_SOV, fontweight="bold")
box(ax, 0.88, 0.420, 0.22, 0.060,
    "support_station\n(per-turbine)",
    C_SOV, fontsize=8.5)

# farm-level
arrow(ax, 0.77, 0.555, 0.695, 0.555, color=C_SOV)
ax.text(0.732, 0.568, "No", ha="center", va="bottom",
        fontsize=8, fontfamily=FONT, color=C_SOV, fontweight="bold")
box(ax, 0.60, 0.555, 0.18, 0.060,
    "support_station\n(farm-level)",
    C_SOV, fontsize=8.5)

# Tier label
arrow(ax, 0.88, 0.390, 0.88, 0.340, color=C_SOV)
ax.text(0.88, 0.315, "Tier 1: stay ≥ 12 h + continuous\nTier 2: otherwise",
        ha="center", va="top", fontsize=7.5, fontfamily=FONT, color=C_SOV,
        style="italic", multialignment="center")

# CTV branch
# D2: all 4 Tier-1 flags?
diamond(ax, 0.5, 0.675, 0.52, 0.070,
        "SOG ≤ 0.5 kt AND dist ≤ 100 m\nAND gap ≤ 15 min AND dur ≥ 10 min?",
        C_DIAMOND, fontsize=8)

# YES → Tier 1
arrow(ax, 0.5, 0.640, 0.5, 0.580, color=C_T1)
ax.text(0.514, 0.610, "Yes", ha="left", va="center",
        fontsize=8, fontfamily=FONT, color=C_T1, fontweight="bold")

box(ax, 0.5, 0.543, 0.42, 0.062,
    "maintenance_visit  Tier 1 — SURE",
    C_T1, bold=True, fontsize=9,
    sub="turbine-attributed (all ≤100m turbines)")

# Multi-turbine note
ax.text(0.5, 0.490, "↓  find ALL turbines within 100 m of any\n"
        "    position in segment → one event per turbine",
        ha="center", va="top", fontsize=7.8, fontfamily=FONT,
        color=C_T1, style="italic", multialignment="center")

# NO → D3
arrow(ax, 0.5, 0.640, 0.5, 0.605, color=C_T2)  # just a connector
arrow(ax, 0.24, 0.675, 0.05, 0.675, color=C_T2)
ax.text(0.14, 0.688, "No", ha="center", va="bottom",
        fontsize=8, fontfamily=FONT, color=C_T2, fontweight="bold")
arrow(ax, 0.05, 0.675, 0.05, 0.595, color=C_T2)

diamond(ax, 0.05, 0.555, 0.32, 0.070,
        "SOG ≤ 2 kt  OR\ngap > 15 min  OR  dist 100–2000 m?",
        C_T2, fontsize=8)

# YES → Tier 2
arrow(ax, 0.05, 0.520, 0.05, 0.448, color=C_T2)
ax.text(0.063, 0.484, "Yes", ha="left", va="center",
        fontsize=8, fontfamily=FONT, color=C_T2, fontweight="bold")
box(ax, 0.05, 0.410, 0.32, 0.062,
    "maintenance_visit  Tier 2 — LIKELY",
    C_T2, bold=True, fontsize=8.5,
    sub="turbine unattributed (turbine_code = null)")

# NO → Discard
arrow(ax, 0.05, 0.379, 0.05, 0.305, color=C_DISCARD)
ax.text(0.063, 0.342, "No", ha="left", va="center",
        fontsize=8, fontfamily=FONT, color=C_DISCARD, fontweight="bold")
box(ax, 0.05, 0.270, 0.32, 0.055,
    "Discard  (transit / no maintenance signal)",
    C_DISCARD, fontsize=8)

# Radius reference diagram
import numpy as np
ax_r = fig.add_axes([0.52, 0.04, 0.18, 0.20])
ax_r.set_aspect("equal")
ax_r.set_xlim(-2500, 2500)
ax_r.set_ylim(-2500, 2500)
ax_r.axis("off")
ax_r.set_facecolor("#f1f5f9")

turbine_marker = ax_r.scatter([0], [0], s=120, marker="^",
                              color="#1e3a5f", zorder=5)
for r, color, lbl, ls in [
    (100,  C_T1,  "100 m  (Tier 1 radius)", "-"),
    (2000, C_T2,  "2 000 m  (search radius)", "--"),
]:
    theta = np.linspace(0, 2 * np.pi, 200)
    ax_r.plot(r * np.cos(theta), r * np.sin(theta), color=color, lw=1.4, ls=ls)
    ax_r.text(r * 0.71, r * 0.71 + 80, lbl, fontsize=6.5,
              fontfamily=FONT, color=color, ha="center")

ax_r.set_title("Spatial radii", fontsize=8, fontfamily=FONT, color="#374151", pad=4)

# Save
OUT_DIR.mkdir(parents=True, exist_ok=True)
fig.savefig(OUT_DIR / "pipeline_diagram.pdf", bbox_inches="tight", dpi=150)
fig.savefig(OUT_DIR / "pipeline_diagram.png", bbox_inches="tight", dpi=180,
            facecolor="white")
print(f"Saved to {OUT_DIR}/pipeline_diagram.pdf  and  .png")
