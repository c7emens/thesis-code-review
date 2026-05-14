"""Descriptive statistics for all thesis data sources.

Renders both an interactive HTML overview (Plotly) and LaTeX-ready exports
(matplotlib PNG/PDF + CSV/TEX summary tables). Domains: vessel AIS, helicopter
ADS-B, ports, airports, weather, wind turbines.

Heavy aggregations are cached in .data_stats_cache.json — re-runs are fast.

Usage:
    python generate_data_stats.py                    # cold/warm full run
    python generate_data_stats.py --no-cache         # full re-run
    python generate_data_stats.py --refresh weather  # invalidate one domain
    python generate_data_stats.py --domain vessel    # render only one domain
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import psycopg2


# Configuration

DB = dict(host="localhost", port=5432, dbname="windfarm",
          user="thesis", password="thesis2026")

OURAIRPORTS_CSV = Path("/mnt/e/data_lake/ourairports_airports.csv")

OUT_HTML  = Path("/mnt/d/thesis/data_statistics.html")
FIG_DIR   = Path("/mnt/d/thesis/main/figures/data_stats")
TBL_DIR   = Path("/mnt/d/thesis/main/tables/data_stats")
TEX_INDEX = Path("/mnt/d/thesis/main/data_statistics.tex")
CACHE     = Path("/mnt/d/thesis/scripts/.data_stats_cache.json")

PRIMARY = "#2c5f8d"
ACCENT  = "#e07b00"
MUTED   = "#9aa5b1"
PALETTE = ["#2c5f8d", "#e07b00", "#5a9367", "#8b3a3a", "#6b4f8a",
           "#c9a227", "#3b8686", "#a04668", "#577590"]

# US Northeast bounding box for airport counts (covers MA, RI, CT, NY, NJ)
US_NE_BBOX = dict(lat_min=38.0, lat_max=43.0, lon_min=-76.0, lon_max=-69.0)

# Weather columns surfaced in the completeness chart (skip flag/indicator columns)
WEATHER_NUMERIC_COLUMNS = [
    "wind_direction", "wind_speed", "visibility",
    "sea_level_pressure", "air_temp", "wet_bulb_temp", "dew_point_temp",
    "sea_surface_temp", "total_cloud_amount", "low_cloud_amount", "cloud_height",
    "ship_speed", "ship_course", "elevation",
]

DOMAINS = ["vessel", "heli", "ports", "airports", "weather", "turbines"]


# Infrastructure

def conn():
    return psycopg2.connect(**DB)


def _load_cache() -> dict:
    if not CACHE.exists():
        return {"version": 1, "results": {}}
    try:
        return json.loads(CACHE.read_text())
    except Exception:
        return {"version": 1, "results": {}}


def _save_cache(cache: dict) -> None:
    cache["generated_at"] = datetime.now(timezone.utc).isoformat()
    tmp = CACHE.with_suffix(".tmp")
    # default=str converts date / datetime / Decimal to strings
    tmp.write_text(json.dumps(cache, default=str))
    os.replace(tmp, CACHE)


_CACHE_STATE = {"data": None, "no_cache": False, "refresh_prefixes": ()}


def cached(key: str, fetcher) -> pd.DataFrame:
    """Run fetcher() returning a DataFrame; cache in .data_stats_cache.json.

    Fetcher signature: `fetcher(cn) -> DataFrame`. cached() opens and closes a
    FRESH connection per fetch. This prevents postgres backend memory from
    accumulating across heavy aggregations (the OOM-killer cause we hit
    earlier when one connection ran the 86-min vessel_overview followed by a
    second large aggregation on the same backend).
    """
    if _CACHE_STATE["data"] is None:
        _CACHE_STATE["data"] = _load_cache()
    cache = _CACHE_STATE["data"]
    refresh = (_CACHE_STATE["no_cache"]
               or any(key.startswith(p) for p in _CACHE_STATE["refresh_prefixes"]))
    if not refresh and key in cache["results"]:
        rec = cache["results"][key]
        return pd.DataFrame(rec["rows"], columns=rec["columns"])
    t0 = time.monotonic()
    cn = conn()
    try:
        with cn.cursor() as cur:
            cur.execute("SET work_mem = '512MB'")
            cur.execute("SET statement_timeout = 0")
        df = fetcher(cn)
    finally:
        cn.close()
    elapsed = time.monotonic() - t0
    print(f"  [fetch] {key:38s}  rows={len(df):>7,}  ({elapsed:5.1f}s)")
    cache["results"][key] = {
        "columns": list(df.columns),
        "rows":    df.values.tolist(),
    }
    _save_cache(cache)
    return df


def save_table(df: pd.DataFrame, name: str, caption: str, label: str) -> Path:
    """Write `<name>.csv` and `<name>.tex` under TBL_DIR. Returns the .tex path."""
    TBL_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = TBL_DIR / f"{name}.csv"
    tex_path = TBL_DIR / f"{name}.tex"
    df.to_csv(csv_path, index=False)
    body = df.to_latex(index=False, escape=True, float_format="%.2f",
                       na_rep="—")
    tex_path.write_text(
        f"\\begin{{table}}[htbp]\n\\centering\n\\caption{{{caption}}}\n"
        f"\\label{{{label}}}\n{body}\\end{{table}}\n"
    )
    return tex_path


def save_fig_dual(name: str, plotly_fig, mpl_fig) -> tuple[str, Path]:
    """Save matplotlib fig as PNG+PDF; return (plotly_html, pdf_path)."""
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    png_path = FIG_DIR / f"{name}.png"
    pdf_path = FIG_DIR / f"{name}.pdf"
    mpl_fig.tight_layout()
    mpl_fig.savefig(png_path, dpi=300, bbox_inches="tight")
    mpl_fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(mpl_fig)
    plotly_fig.update_layout(
        height=380, margin=dict(l=40, r=20, t=50, b=40),
        font=dict(family="Inter, system-ui, sans-serif", size=12),
        plot_bgcolor="#fafafa", paper_bgcolor="white",
    )
    html = pio.to_html(plotly_fig, include_plotlyjs=False, full_html=False,
                       config={"displayModeBar": False})
    return html, pdf_path


def _mpl_setup():
    plt.style.use("seaborn-v0_8-whitegrid")
    plt.rcParams.update({
        "figure.figsize": (6, 3.7),
        "axes.spines.top": False,
        "axes.spines.right": False,
        "font.size": 10,
        "axes.titlesize": 11,
    })


# Chart helpers

def _fig_bar(df, x, y, *, orientation="v", title="", x_label="", y_label="",
             color=PRIMARY, log_y=False):
    plotly = go.Figure(go.Bar(
        x=df[x] if orientation == "v" else df[y],
        y=df[y] if orientation == "v" else df[x],
        orientation=orientation, marker_color=color,
    ))
    plotly.update_layout(title=title, xaxis_title=x_label, yaxis_title=y_label)
    if log_y:
        plotly.update_yaxes(type="log")
    if orientation == "h":
        plotly.update_yaxes(autorange="reversed")

    mpl, ax = plt.subplots()
    if orientation == "v":
        ax.bar(df[x].astype(str), df[y], color=color)
        ax.set_xlabel(x_label); ax.set_ylabel(y_label)
        if len(df) > 8:
            ax.tick_params(axis="x", labelrotation=45)
    else:
        ax.barh(df[y].astype(str), df[x], color=color)
        ax.invert_yaxis()
        ax.set_xlabel(x_label); ax.set_ylabel(y_label)
    if log_y:
        ax.set_yscale("log")
    ax.set_title(title)
    return plotly, mpl


def _fig_donut(labels, values, *, title=""):
    plotly = go.Figure(go.Pie(labels=labels, values=values, hole=0.4,
                              marker=dict(colors=PALETTE)))
    plotly.update_layout(title=title)
    mpl, ax = plt.subplots()
    ax.pie(values, labels=labels, colors=PALETTE, autopct="%1.0f%%",
           startangle=90, wedgeprops=dict(width=0.4))
    ax.set_title(title)
    return plotly, mpl


def _fig_hist(values, *, bins=40, title="", x_label="", y_label="Count",
              color=PRIMARY):
    plotly = go.Figure(go.Histogram(x=values, nbinsx=bins, marker_color=color))
    plotly.update_layout(title=title, xaxis_title=x_label, yaxis_title=y_label)
    mpl, ax = plt.subplots()
    ax.hist(values, bins=bins, color=color, edgecolor="white", linewidth=0.4)
    ax.set_xlabel(x_label); ax.set_ylabel(y_label); ax.set_title(title)
    return plotly, mpl


def _fig_line(df, x, y, *, title="", x_label="", y_label="", color=PRIMARY):
    plotly = go.Figure(go.Scatter(x=df[x], y=df[y], mode="lines+markers",
                                  line=dict(color=color, width=1.5),
                                  marker=dict(size=4)))
    plotly.update_layout(title=title, xaxis_title=x_label, yaxis_title=y_label)
    mpl, ax = plt.subplots()
    ax.plot(df[x], df[y], color=color, linewidth=1.5, marker="o", markersize=3)
    ax.set_xlabel(x_label); ax.set_ylabel(y_label); ax.set_title(title)
    if len(df) > 8:
        ax.tick_params(axis="x", labelrotation=45)
    return plotly, mpl


# Section: Vessel AIS

def section_vessel() -> dict:
    print("\n[vessel]")

    # Summary
    overview = cached("vessel_overview", lambda cn: pd.read_sql("""
        SELECT COUNT(*) AS rows, COUNT(DISTINCT mms_id) AS mmsi,
               MIN(time)::date AS time_min, MAX(time)::date AS time_max,
               COUNT(DISTINCT vessel_type) AS types,
               COUNT(DISTINCT ais_transceiver_class) AS classes
        FROM vessel_data_ais
    """, cn))

    # SAMPLED queries below: full-table scans on the 4.58B-row compressed
    # hypertable trigger OOM (the kernel kills the postgres backend during
    # decompression of all 79 chunks). Descriptive stats use targeted samples:
    # — type/class breakdown: 1-week slice (June 2024). Composition is stable
    #   across the dataset, so a representative week is enough.
    # — monthly trends: 1-day-per-month samples (the 15th of each month).
    #   Counts are reported as approximate (multiplied by 30 to estimate
    #   monthly totals). The sampled distinct-MMSI per day is reported as-is.
    type_breakdown = cached("vessel_type_breakdown", lambda cn: pd.read_sql("""
        SELECT COALESCE(vessel_type::text, 'unknown') AS vessel_type,
               COUNT(*) AS positions
        FROM vessel_data_ais
        WHERE time >= '2024-06-01' AND time < '2024-06-08'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 15
    """, cn))

    class_breakdown = cached("vessel_class_breakdown", lambda cn: pd.read_sql("""
        SELECT COALESCE(ais_transceiver_class, 'unknown') AS ais_class,
               COUNT(*) AS positions
        FROM vessel_data_ais
        WHERE time >= '2024-06-01' AND time < '2024-06-08'
        GROUP BY 1 ORDER BY 2 DESC
    """, cn))

    monthly_positions = cached("vessel_monthly_positions", lambda cn: pd.read_sql("""
        WITH sample_days AS (
            SELECT generate_series(
                DATE_TRUNC('month', '2023-12-01'::date) + INTERVAL '14 days',
                DATE_TRUNC('month', '2025-06-01'::date) + INTERVAL '14 days',
                INTERVAL '1 month'
            )::date AS d
        )
        SELECT DATE_TRUNC('month', s.d)::date AS month,
               (SELECT COUNT(*) FROM vessel_data_ais
                WHERE time >= s.d AND time < s.d + INTERVAL '1 day') * 30
                   AS positions
        FROM sample_days s
        ORDER BY 1
    """, cn))

    monthly_active = cached("vessel_monthly_active_mmsi", lambda cn: pd.read_sql("""
        WITH sample_days AS (
            SELECT generate_series(
                DATE_TRUNC('month', '2023-12-01'::date) + INTERVAL '14 days',
                DATE_TRUNC('month', '2025-06-01'::date) + INTERVAL '14 days',
                INTERVAL '1 month'
            )::date AS d
        )
        SELECT DATE_TRUNC('month', s.d)::date AS month,
               (SELECT COUNT(DISTINCT mms_id) FROM vessel_data_ais
                WHERE time >= s.d AND time < s.d + INTERVAL '1 day')
                   AS active_mmsi
        FROM sample_days s
        ORDER BY 1
    """, cn))

    # Summary table
    o = overview.iloc[0]
    summary = pd.DataFrame([
        ("Position records",   f"{int(o['rows']):,}"),
        ("Distinct vessels",   f"{int(o['mmsi']):,}"),
        ("First observation",  str(o["time_min"])),
        ("Last observation",   str(o["time_max"])),
        ("Distinct vessel types",   f"{int(o['types']):,}"),
        ("Distinct AIS classes",    f"{int(o['classes']):,}"),
    ], columns=["Metric", "Value"])

    cards, tex_files = [], []
    tex_files.append(save_table(summary, "vessel_summary",
                                "Vessel AIS dataset summary.",
                                "tab:datastat_vessel_summary"))

    p, m = _fig_bar(type_breakdown, "vessel_type", "positions",
                    orientation="h", title="Top vessel types by position count",
                    x_label="Positions")
    h, _ = save_fig_dual("vessel_type_breakdown", p, m)
    cards.append(("Vessel type breakdown", h))
    tex_files.append(("vessel_type_breakdown",
                      "Top 15 vessel types by AIS position count "
                      "(based on a 1-week sample, 1\\textendash 7 June 2024)."))

    p, m = _fig_donut(class_breakdown["ais_class"].astype(str).tolist(),
                      class_breakdown["positions"].tolist(),
                      title="AIS transceiver class share")
    h, _ = save_fig_dual("vessel_class_breakdown", p, m)
    cards.append(("AIS class breakdown", h))
    tex_files.append(("vessel_class_breakdown",
                      "Share of AIS positions by transceiver class "
                      "(based on a 1-week sample, 1\\textendash 7 June 2024)."))

    p, m = _fig_bar(monthly_positions, "month", "positions",
                    title="Monthly AIS position count",
                    x_label="Month", y_label="Positions")
    h, _ = save_fig_dual("vessel_monthly_positions", p, m)
    cards.append(("Monthly position count", h))
    tex_files.append(("vessel_monthly_positions",
                      "Estimated monthly AIS position counts (sampled from "
                      "the 15th of each month, scaled by 30)."))

    p, m = _fig_line(monthly_active, "month", "active_mmsi",
                     title="Monthly distinct active vessels",
                     x_label="Month", y_label="Distinct MMSI")
    h, _ = save_fig_dual("vessel_monthly_active_mmsi", p, m)
    cards.append(("Active vessels per month", h))
    tex_files.append(("vessel_monthly_active_mmsi",
                      "Distinct active vessels (MMSI) observed on the 15th of "
                      "each month (single-day snapshot per month)."))

    return dict(name="Vessel AIS", anchor="datastat_vessel",
                kpi=summary, cards=cards, tex_files=tex_files)


# Section: Helicopter / ADS-B

def section_heli() -> dict:
    print("\n[heli]")

    overview = cached("heli_overview", lambda cn: pd.read_sql("""
        SELECT
          (SELECT COUNT(*) FROM stage1_helicopter_hits)             AS s1_hits,
          (SELECT COUNT(DISTINCT icao24) FROM stage1_helicopter_hits) AS s1_icao24,
          (SELECT COUNT(*) FROM stage2_helicopter_tracks)            AS s2_positions,
          (SELECT COUNT(DISTINCT icao24) FROM stage2_helicopter_tracks) AS s2_icao24,
          (SELECT COUNT(*) FROM stage3_helicopter_events)            AS s3_events,
          (SELECT COUNT(DISTINCT icao24) FROM stage3_helicopter_events) AS s3_icao24,
          (SELECT MIN(flight_date) FROM stage1_helicopter_hits)      AS d_min,
          (SELECT MAX(flight_date) FROM stage1_helicopter_hits)      AS d_max
    """, cn))

    monthly_stage1 = cached("heli_monthly_stage1", lambda cn: pd.read_sql("""
        SELECT DATE_TRUNC('month', flight_date)::date AS month, COUNT(*) AS hits
        FROM stage1_helicopter_hits GROUP BY 1 ORDER BY 1
    """, cn))

    altitudes = cached("heli_alt_distribution", lambda cn: pd.read_sql("""
        SELECT min_alt_m FROM stage1_helicopter_hits
        WHERE min_alt_m IS NOT NULL AND min_alt_m BETWEEN -100 AND 1500
    """, cn))

    per_aircraft = cached("heli_per_aircraft_top20", lambda cn: pd.read_sql("""
        SELECT icao24, COUNT(*) AS events
        FROM stage3_helicopter_events
        GROUP BY 1 ORDER BY 2 DESC LIMIT 20
    """, cn))

    per_project = cached("heli_per_project", lambda cn: pd.read_sql("""
        SELECT project_name, COUNT(*) AS events
        FROM stage3_helicopter_events
        GROUP BY 1 ORDER BY 2 DESC
    """, cn))

    o = overview.iloc[0]
    summary = pd.DataFrame([
        ("Stage 1 hits",             f"{int(o['s1_hits']):,}"),
        ("Stage 1 distinct ICAO24",  f"{int(o['s1_icao24']):,}"),
        ("Stage 2 positions",        f"{int(o['s2_positions']):,}"),
        ("Stage 2 distinct ICAO24",  f"{int(o['s2_icao24']):,}"),
        ("Stage 3 events",           f"{int(o['s3_events']):,}"),
        ("Stage 3 distinct ICAO24",  f"{int(o['s3_icao24']):,}"),
        ("Date range",               f"{o['d_min']} → {o['d_max']}"),
    ], columns=["Metric", "Value"])

    cards, tex_files = [], []
    tex_files.append(save_table(summary, "heli_summary",
                                "Helicopter ADS-B dataset summary.",
                                "tab:datastat_heli_summary"))

    p, m = _fig_bar(monthly_stage1, "month", "hits",
                    title="Stage 1 helicopter hits per month",
                    x_label="Month", y_label="Hits")
    h, _ = save_fig_dual("heli_monthly_stage1", p, m)
    cards.append(("Monthly Stage 1 hits", h))
    tex_files.append(("heli_monthly_stage1",
                      "Monthly Stage 1 tripwire hits."))

    p, m = _fig_hist(altitudes["min_alt_m"], bins=60,
                     title="Stage 1 minimum altitude distribution",
                     x_label="Min altitude (m)")
    h, _ = save_fig_dual("heli_alt_distribution", p, m)
    cards.append(("Altitude distribution", h))
    tex_files.append(("heli_alt_distribution",
                      "Distribution of minimum recorded altitude per Stage 1 hit."))

    p, m = _fig_bar(per_aircraft, "icao24", "events", orientation="h",
                    title="Top 20 helicopters by Stage 3 event count",
                    x_label="Events")
    h, _ = save_fig_dual("heli_per_aircraft_top20", p, m)
    cards.append(("Top 20 helicopters", h))
    tex_files.append(("heli_per_aircraft_top20",
                      "Top 20 ICAO24 codes by classified Stage 3 event count."))

    p, m = _fig_donut(per_project["project_name"].tolist(),
                      per_project["events"].tolist(),
                      title="Stage 3 events by wind farm")
    h, _ = save_fig_dual("heli_per_project", p, m)
    cards.append(("Events per wind farm", h))
    tex_files.append(("heli_per_project",
                      "Distribution of Stage 3 helicopter events across wind farms."))

    return dict(name="Helicopter ADS-B", anchor="datastat_heli",
                kpi=summary, cards=cards, tex_files=tex_files)


# Section: Ports

def section_ports() -> dict:
    print("\n[ports]")

    overview = cached("ports_overview", lambda cn: pd.read_sql("""
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE name IS NOT NULL AND name <> '') AS with_name,
               COUNT(*) FILTER (WHERE port_type IS NOT NULL) AS with_type
        FROM osm_ports
    """, cn))

    nearest = cached("ports_nearest_per_farm", lambda cn: pd.read_sql("""
        WITH farm_centroids AS (
          SELECT project_name,
                 AVG(latitude)  AS c_lat,
                 AVG(longitude) AS c_lon
          FROM wind_turbines GROUP BY project_name
        )
        SELECT f.project_name,
               MIN(
                 ST_Distance(
                   ST_SetSRID(ST_MakePoint(p.longitude, p.latitude), 4326)::geography,
                   ST_SetSRID(ST_MakePoint(f.c_lon,    f.c_lat),    4326)::geography
                 )
               ) / 1000.0 AS nearest_km
        FROM farm_centroids f
        CROSS JOIN osm_ports p
        WHERE p.latitude  BETWEEN f.c_lat - 1.5 AND f.c_lat + 1.5
          AND p.longitude BETWEEN f.c_lon - 1.5 AND f.c_lon + 1.5
        GROUP BY f.project_name
        ORDER BY nearest_km
    """, cn))

    top_types = cached("ports_top_types", lambda cn: pd.read_sql("""
        SELECT COALESCE(port_type, 'unknown') AS port_type, COUNT(*) AS n
        FROM osm_ports
        GROUP BY 1 ORDER BY 2 DESC LIMIT 15
    """, cn))

    o = overview.iloc[0]
    summary = pd.DataFrame([
        ("OSM port records",        f"{int(o['total']):,}"),
        ("Records with name",       f"{int(o['with_name']):,}"),
        ("Records with port_type",  f"{int(o['with_type']):,}"),
    ], columns=["Metric", "Value"])

    cards, tex_files = [], []
    tex_files.append(save_table(summary, "ports_summary",
                                "OpenStreetMap ports dataset summary.",
                                "tab:datastat_ports_summary"))

    p, m = _fig_bar(nearest, "project_name", "nearest_km",
                    title="Distance from each wind farm to nearest OSM port",
                    x_label="Wind farm", y_label="Distance (km)")
    h, _ = save_fig_dual("ports_nearest_per_farm", p, m)
    cards.append(("Nearest port per farm", h))
    tex_files.append(("ports_nearest_per_farm",
                      "Distance from each wind farm centroid to its nearest OSM port."))

    p, m = _fig_bar(top_types, "port_type", "n", orientation="h",
                    title="Top OSM port types",
                    x_label="Records")
    h, _ = save_fig_dual("ports_top_types", p, m)
    cards.append(("Top port types", h))
    tex_files.append(("ports_top_types",
                      "Top 15 \\texttt{port\\_type} categories in OSM."))

    return dict(name="Ports", anchor="datastat_ports",
                kpi=summary, cards=cards, tex_files=tex_files)


# Section: Airports

def section_airports() -> dict:
    print("\n[airports]")

    osm_overview = cached("airports_osm_overview", lambda cn: pd.read_sql("""
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE icao IS NOT NULL) AS with_icao,
               COUNT(*) FILTER (WHERE aeroway = 'heliport')   AS heliports,
               COUNT(*) FILTER (WHERE aeroway = 'aerodrome')  AS aerodromes
        FROM osm_airports
    """, cn))

    bbox_count = cached("airports_us_ne_bbox", lambda cn: pd.read_sql("""
        SELECT COUNT(*) AS in_bbox,
               COUNT(*) FILTER (WHERE aeroway = 'heliport')  AS heliports_in_bbox,
               COUNT(*) FILTER (WHERE aeroway = 'aerodrome') AS aerodromes_in_bbox
        FROM osm_airports
        WHERE latitude BETWEEN %s AND %s
          AND longitude BETWEEN %s AND %s
    """, cn, params=(US_NE_BBOX["lat_min"], US_NE_BBOX["lat_max"],
                    US_NE_BBOX["lon_min"], US_NE_BBOX["lon_max"])))

    # OurAirports CSV — read via cached() wrapper to keep schema consistent
    def load_ourairports():
        return pd.read_csv(OURAIRPORTS_CSV, low_memory=False, usecols=[
            "type", "latitude_deg", "longitude_deg",
        ])

    type_df = cached("ourairports_type_breakdown", lambda:
        load_ourairports().groupby("type").size()
            .reset_index(name="count").sort_values("count", ascending=False))

    oa_overview = cached("ourairports_overview", lambda cn: pd.DataFrame([{
        "total": int(load_ourairports().shape[0])
    }]))

    osmo = osm_overview.iloc[0]
    bb   = bbox_count.iloc[0]
    summary = pd.DataFrame([
        ("OSM airport/heliport records",  f"{int(osmo['total']):,}"),
        ("OSM aerodromes",                f"{int(osmo['aerodromes']):,}"),
        ("OSM heliports",                 f"{int(osmo['heliports']):,}"),
        ("OurAirports records",           f"{int(oa_overview.iloc[0]['total']):,}"),
        ("OSM airports in US-NE bbox",    f"{int(bb['in_bbox']):,}"),
        ("OSM heliports in US-NE bbox",   f"{int(bb['heliports_in_bbox']):,}"),
    ], columns=["Metric", "Value"])

    cards, tex_files = [], []
    tex_files.append(save_table(summary, "airports_summary",
                                "Airports and heliports dataset summary.",
                                "tab:datastat_airports_summary"))

    p, m = _fig_bar(type_df.head(8), "type", "count", orientation="h",
                    title="OurAirports records by facility type",
                    x_label="Records")
    h, _ = save_fig_dual("airports_type_breakdown", p, m)
    cards.append(("Airport type breakdown", h))
    tex_files.append(("airports_type_breakdown",
                      "Distribution of OurAirports facility types."))

    bbox_df = pd.DataFrame([
        ("All", int(bb["in_bbox"])),
        ("Heliports", int(bb["heliports_in_bbox"])),
        ("Aerodromes", int(bb["aerodromes_in_bbox"])),
    ], columns=["Category", "Count"])
    p, m = _fig_bar(bbox_df, "Category", "Count",
                    title="OSM airports in US-NE bounding box (38–43°N, 76–69°W)",
                    x_label="", y_label="Records")
    h, _ = save_fig_dual("airports_us_ne_bbox", p, m)
    cards.append(("US-NE bounding box", h))
    tex_files.append(("airports_us_ne_bbox",
                      "OSM airports/heliports within the US Northeast bounding box."))

    return dict(name="Airports", anchor="datastat_airports",
                kpi=summary, cards=cards, tex_files=tex_files)


# Section: Weather

def section_weather() -> dict:
    print("\n[weather]")

    overview = cached("weather_overview", lambda cn: pd.read_sql("""
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT station_id) AS stations,
               MIN(time)::date AS time_min,
               MAX(time)::date AS time_max
        FROM weather_observations
    """, cn))

    monthly = cached("weather_monthly_obs", lambda cn: pd.read_sql("""
        SELECT DATE_TRUNC('month', time)::date AS month, COUNT(*) AS observations
        FROM weather_observations
        GROUP BY 1 ORDER BY 1
    """, cn))

    completeness_query = "SELECT COUNT(*) AS total, " + ", ".join(
        f"COUNT({col}) AS nn_{col}" for col in WEATHER_NUMERIC_COLUMNS
    ) + " FROM weather_observations"
    completeness = cached("weather_completeness", lambda cn: pd.read_sql(completeness_query, cn))

    windspeed = cached("weather_windspeed_dist", lambda cn: pd.read_sql("""
        SELECT wind_speed FROM weather_observations
        WHERE wind_speed IS NOT NULL AND wind_speed BETWEEN 0 AND 40
    """, cn))

    o = overview.iloc[0]
    summary = pd.DataFrame([
        ("Observations",           f"{int(o['rows']):,}"),
        ("Distinct stations",      f"{int(o['stations']):,}"),
        ("First observation",      str(o["time_min"])),
        ("Last observation",       str(o["time_max"])),
    ], columns=["Metric", "Value"])

    cards, tex_files = [], []
    tex_files.append(save_table(summary, "weather_summary",
                                "ICOADS weather observations dataset summary.",
                                "tab:datastat_weather_summary"))

    p, m = _fig_bar(monthly, "month", "observations",
                    title="Monthly weather observations",
                    x_label="Month", y_label="Observations")
    h, _ = save_fig_dual("weather_monthly_obs", p, m)
    cards.append(("Monthly observations", h))
    tex_files.append(("weather_monthly_obs",
                      "Monthly ICOADS observation counts."))

    crow = completeness.iloc[0]
    total = int(crow["total"])
    comp_df = pd.DataFrame(
        [(col, 100.0 * int(crow[f"nn_{col}"]) / total) for col in WEATHER_NUMERIC_COLUMNS],
        columns=["column", "pct_non_null"],
    ).sort_values("pct_non_null", ascending=False)
    p, m = _fig_bar(comp_df, "column", "pct_non_null", orientation="h",
                    title="Weather column completeness (% non-null)",
                    x_label="% non-null")
    h, _ = save_fig_dual("weather_completeness", p, m)
    cards.append(("Column completeness", h))
    tex_files.append(("weather_completeness",
                      "Per-column non-null fraction across the weather observations table."))

    p, m = _fig_hist(windspeed["wind_speed"], bins=40,
                     title="Wind speed distribution (0–40 m/s)",
                     x_label="Wind speed (m/s)")
    h, _ = save_fig_dual("weather_windspeed_dist", p, m)
    cards.append(("Wind speed distribution", h))
    tex_files.append(("weather_windspeed_dist",
                      "Distribution of measured wind speeds (0–40 m/s)."))

    return dict(name="Weather (ICOADS)", anchor="datastat_weather",
                kpi=summary, cards=cards, tex_files=tex_files)


# Section: Wind turbines

def section_turbines() -> dict:
    print("\n[turbines]")

    overview = cached("turbines_overview", lambda cn: pd.read_sql("""
        SELECT COUNT(*) AS total,
               COUNT(DISTINCT project_name) AS projects,
               AVG(height_meters)::numeric(6,1) AS avg_height,
               MIN(height_meters) AS min_height,
               MAX(height_meters) AS max_height
        FROM wind_turbines
    """, cn))

    per_project = cached("turbines_per_project", lambda cn: pd.read_sql("""
        SELECT project_name, COUNT(*) AS turbines
        FROM wind_turbines
        GROUP BY 1 ORDER BY 2 DESC
    """, cn))

    heights = cached("turbines_height_distribution", lambda cn: pd.read_sql("""
        SELECT height_meters FROM wind_turbines
        WHERE height_meters IS NOT NULL
    """, cn))

    o = overview.iloc[0]
    summary = pd.DataFrame([
        ("Turbines",         f"{int(o['total']):,}"),
        ("Projects",         f"{int(o['projects']):,}"),
        ("Mean height (m)",  f"{float(o['avg_height'] or 0):.1f}"),
        ("Min height (m)",   f"{float(o['min_height'] or 0):.1f}"),
        ("Max height (m)",   f"{float(o['max_height'] or 0):.1f}"),
    ], columns=["Metric", "Value"])

    cards, tex_files = [], []
    tex_files.append(save_table(summary, "turbines_summary",
                                "Wind turbines reference dataset summary.",
                                "tab:datastat_turbines_summary"))

    p, m = _fig_bar(per_project, "project_name", "turbines",
                    title="Turbines per wind farm",
                    x_label="Project", y_label="Turbines")
    h, _ = save_fig_dual("turbines_per_project", p, m)
    cards.append(("Turbines per farm", h))
    tex_files.append(("turbines_per_project",
                      "Number of turbines in each wind farm."))

    if not heights.empty:
        p, m = _fig_hist(heights["height_meters"], bins=20,
                         title="Turbine height distribution",
                         x_label="Height (m)")
        h, _ = save_fig_dual("turbines_height_distribution", p, m)
        cards.append(("Height distribution", h))
        tex_files.append(("turbines_height_distribution",
                          "Distribution of turbine heights across all installations."))

    return dict(name="Wind turbines", anchor="datastat_turbines",
                kpi=summary, cards=cards, tex_files=tex_files)


# Output

def render_html(sections: list[dict]) -> str:
    section_html = ""
    for s in sections:
        rows = "".join(
            f"<tr><td>{r.iloc[0]}</td><td>{r.iloc[1]}</td></tr>"
            for _, r in s["kpi"].iterrows()
        )
        cards_html = "\n".join(
            f'<div class="card"><div class="card-title">{name}</div>{html}</div>'
            for name, html in s["cards"]
        )
        section_html += f"""
        <section id="{s['anchor']}">
          <h2>{s['name']}</h2>
          <div class="kpi-table"><table>{rows}</table></div>
          <div class="grid">{cards_html}</div>
        </section>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Data Statistics — Thesis</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{ font-family: Inter, system-ui, sans-serif; margin: 0;
           background: #f5f6f8; color: #222; }}
    header {{ background: linear-gradient(135deg, #2c5f8d, #1f4666);
             color: white; padding: 28px 40px; }}
    header h1 {{ margin: 0 0 6px; font-size: 1.7em; }}
    header .sub {{ opacity: 0.85; }}
    main {{ max-width: 1400px; margin: 0 auto; padding: 24px 40px; }}
    section {{ margin-bottom: 36px; }}
    section h2 {{ font-size: 1.25em; color: #1f4666;
                  border-bottom: 2px solid #2c5f8d; padding-bottom: 4px; }}
    .kpi-table {{ margin: 12px 0 14px; }}
    .kpi-table table {{ border-collapse: collapse; font-size: 0.92em; }}
    .kpi-table td {{ padding: 4px 16px 4px 0; }}
    .kpi-table tr td:first-child {{ color: #555; }}
    .kpi-table tr td:last-child {{ font-weight: 600; color: #1f4666; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
            gap: 16px; }}
    .card {{ background: white; border-radius: 8px; padding: 12px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05); }}
    .card-title {{ font-weight: 600; font-size: 0.95em; color: #1f4666;
                  margin-bottom: 6px; padding-bottom: 4px;
                  border-bottom: 1px solid #eee; }}
    nav {{ background: white; padding: 12px 40px; border-bottom: 1px solid #e2e6ea;
          font-size: 0.92em; }}
    nav a {{ color: #2c5f8d; text-decoration: none; margin-right: 18px; }}
    nav a:hover {{ text-decoration: underline; }}
    footer {{ text-align: center; color: #888; padding: 20px; font-size: 0.85em; }}
  </style>
</head>
<body>
  <header>
    <h1>Data Statistics</h1>
    <div class="sub">Descriptive overview of all thesis data sources</div>
  </header>
  <nav>{"".join(f'<a href="#{s["anchor"]}">{s["name"]}</a>' for s in sections)}</nav>
  <main>{section_html}</main>
  <footer>Generated by generate_data_stats.py · {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</footer>
</body>
</html>"""


def render_tex_index(sections: list[dict]) -> str:
    lines = ["% Auto-generated by generate_data_stats.py — do not edit by hand.",
             "% Use \\section{Data Statistics}\\input{data_statistics} to embed.",
             ""]
    for s in sections:
        lines.append(f"\\subsection*{{{s['name']}}}\\label{{subsec:{s['anchor']}}}")
        for entry in s["tex_files"]:
            if isinstance(entry, Path):
                # Summary table — \input directly
                rel = entry.relative_to(Path("/mnt/d/thesis/main"))
                lines.append(f"\\input{{{rel.with_suffix('').as_posix()}}}")
            else:
                fig_name, caption = entry
                lines.append(f"\\begin{{figure}}[htbp]\\centering")
                lines.append(f"  \\includegraphics[width=0.85\\textwidth]"
                             f"{{figures/data_stats/{fig_name}.pdf}}")
                lines.append(f"  \\caption{{{caption}}}")
                lines.append(f"  \\label{{fig:{fig_name}}}")
                lines.append(f"\\end{{figure}}")
        lines.append("")
    return "\n".join(lines)


# Main

SECTION_FUNCS = {
    "vessel":   section_vessel,
    "heli":     section_heli,
    "ports":    section_ports,
    "airports": section_airports,
    "weather":  section_weather,
    "turbines": section_turbines,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-cache", action="store_true",
                        help="Ignore the cache and recompute every aggregation.")
    parser.add_argument("--refresh", action="append", default=[],
                        choices=DOMAINS,
                        help="Drop cached results for one domain (repeatable).")
    parser.add_argument("--domain", action="append", default=[],
                        choices=DOMAINS,
                        help="Render only specified domain(s) (repeatable). "
                             "Default: all.")
    args = parser.parse_args()

    _CACHE_STATE["no_cache"] = args.no_cache
    _CACHE_STATE["refresh_prefixes"] = tuple(f"{d}_" for d in args.refresh) + tuple(
        d if d in {"airports"} else f"{d}_" for d in args.refresh
    )
    _CACHE_STATE["data"] = _load_cache()

    _mpl_setup()
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    TBL_DIR.mkdir(parents=True, exist_ok=True)

    domains_to_run = args.domain or DOMAINS

    # When --domain is used we still load existing sections from cache so the
    # combined HTML/TeX stay consistent. For now, only render requested domains.
    # Each cached fetch opens its own short-lived connection (see cached()),
    # so main() doesn't hold a connection across heavy aggregations.
    sections: list[dict] = []
    for d in domains_to_run:
        sections.append(SECTION_FUNCS[d]())

    OUT_HTML.write_text(render_html(sections))
    print(f"\nWrote {OUT_HTML}  ({OUT_HTML.stat().st_size/1024:.1f} KB)")

    TEX_INDEX.parent.mkdir(parents=True, exist_ok=True)
    TEX_INDEX.write_text(render_tex_index(sections))
    print(f"Wrote {TEX_INDEX}")
    print(f"Figures → {FIG_DIR}")
    print(f"Tables  → {TBL_DIR}")

    # Sanity check
    print("\nSanity check (expected vs current):")
    for d in domains_to_run:
        s = next(x for x in sections if x["name"].lower().startswith(d[:4]) or
                 d in x["anchor"])
        first_value = s["kpi"].iloc[0]["Value"]
        print(f"  {s['name']:<22s}  first KPI = {first_value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
