#!/usr/bin/env python3
"""
Add is_offshore_ne column to osm_wind_turbines using land polygons.

Two polygon sources are supported (--source):
  ne     — Natural Earth 1:10m land polygons (~5 MB, fast, default)
  gshhg  — GSHHG high-resolution shoreline (~200 MB, slower, more accurate)
  osm    — OSM land polygons split (~900 MB, continuously updated, best for reclaimed land)

A minimum distance from land (--min-distance, default 500 m) is applied via
ST_DWithin to avoid false positives for turbines sitting on the polygon edge.

Columns after running:
  is_offshore    — original OSM tag (sparse, unreliable for NA)
  is_offshore_ne — derived from spatial join (corrected)

Usage:
  python fix_osm_offshore_flag.py
  python fix_osm_offshore_flag.py --source gshhg
  python fix_osm_offshore_flag.py --source osm
  python fix_osm_offshore_flag.py --min-distance 1000
  python fix_osm_offshore_flag.py --dry-run
"""

import argparse
import sys
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import psycopg2
import requests
from sqlalchemy import create_engine

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "windfarm",
    "user":     "thesis",
    "password": "thesis2026",
}

NE_URL   = "https://naciscdn.org/naturalearth/10m/physical/ne_10m_land.zip"
NE_CACHE = Path("/mnt/e/data_lake/ne_10m_land.zip")

# GSHHG high-resolution shoreline (level 1 = land boundaries)
# Version 2.3.7 — https://www.ngdc.noaa.gov/mgg/shorelines/
GSHHG_URL   = "https://www.ngdc.noaa.gov/mgg/shorelines/data/gshhg/latest/gshhg-shp-2.3.7.zip"
GSHHG_CACHE = Path("/mnt/e/data_lake/gshhg-shp-2.3.7.zip")

# OSM land polygons (split, WGS84) — continuously updated, captures land reclamation
# Source: osmdata.openstreetmap.de
OSM_COAST_URL   = "https://osmdata.openstreetmap.de/download/land-polygons-split-4326.zip"
OSM_COAST_CACHE = Path("/mnt/e/data_lake/osm_land_polygons_split_4326.zip")


# Download

def _download(url: str, cache: Path, label: str) -> Path:
    if cache.exists():
        print(f"Using cached: {cache}")
        return cache
    print(f"Downloading {label}...")
    r = requests.get(url, timeout=120, stream=True)
    r.raise_for_status()
    cache.parent.mkdir(parents=True, exist_ok=True)
    total = int(r.headers.get("content-length", 0))
    downloaded = 0
    with open(cache, "wb") as f:
        for chunk in r.iter_content(65536):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = 100 * downloaded / total
                print(f"\r  {pct:.0f}%  ({downloaded // 1_048_576} MB)", end="", flush=True)
    print(f"\nCached: {cache}")
    return cache


def download_ne_land() -> Path:
    return _download(NE_URL, NE_CACHE, "Natural Earth 10m land polygons (~5 MB)")


def download_gshhg() -> Path:
    return _download(GSHHG_URL, GSHHG_CACHE, "GSHHG high-resolution shoreline (~200 MB)")


def download_osm_coastline() -> Path:
    return _download(OSM_COAST_URL, OSM_COAST_CACHE,
                     "OSM land polygons split (~900 MB, continuously updated)")


# Load shapefile into PostGIS

def _load_shapefile(conn, zip_path: Path, shp_glob: str,
                    table_name: str, label: str) -> None:
    """Load a shapefile from a zip into PostGIS via geopandas."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT to_regclass('public.{table_name}')")
        if cur.fetchone()[0] is not None:
            print(f"{table_name} table already exists — skipping load.")
            return

    print(f"Loading {label} into PostGIS as {table_name}...")
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmpdir)

        shp = next(Path(tmpdir).glob(shp_glob))
        gdf = gpd.read_file(shp)
        gdf = gdf.set_crs(4326, allow_override=True)

    cfg = DB_CONFIG
    url = (f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
           f"@{cfg['host']}:{cfg['port']}/{cfg['dbname']}")
    engine = create_engine(url)
    gdf.to_postgis(table_name, engine, schema="public",
                   if_exists="replace", index=False)
    print(f"{table_name} loaded ({len(gdf):,} polygons).")


def load_ne_land(conn, zip_path: Path) -> None:
    _load_shapefile(conn, zip_path, "*.shp", "ne_land", "Natural Earth 10m land")


def load_gshhg(conn, zip_path: Path) -> None:
    _load_shapefile(conn, zip_path, "**/GSHHS_h_L1.shp", "gshhg_land",
                    "GSHHG high-resolution land (L1)")


def load_osm_coastline(conn, zip_path: Path) -> None:
    _load_shapefile(conn, zip_path, "**/land_polygons.shp", "osm_land",
                    "OSM land polygons (split, WGS84)")


# Spatial index

def ensure_spatial_index(conn, table_name: str) -> None:
    """Create a GIST index on the geometry column if it doesn't already exist."""
    index_name = f"{table_name}_geom_idx"
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM pg_indexes
            WHERE tablename = %s AND indexname = %s
        """, (table_name, index_name))
        if cur.fetchone():
            print(f"Spatial index on {table_name} already exists.")
            return
    print(f"Creating spatial index on {table_name} (one-time, may take a minute)...")
    with conn.cursor() as cur:
        cur.execute(f"CREATE INDEX {index_name} ON {table_name} USING GIST (geometry)")
    conn.commit()
    print("Index created.")


# Add column and run spatial update

def add_column(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE osm_wind_turbines
            ADD COLUMN IF NOT EXISTS is_offshore_ne BOOLEAN
        """)
    conn.commit()
    print("Column is_offshore_ne added (or already exists).")


_LAT_BANDS = [
    (-90, -60), (-60, -30), (-30,   0),
    (  0,  30), ( 30,  60), ( 60,  90),
]


def run_spatial_update(conn, min_distance_m: float = 500,
                        land_table: str = "ne_land") -> int:
    """
    Set is_offshore_ne = TRUE for turbines at least min_distance_m metres from
    any land polygon. Processes in latitude bands to avoid connection timeouts.

    Uses geometry-based ST_DWithin (degrees) so the GIST index is active.
    Degree threshold = min_distance_m / 111320 (equatorial approximation,
    conservative: slightly over-excludes near poles, fine for offshore classification).
    """
    min_distance_deg = min_distance_m / 111_320
    print(f"Running spatial classification "
          f"(source: {land_table}, min distance: {min_distance_m} m "
          f"≈ {min_distance_deg:.6f}°, processing in 6 latitude bands)...")

    sql = f"""
        UPDATE osm_wind_turbines t
        SET is_offshore_ne = TRUE
        WHERE latitude BETWEEN %s AND %s
          AND NOT EXISTS (
            SELECT 1 FROM {land_table} l
            WHERE ST_DWithin(
                ST_SetSRID(ST_MakePoint(t.longitude, t.latitude), 4326),
                l.geometry,
                %s
            )
        )
    """

    with conn.cursor() as cur:
        cur.execute("UPDATE osm_wind_turbines SET is_offshore_ne = FALSE")
    conn.commit()

    total = 0
    for lat_min, lat_max in _LAT_BANDS:
        with conn.cursor() as cur:
            cur.execute(sql, (lat_min, lat_max, min_distance_deg))
            n = cur.rowcount
        conn.commit()
        total += n
        print(f"  lat {lat_min:+d}…{lat_max:+d}: {n:,} offshore")

    return total


# Comparison report

def print_comparison(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                is_offshore    AS osm_tag,
                is_offshore_ne AS ne_derived,
                COUNT(*)       AS turbines
            FROM osm_wind_turbines
            GROUP BY 1, 2
            ORDER BY 1, 2
        """)
        rows = cur.fetchall()

    print(f"\n{'OSM tag':<10}  {'NE derived':<12}  {'Turbines':>10}  Interpretation")
    print(f"{'-'*10}  {'-'*12}  {'-'*10}  {'-'*40}")
    labels = {
        (True,  True):  "Correctly tagged offshore",
        (True,  False): "OSM says offshore but on land → OSM error",
        (False, True):  "Untagged but actually offshore → OSM gap ★",
        (False, False): "Correctly tagged onshore",
        (None,  True):  "NULL tag, actually offshore",
        (None,  False): "NULL tag, onshore",
    }
    for osm, ne, n in rows:
        label = labels.get((osm, ne), "")
        print(f"{str(osm):<10}  {str(ne):<12}  {n:>10,}  {label}")

    # NA-specific breakdown
    print("\nNA (lat 25–72, lon -170 to -60):")
    cur = conn.cursor()
    cur.execute("""
        SELECT is_offshore, is_offshore_ne, COUNT(*)
        FROM osm_wind_turbines
        WHERE latitude BETWEEN 25 AND 72
          AND longitude BETWEEN -170 AND -60
        GROUP BY 1, 2 ORDER BY 1, 2
    """)
    for osm, ne, n in cur.fetchall():
        label = labels.get((osm, ne), "")
        print(f"  {str(osm):<10}  {str(ne):<12}  {n:>8,}  {label}")
    cur.close()


# Entry point

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Add is_offshore_ne column to osm_wind_turbines via land polygons."
    )
    parser.add_argument("--source", choices=["ne", "gshhg", "osm"], default="ne",
                        help="Land polygon source: 'ne' = Natural Earth 1:10m (default, fast), "
                             "'gshhg' = GSHHG high-res (~200 MB), "
                             "'osm' = OSM land polygons split (~900 MB, current, best for reclaimed land)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Only print comparison without downloading or updating")
    parser.add_argument("--min-distance", type=float, default=500, metavar="METRES",
                        help="Minimum distance from land to classify as offshore (default: 500 m)")
    args = parser.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)

    if args.dry_run:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('osm_wind_turbines')")
            if cur.fetchone()[0] is None:
                print("osm_wind_turbines table not found.")
                conn.close()
                return 1
        print_comparison(conn)
        conn.close()
        return 0

    if args.source == "gshhg":
        zip_path   = download_gshhg()
        load_gshhg(conn, zip_path)
        land_table = "gshhg_land"
    elif args.source == "osm":
        zip_path   = download_osm_coastline()
        load_osm_coastline(conn, zip_path)
        land_table = "osm_land"
    else:
        zip_path   = download_ne_land()
        load_ne_land(conn, zip_path)
        land_table = "ne_land"

    add_column(conn)
    ensure_spatial_index(conn, land_table)
    n = run_spatial_update(conn, args.min_distance, land_table)
    print(f"Classified {n:,} turbines as offshore "
          f"(source: {land_table}, >{args.min_distance} m from land).")
    print_comparison(conn)

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
