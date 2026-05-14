#!/usr/bin/env python3
# Download all wind turbine nodes from OpenStreetMap via the Overpass API.
#
# Queries the Overpass API in continental bounding-box chunks to avoid timeouts,
# then writes a single deduplicated CSV to the output path.  The script can be
# re-run safely; existing output is overwritten.
#
# OSM tags captured
#   osm_id, latitude, longitude, name, ref, operator, manufacturer, model,
#   output_kw (normalised from generator:output:electricity), hub_height_m,
#   rotor_diameter_m, start_date, location_tag, is_offshore
#
# Usage:
#   python fetch_osm_wind_turbines.py
#   python fetch_osm_wind_turbines.py --out /mnt/e/data_lake/wind_turbines/osm_wind_turbines.csv
#   python fetch_osm_wind_turbines.py --offshore-only

import argparse
import csv
import sys
import time
from pathlib import Path

import requests

# Constants

## Default output path.
DEFAULT_OUT = Path("/mnt/e/data_lake/wind_turbines/osm_wind_turbines.csv")

## Overpass API endpoint.
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

## Seconds to wait between regional chunk requests (rate limiting).
REQUEST_PAUSE = 5

## Maximum retries per chunk on HTTP error / timeout.
MAX_RETRIES = 3

## Seconds to wait before retrying a failed request.
RETRY_PAUSE = 30

## CSV column order.
CSV_COLUMNS = [
    "osm_id",
    "latitude",
    "longitude",
    "name",
    "ref",
    "operator",
    "manufacturer",
    "model",
    "output_kw",
    "hub_height_m",
    "rotor_diameter_m",
    "start_date",
    "location_tag",
    "is_offshore",
]

## Regional bounding boxes [south, west, north, east] covering all land
#  and near-shore offshore areas where wind turbines exist.
#  Dense regions (North America, Europe, East Asia) are split into ~20°×15°
#  tiles to keep each Overpass query within the 300-second time limit.
REGIONS = [
    # North America — 6 tiles (Great Plains is the densest area on Earth)
    ("US East",            35.0,  -90.0,  55.0,  -60.0),
    ("US Midwest",         35.0, -105.0,  55.0,  -90.0),
    ("US West",            25.0, -125.0,  55.0, -105.0),
    ("Canada North",       55.0, -140.0,  72.0,  -60.0),
    ("Alaska",             55.0, -170.0,  72.0, -140.0),
    ("Mexico / Caribbean", 15.0, -120.0,  35.0,  -60.0),
    # Europe — 4 tiles (UK + North Sea offshore is very dense)
    ("Europe NW",          50.0,  -15.0,  72.0,   15.0),
    ("Europe SW Iberia",   35.0,  -15.0,  50.0,    0.0),  # Spain, Portugal
    ("Europe SW France",   35.0,    0.0,  50.0,   15.0),  # France, Italy N
    ("Europe NE North",    57.0,   15.0,  72.0,   35.0),  # Scandinavia
    ("Europe NE South",    45.0,   15.0,  57.0,   35.0),  # Poland, Baltic, Czech
    ("Europe SE",          35.0,   15.0,  50.0,   45.0),
    # Rest of world
    ("North Africa/MidEast", 10.0,  -20.0,  40.0,   60.0),  # Morocco→Gulf
    ("Sub-Saharan Africa", -40.0,  -20.0,  10.0,   60.0),  # South Africa etc.
    ("South America",      -60.0,  -85.0,  15.0,  -30.0),
    ("South Asia",           5.0,   60.0,  35.0,   95.0),
    ("East Asia N",         35.0,   95.0,  55.0,  130.0),
    ("East Asia S",         10.0,   95.0,  35.0,  130.0),
    ("Japan / Korea",       25.0,  130.0,  55.0,  148.0),
    ("Southeast Asia",     -15.0,   95.0,  25.0,  130.0),
    ("Oceania",            -50.0,  110.0,   0.0,  180.0),
    ("Far East / Pacific",  20.0,  145.0,  72.0,  180.0),
]


# Helpers

def _parse_output_kw(raw: str | None) -> float | None:
    """
    Normalise generator:output:electricity to kilowatts.

    Handles formats: "8 MW", "8MW", "8000 kW", "8000kW", "8000000 W",
    "8000000W", bare numbers (assumed watts).

    raw: Raw tag value string, or None.
    Returns: Power in kW as float, or None if unparseable.
    """
    if not raw:
        return None
    s = raw.strip().lower().replace(",", ".")
    multiplier = 1.0
    if "mw" in s:
        multiplier = 1_000.0
        s = s.replace("mw", "").strip()
    elif "kw" in s:
        multiplier = 1.0
        s = s.replace("kw", "").strip()
    elif "w" in s:
        multiplier = 0.001
        s = s.replace("w", "").strip()
    try:
        return float(s) * multiplier
    except (ValueError, TypeError):
        return None


def _parse_metres(raw: str | None) -> float | None:
    """
    Parse a tag value that represents a length in metres.

    Strips trailing unit suffixes ("m", "ft") and converts feet to metres
    when the suffix is "ft".

    raw: Raw tag value string, or None.
    Returns: Length in metres as float, or None if unparseable.
    """
    if not raw:
        return None
    s = raw.strip().lower()
    factor = 1.0
    if s.endswith("ft"):
        s = s[:-2].strip()
        factor = 0.3048
    elif s.endswith("m"):
        s = s[:-1].strip()
    try:
        return float(s) * factor
    except (ValueError, TypeError):
        return None


def _build_query(south: float, west: float, north: float, east: float) -> str:
    """
    Build an Overpass QL query for wind generator nodes in a bounding box.

    south: Southern latitude boundary.
    west: Western longitude boundary.
    north: Northern latitude boundary.
    east: Eastern longitude boundary.
    Returns: Overpass QL query string.
    """
    bbox = f"{south},{west},{north},{east}"
    return (
        f'[out:json][timeout:300];'
        f'node["power"="generator"]["generator:source"="wind"]'
        f'({bbox});'
        f'out body;'
    )


def _fetch_region(name: str, south: float, west: float,
                  north: float, east: float) -> list[dict]:
    """
    Fetch all wind turbine nodes within a regional bounding box.

    Retries up to MAX_RETRIES times on transient errors.

    name: Human-readable region name (used for progress output).
    south: Southern latitude boundary.
    west: Western longitude boundary.
    north: Northern latitude boundary.
    east: Eastern longitude boundary.
    Returns: List of raw OSM element dicts from the Overpass response.
    """
    query = _build_query(south, west, north, east)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                OVERPASS_URL,
                data={"data": query},
                timeout=360,
            )
            resp.raise_for_status()
            data = resp.json()
            if "remark" in data:
                print(f"  {name}: Overpass warning — {data['remark']}", file=sys.stderr)
            elements = data.get("elements", [])
            print(f"  {name}: {len(elements):,} nodes")
            return elements
        except (requests.RequestException, ValueError) as exc:
            print(f"  {name}: attempt {attempt}/{MAX_RETRIES} failed — {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_PAUSE)
    print(f"  {name}: all retries exhausted, skipping region", file=sys.stderr)
    return []


def _element_to_row(elem: dict, offshore_only: bool) -> dict | None:
    """
    Convert a raw OSM element dict to a CSV row dict.

    elem: Raw OSM element as returned by the Overpass API.
    offshore_only: If True, return None for turbines not tagged offshore.
    Returns: Dict with keys matching CSV_COLUMNS, or None to skip.
    """
    tags = elem.get("tags", {})
    loc_tag   = tags.get("location", "")
    is_offshore = (
        loc_tag == "offshore"
        or tags.get("offshore") == "yes"
        or tags.get("seamark:type") is not None
    )
    if offshore_only and not is_offshore:
        return None

    return {
        "osm_id":           elem["id"],
        "latitude":         elem.get("lat"),
        "longitude":        elem.get("lon"),
        "name":             tags.get("name") or tags.get("ref:name") or None,
        "ref":              tags.get("ref") or None,
        "operator":         tags.get("operator") or None,
        "manufacturer":     tags.get("manufacturer") or None,
        "model":            tags.get("model") or None,
        "output_kw":        _parse_output_kw(tags.get("generator:output:electricity")),
        "hub_height_m":     _parse_metres(tags.get("height:hub") or tags.get("hub:height")),
        "rotor_diameter_m": _parse_metres(tags.get("rotor:diameter")),
        "start_date":       tags.get("start_date") or None,
        "location_tag":     loc_tag or None,
        "is_offshore":      is_offshore,
    }


# Entry point

def main() -> int:
    """
    Command-line entry point.

    Returns: Exit code: 0 on success, 1 on error.
    """
    parser = argparse.ArgumentParser(
        description="Download wind turbine nodes from OpenStreetMap via Overpass.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s --out /mnt/e/data_lake/wind_turbines/osm_wind_turbines.csv
  %(prog)s --offshore-only
        """,
    )
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT, metavar="PATH",
        help=f"Output CSV path (default: {DEFAULT_OUT})",
    )
    parser.add_argument(
        "--offshore-only", action="store_true",
        help="Only write turbines tagged as offshore (location=offshore / offshore=yes)",
    )
    parser.add_argument(
        "--start-from", metavar="REGION", default=None,
        help="Skip all regions before this name and append to existing CSV "
             "(useful to resume after a crash). Region names: "
             + ", ".join(f'"{r[0]}"' for r in REGIONS),
    )
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)

    # Determine which regions to run and whether to append or overwrite.
    regions_to_run = REGIONS
    file_mode = "w"
    if args.start_from:
        names = [r[0] for r in REGIONS]
        if args.start_from not in names:
            print(f"Error: unknown region '{args.start_from}'. "
                  f"Valid names: {', '.join(names)}", file=sys.stderr)
            return 1
        idx = names.index(args.start_from)
        regions_to_run = REGIONS[idx:]
        file_mode = "a"   # append to existing data
        print(f"Resuming from region #{idx + 1}: '{args.start_from}' (appending to {args.out.name})")

    print(f"Fetching wind turbines from OpenStreetMap Overpass API ...")
    print(f"Output: {args.out}")
    if args.offshore_only:
        print("Filter: offshore only")
    print()

    seen_ids: set[int] = set()
    total_nodes = 0
    total_written = 0

    with open(args.out, file_mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if file_mode == "w":
            writer.writeheader()

        for region_name, south, west, north, east in regions_to_run:
            elements = _fetch_region(region_name, south, west, north, east)
            total_nodes += len(elements)

            for elem in elements:
                osm_id = elem.get("id")
                if osm_id in seen_ids:
                    continue
                seen_ids.add(osm_id)

                row = _element_to_row(elem, args.offshore_only)
                if row is None:
                    continue
                writer.writerow(row)
                total_written += 1

            if elements:
                time.sleep(REQUEST_PAUSE)

    print()
    print(f"Done.")
    print(f"  Nodes fetched : {total_nodes:,}")
    print(f"  Rows written  : {total_written:,} (deduplicated)")
    print(f"  Output        : {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
