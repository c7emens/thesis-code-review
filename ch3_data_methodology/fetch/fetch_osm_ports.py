#!/usr/bin/env python3
# Download port, harbour, marina, and ferry-terminal features from OSM.
#
# Queries the Overpass API in continental bounding-box chunks (same strategy as
# fetch_osm_wind_turbines.py) and writes a deduplicated CSV.  Handles nodes,
# ways, and relations — ways/relations return their centroid via Overpass
# `out center`.
#
# OSM tags queried
#   harbour=*         — any harbour/port/marina node or area
#   seamark:type=harbour — nautical harbour marker
#   amenity=ferry_terminal — ferry terminals
#   landuse=harbour   — harbour land-use area
#
# CSV columns
#   osm_id, osm_type, latitude, longitude, name, operator,
#   harbour, seamark_type, amenity, port_type
#
# Usage:
#   python fetch_osm_ports.py
#   python fetch_osm_ports.py --out /mnt/e/data_lake/ports/osm_ports.csv
#   python fetch_osm_ports.py --exclude-marina

import argparse
import csv
import sys
import time
from pathlib import Path

import requests


# Constants

## Default output path.
DEFAULT_OUT = Path("/mnt/e/data_lake/ports/osm_ports.csv")

## Overpass API endpoint.
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

## Seconds to wait between regional chunk requests (rate limiting).
REQUEST_PAUSE = 8

## Maximum retries per chunk on HTTP error / timeout.
MAX_RETRIES = 3

## Seconds to wait before retrying a failed request.
RETRY_PAUSE = 45

## CSV column order.
CSV_COLUMNS = [
    "osm_id",
    "osm_type",
    "latitude",
    "longitude",
    "name",
    "operator",
    "harbour",
    "seamark_type",
    "amenity",
    "port_type",
]

## Regional bounding boxes [south, west, north, east].
REGIONS = [
    ("North America",      20.0, -170.0,  72.0,  -50.0),
    ("Europe West",        35.0,  -15.0,  72.0,   20.0),
    ("Europe East",        35.0,   20.0,  72.0,   45.0),
    ("Middle East/Africa", -40.0,  -20.0,  40.0,   60.0),
    ("South America",      -60.0,  -85.0,  15.0,  -30.0),
    ("South Asia",           5.0,   60.0,  40.0,   95.0),
    ("East Asia",           15.0,   95.0,  55.0,  145.0),
    ("Southeast Asia",     -15.0,   95.0,  25.0,  145.0),
    ("Oceania",            -50.0,  110.0,   0.0,  180.0),
    ("Far East / Pacific",  20.0,  145.0,  72.0,  180.0),
]


# Query builder

def _build_query(south: float, west: float, north: float, east: float) -> str:
    """
    Build an Overpass QL query for port/harbour features in a bounding box.

    Unions four tag predicates across node and way element types.  `out center`
    returns the centroid lat/lon for way and relation elements.

    south: Southern latitude boundary.
    west: Western longitude boundary.
    north: Northern latitude boundary.
    east: Eastern longitude boundary.
    Returns: Overpass QL query string.
    """
    bbox = f"{south},{west},{north},{east}"
    return (
        f'[out:json][timeout:300];'
        f'('
        f'  node["harbour"]({bbox});'
        f'  way["harbour"]({bbox});'
        f'  node["seamark:type"="harbour"]({bbox});'
        f'  way["seamark:type"="harbour"]({bbox});'
        f'  node["amenity"="ferry_terminal"]({bbox});'
        f'  way["amenity"="ferry_terminal"]({bbox});'
        f'  node["landuse"="harbour"]({bbox});'
        f'  way["landuse"="harbour"]({bbox});'
        f');'
        f'out center;'
    )


# Fetch

def _fetch_region(name: str, south: float, west: float,
                  north: float, east: float) -> list[dict]:
    """
    Fetch all port features within a regional bounding box.

    Retries up to MAX_RETRIES times on transient errors.

    name: Human-readable region name for progress output.
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
            print(f"  {name}: {len(elements):,} elements")
            return elements
        except (requests.RequestException, ValueError) as exc:
            print(f"  {name}: attempt {attempt}/{MAX_RETRIES} failed — {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_PAUSE)
    print(f"  {name}: all retries exhausted, skipping", file=sys.stderr)
    return []


# Row conversion

def _element_to_row(elem: dict, exclude_marina: bool) -> dict | None:
    """
    Convert a raw OSM element dict to a CSV row dict.

    Extracts lat/lon from `lat`/`lon` (nodes) or `center` (ways/relations).
    Derives a simplified port_type from the available tags.

    elem: Raw OSM element dict from Overpass.
    exclude_marina: If True, skip elements where port_type == 'marina'.
    Returns: Dict keyed by CSV_COLUMNS, or None to skip.
    """
    tags     = elem.get("tags", {})
    osm_type = elem.get("type", "node")

    if osm_type == "node":
        lat = elem.get("lat")
        lon = elem.get("lon")
    else:
        center = elem.get("center", {})
        lat = center.get("lat")
        lon = center.get("lon")

    if lat is None or lon is None:
        return None

    harbour_tag  = tags.get("harbour")   or None
    seamark_type = tags.get("seamark:type") or None
    amenity      = tags.get("amenity")   or None

    # Derive a simplified port_type for easy filtering.
    if amenity == "ferry_terminal":
        port_type = "ferry_terminal"
    elif harbour_tag == "marina":
        port_type = "marina"
    elif harbour_tag in {"yes", "commercial", "industrial", "fishing"}:
        port_type = "harbour"
    elif seamark_type == "harbour":
        port_type = "harbour"
    elif harbour_tag:
        port_type = harbour_tag   # e.g. 'ferry', 'private', 'military'
    else:
        port_type = "harbour"     # landuse=harbour with no harbour tag

    if exclude_marina and port_type == "marina":
        return None

    return {
        "osm_id":       elem["id"],
        "osm_type":     osm_type,
        "latitude":     lat,
        "longitude":    lon,
        "name":         tags.get("name") or tags.get("ref:name") or None,
        "operator":     tags.get("operator") or None,
        "harbour":      harbour_tag,
        "seamark_type": seamark_type,
        "amenity":      amenity,
        "port_type":    port_type,
    }


# Entry point

def main() -> int:
    """
    Command-line entry point.

    Returns: Exit code: 0 on success, 1 on error.
    """
    parser = argparse.ArgumentParser(
        description="Download port/harbour features from OpenStreetMap via Overpass.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s --out /mnt/e/data_lake/ports/osm_ports.csv
  %(prog)s --exclude-marina
        """,
    )
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT, metavar="PATH",
        help=f"Output CSV path (default: {DEFAULT_OUT})",
    )
    parser.add_argument(
        "--exclude-marina", action="store_true",
        help="Skip marina features (port_type=marina)",
    )
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)

    print("Fetching port/harbour features from OpenStreetMap Overpass API ...")
    print(f"Output: {args.out}")
    if args.exclude_marina:
        print("Filter: marinas excluded")
    print()

    seen_ids: set[int] = set()
    total_elements = 0
    total_written  = 0

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        for region_name, south, west, north, east in REGIONS:
            elements = _fetch_region(region_name, south, west, north, east)
            total_elements += len(elements)

            for elem in elements:
                osm_id = elem.get("id")
                if osm_id in seen_ids:
                    continue
                seen_ids.add(osm_id)

                row = _element_to_row(elem, args.exclude_marina)
                if row is None:
                    continue
                writer.writerow(row)
                total_written += 1

            if elements:
                time.sleep(REQUEST_PAUSE)

    print()
    print("Done.")
    print(f"  Elements fetched : {total_elements:,}")
    print(f"  Rows written     : {total_written:,} (deduplicated)")
    print(f"  Output           : {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
