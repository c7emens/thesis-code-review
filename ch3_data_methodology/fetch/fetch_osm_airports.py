#!/usr/bin/env python3
# Download airport, heliport, and helipad features from OSM via Overpass API.
#
# Queries the Overpass API in continental bounding-box chunks (same strategy as
# fetch_osm_ports.py) and writes a deduplicated CSV.  Handles nodes and ways —
# ways return their centroid via Overpass `out center`.
#
# OSM tags queried
#   aeroway=aerodrome   — all airports and airfields
#   aeroway=heliport    — dedicated helicopter facilities
#   aeroway=helipad     — individual helicopter landing pads
#
# CSV columns
#   osm_id, osm_type, latitude, longitude, name, operator,
#   icao, iata, aeroway, ele_m
#
# Usage:
#   python fetch_osm_airports.py
#   python fetch_osm_airports.py --out /mnt/e/data_lake/airports/osm_airports.csv
#   python fetch_osm_airports.py --type heliport
#   python fetch_osm_airports.py --type helipad
#   python fetch_osm_airports.py --start-from "Europe East"

import argparse
import csv
import sys
import time
from pathlib import Path

import requests


# Constants

## Default output path.
DEFAULT_OUT = Path("/mnt/e/data_lake/airports/osm_airports.csv")

## Overpass API endpoint.
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

## Seconds to wait between regional chunk requests (rate limiting).
REQUEST_PAUSE = 8

## Maximum retries per chunk on HTTP error / timeout.
MAX_RETRIES = 3

## Seconds to wait before retrying a failed request.
RETRY_PAUSE = 45

## Valid aeroway type filter values.
VALID_TYPES = {"aerodrome", "heliport", "helipad"}

## CSV column order.
CSV_COLUMNS = [
    "osm_id",
    "osm_type",
    "latitude",
    "longitude",
    "name",
    "operator",
    "icao",
    "iata",
    "aeroway",
    "ele_m",
]

## Regional bounding boxes [south, west, north, east].
REGIONS = [
    ("North America",       20.0, -170.0,  72.0,  -50.0),
    ("Europe West",         35.0,  -15.0,  72.0,   20.0),
    ("Europe East",         35.0,   20.0,  72.0,   45.0),
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
    Build an Overpass QL query for airport/heliport/helipad features
    in a bounding box.

    Unions three aeroway tag predicates across node and way element types.
    `out center` returns the centroid lat/lon for way elements.

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
        f'  node["aeroway"="aerodrome"]({bbox});'
        f'  way["aeroway"="aerodrome"]({bbox});'
        f'  node["aeroway"="heliport"]({bbox});'
        f'  way["aeroway"="heliport"]({bbox});'
        f'  node["aeroway"="helipad"]({bbox});'
        f'  way["aeroway"="helipad"]({bbox});'
        f');'
        f'out center;'
    )


# Fetch

def _fetch_region(name: str, south: float, west: float,
                  north: float, east: float) -> list[dict]:
    """
    Fetch all airport/heliport/helipad features within a regional bbox.

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

def _parse_ele(raw: str | None) -> float | None:
    """
    Parse an elevation tag value to metres.

    Strips trailing 'm' suffix; returns None if unparseable.

    raw: Raw tag value string, or None.
    Returns: Elevation in metres as float, or None.
    """
    if not raw:
        return None
    s = raw.strip().lower()
    if s.endswith("m"):
        s = s[:-1].strip()
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _element_to_row(elem: dict, type_filter: str | None) -> dict | None:
    """
    Convert a raw OSM element dict to a CSV row dict.

    Extracts lat/lon from `lat`/`lon` (nodes) or `center` (ways).

    elem: Raw OSM element dict from Overpass.
    type_filter: If set, skip elements whose aeroway tag != type_filter.
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

    aeroway = tags.get("aeroway") or None

    if type_filter and aeroway != type_filter:
        return None

    return {
        "osm_id":   elem["id"],
        "osm_type": osm_type,
        "latitude": lat,
        "longitude": lon,
        "name":     tags.get("name") or tags.get("ref:name") or None,
        "operator": tags.get("operator") or None,
        "icao":     tags.get("icao") or tags.get("ref:icao") or None,
        "iata":     tags.get("iata") or tags.get("ref:iata") or None,
        "aeroway":  aeroway,
        "ele_m":    _parse_ele(tags.get("ele")),
    }


# Entry point

def main() -> int:
    """
    Command-line entry point.

    Returns: Exit code: 0 on success, 1 on error.
    """
    parser = argparse.ArgumentParser(
        description="Download airport/heliport/helipad features from OSM via Overpass.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s --out /mnt/e/data_lake/airports/osm_airports.csv
  %(prog)s --type heliport
  %(prog)s --type helipad
  %(prog)s --start-from "Europe East"
        """,
    )
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT, metavar="PATH",
        help=f"Output CSV path (default: {DEFAULT_OUT})",
    )
    parser.add_argument(
        "--type", choices=sorted(VALID_TYPES), default=None, metavar="TYPE",
        help="Only write features with this aeroway type (aerodrome/heliport/helipad)",
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
        file_mode = "a"
        print(f"Resuming from region #{idx + 1}: '{args.start_from}' "
              f"(appending to {args.out.name})")

    print("Fetching airport/heliport/helipad features from OpenStreetMap Overpass API ...")
    print(f"Output: {args.out}")
    if args.type:
        print(f"Filter: aeroway={args.type} only")
    print()

    seen_ids: set[int] = set()
    total_elements = 0
    total_written  = 0

    with open(args.out, file_mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if file_mode == "w":
            writer.writeheader()

        for region_name, south, west, north, east in regions_to_run:
            elements = _fetch_region(region_name, south, west, north, east)
            total_elements += len(elements)

            for elem in elements:
                osm_id = elem.get("id")
                if osm_id in seen_ids:
                    continue
                seen_ids.add(osm_id)

                row = _element_to_row(elem, args.type)
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
