#!/usr/bin/env python3
# Fetch helicopter positions near offshore wind turbines from OpenSky Network.
#
# OpenSky captures low-altitude offshore positions via satellite ADS-B (Aireon/Spire),
# filling the blind spot that ground-based ADS-B receivers (FlightRadar24) cannot cover.
#
# Strategy:
#   1. Load helicopter ICAOs from local TimescaleDB (aircraft_desc ILIKE '%heli%').
#   2. Compute turbine bounding box from wind_turbines table (+ buffer).
#   3. Query OpenSky Trino month-by-month — each month stays well under the 30-min
#      query timeout.
#   4. Write matching positions to CSV.
#
# Usage:
#   python fetch_opensky_helicopters.py --project Block_Island
#   python fetch_opensky_helicopters.py --year 2024 --max-alt 2000
#   python fetch_opensky_helicopters.py --start 2024-01-01 --end 2024-07-01 --output out.csv

import argparse
import collections
import csv
import math
import signal
import os
import sys
from dotenv import load_dotenv; load_dotenv()
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import trino
import trino.auth
from opensky_auth import make_oauth2_auth

# Local database

## Default local TimescaleDB connection parameters.
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "windfarm",
    "user": "thesis",
    "password": "thesis2026",
}

# OpenSky Trino

## OpenSky Trino gateway hostname.
OPENSKY_HOST   = "trino.opensky-network.org"
## OpenSky Trino gateway port (HTTPS).
OPENSKY_PORT   = 443
## Trino catalog name for OpenSky data.
OPENSKY_CATALOG = "minio"
## Trino schema name for OpenSky data.
OPENSKY_SCHEMA  = "osky"

# Search parameters

## Barometric altitude ceiling in feet for position filtering.
MAX_ALT_FT      = 2000
## Bounding-box buffer around turbines in kilometres.
BUFFER_KM       = 20

# Output columns

## Ordered list of column names written to the output CSV.
OUTPUT_FIELDS = [
    "icao24", "registration", "aircraft_desc",
    "time_utc",
    "lat", "lon", "alt_ft",
    "groundspeed_kts", "heading",
    "n_sensors",
]

# OpenSky queries

## Trino SQL template for querying positions filtered by known helicopter ICAOs.
OPENSKY_QUERY = """
    SELECT
        icao24,
        mintime,
        lat,
        lon,
        alt,
        groundspeed,
        heading,
        CARDINALITY(sensors) AS n_sensors
    FROM {catalog}.{schema}.position_data4
    WHERE hour >= {hour_start}
      AND hour <  {hour_end}
      AND lat  BETWEEN {lat_min} AND {lat_max}
      AND lon  BETWEEN {lon_min} AND {lon_max}
      AND alt  IS NOT NULL
      AND alt  < {max_alt}
      AND icao24 IN ({icao_list})
      AND (surface IS NULL OR surface = false)
    ORDER BY icao24, mintime
"""

## Trino SQL template for querying all low-altitude aircraft (no ICAO filter).
OPENSKY_QUERY_NO_ICAO_FILTER = """
    SELECT
        icao24,
        mintime,
        lat,
        lon,
        alt,
        groundspeed,
        heading,
        CARDINALITY(sensors) AS n_sensors
    FROM {catalog}.{schema}.position_data4
    WHERE hour >= {hour_start}
      AND hour <  {hour_end}
      AND lat  BETWEEN {lat_min} AND {lat_max}
      AND lon  BETWEEN {lon_min} AND {lon_max}
      AND alt  IS NOT NULL
      AND alt  < {max_alt}
      AND (surface IS NULL OR surface = false)
    ORDER BY icao24, mintime
"""


# Helpers

def get_helicopter_icaos(conn, project_name=None) -> dict[str, dict]:
    """
    Return ICAO metadata for all helicopters in the local flight_data_adsb_over_ground table.

    Scans only one recent chunk (one month) to avoid a full-table scan on the
    663 GB compressed hypertable. Spatial filtering by project is handled
    separately via the OpenSky bbox query.

    conn: Active psycopg2 database connection.
    project_name: Unused; reserved for future per-project filtering.
    Returns: Dict mapping lowercase ICAO hex to {registration, aircraft_desc}.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # Scan only one recent chunk (one month) to avoid full-table scan on
    # 663 GB compressed hypertable. Any month with helicopter activity works.
    cur.execute("""
            SELECT DISTINCT icao, registration, aircraft_desc
            FROM flight_data_adsb_over_ground
            WHERE aircraft_desc ILIKE '%%heli%%'
              AND time >= '2024-06-01'
              AND time <  '2024-07-01'
        """)
    rows = cur.fetchall()
    cur.close()
    return {r["icao"].lower(): dict(r) for r in rows}


def get_farm_bbox(conn, project_name=None, buffer_km=BUFFER_KM) -> dict:
    """
    Compute the lat/lon bounding box covering target turbines plus a buffer.

    conn: Active psycopg2 database connection.
    project_name: If provided, restrict to turbines in this project.
    buffer_km: Buffer distance in kilometres to expand the bounding box.
    Returns: Dict with keys lat_min, lat_max, lon_min, lon_max.
    """
    cur = conn.cursor()
    if project_name:
        cur.execute(
            "SELECT MIN(latitude), MAX(latitude), MIN(longitude), MAX(longitude) "
            "FROM wind_turbines WHERE project_name = %s", (project_name,)
        )
    else:
        cur.execute(
            "SELECT MIN(latitude), MAX(latitude), MIN(longitude), MAX(longitude) "
            "FROM wind_turbines"
        )
    lat_min, lat_max, lon_min, lon_max = cur.fetchone()
    cur.close()
    buf_lat = buffer_km / 111.0
    buf_lon = buffer_km / (111.0 * math.cos(math.radians((lat_min + lat_max) / 2)))
    return {
        "lat_min": lat_min - buf_lat,
        "lat_max": lat_max + buf_lat,
        "lon_min": lon_min - buf_lon,
        "lon_max": lon_max + buf_lon,
    }


def iter_chunks(start: datetime, end: datetime, chunk_days: int):
    """
    Yield (chunk_start, chunk_end) pairs of chunk_days width covering [start, end).

    start: Start of the overall time range (UTC).
    end: End of the overall time range (UTC, exclusive).
    chunk_days: Width of each chunk in days.
    """
    delta = timedelta(days=chunk_days)
    cur = start.replace(hour=0, minute=0, second=0, microsecond=0)
    while cur < end:
        nxt = cur + delta
        yield cur, min(nxt, end)
        cur = nxt


def list_projects(conn):
    """
    Print all available project names and their turbine counts to stdout.

    conn: Active psycopg2 database connection.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT project_name, COUNT(*) AS turbines "
        "FROM wind_turbines GROUP BY project_name ORDER BY project_name"
    )
    print("Available projects:")
    for row in cur.fetchall():
        print(f"  {row[0]:<25} ({row[1]} turbines)")
    cur.close()


# Main

def main():
    """
    Command-line entry point.

    Returns: Exit code (0 on success, 1 if no helicopter ICAOs found).
    """
    parser = argparse.ArgumentParser(
        description="Fetch helicopter positions near offshore wind turbines from OpenSky.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --list-projects
  %(prog)s --project Block_Island --output block_island_helicopters.csv
  %(prog)s --year 2024
  %(prog)s --start 2024-01-01 --end 2024-07-01 --max-alt 1500
        """,
    )
    parser.add_argument("--project", "-p", metavar="NAME",
                        help="Filter by project name. Omit for all projects.")
    parser.add_argument("--start", metavar="YYYY-MM-DD", default="2024-01-01",
                        help="Start date (default: 2024-01-01)")
    parser.add_argument("--end",   metavar="YYYY-MM-DD", default="2025-01-01",
                        help="End date, exclusive (default: 2025-01-01)")
    parser.add_argument("--year", metavar="YYYY", type=int,
                        help="Shortcut: cover a full calendar year")
    parser.add_argument("--max-alt", metavar="FEET", type=int, default=MAX_ALT_FT,
                        help=f"Altitude ceiling in feet AMSL (default: {MAX_ALT_FT})")
    parser.add_argument("--buffer", metavar="KM", type=float, default=BUFFER_KM,
                        help=f"Bbox buffer around turbines in km (default: {BUFFER_KM})")
    parser.add_argument("--output", "-o", metavar="FILE",
                        default="opensky_helicopters.csv",
                        help="Output CSV file (default: opensky_helicopters.csv)")
    parser.add_argument("--icao", metavar="ICAO[,ICAO,...]",
                        help="Comma-separated ICAO hex codes to search (skips DB lookup). "
                             "Example: --icao a0ab4f,ab3fbf,a0b674")
    parser.add_argument("--chunk-days", metavar="N", type=int, default=30,
                        help="Query chunk size in days (default: 30). "
                             "Use 1 for day-by-day, 7 for weekly.")
    parser.add_argument("--user", metavar="EMAIL",
                        default=os.environ.get("OPENSKY_USERNAME"),
                        help="OpenSky username. Defaults to $OPENSKY_USERNAME (from /mnt/d/thesis/.env). Password auth also reads $OPENSKY_PASS or $OPENSKY_PASSWORD from .env.")
    parser.add_argument("--all-aircraft", action="store_true",
                        help="Skip ICAO filter — fetch ALL low-altitude aircraft in the bbox. "
                             "Useful for discovery when specific ICAOs are unknown.")
    parser.add_argument("--list-projects", action="store_true",
                        help="List available projects and exit")
    args = parser.parse_args()

    local_conn = psycopg2.connect(**DB_CONFIG)

    if args.list_projects:
        list_projects(local_conn)
        local_conn.close()
        return 0

    if args.year:
        args.start = f"{args.year}-01-01"
        args.end   = f"{args.year + 1}-01-01"

    t_start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    t_end   = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    months  = list(iter_chunks(t_start, t_end, args.chunk_days))

    # Load helicopter ICAOs — fast path: user-supplied list; slow path: DB scan
    if args.all_aircraft:
        icao_meta = {}
        icao_sql_list = None
        print("⚠️  --all-aircraft: ICAO filter disabled — fetching all low-altitude traffic in bbox.")
    elif args.icao:
        supplied = [h.strip().lower() for h in args.icao.split(",") if h.strip()]
        icao_meta = {icao: {"icao": icao, "registration": "", "aircraft_desc": ""
                            } for icao in supplied}
        icao_sql_list = ", ".join(f"'{k}'" for k in sorted(icao_meta))
        print(f"Using {len(icao_meta)} user-supplied ICAOs (skipping DB lookup).")
    else:
        print("Looking up helicopter ICAOs from local DB...", end=" ", flush=True)
        icao_meta = get_helicopter_icaos(local_conn, args.project)
        print(f"{len(icao_meta)} found.")
        if not icao_meta:
            print("❌  No helicopter ICAOs. Use --icao a0ab4f,... or --all-aircraft.")
            local_conn.close()
            return 1
        icao_sql_list = ", ".join(f"'{k}'" for k in sorted(icao_meta))

    # Bounding box
    bbox = get_farm_bbox(local_conn, args.project, buffer_km=args.buffer)
    local_conn.close()

    print(f"Parameters:")
    print(f"  Project       : {args.project or 'all'}")
    print(f"  Date range    : {t_start.date()} → {t_end.date()} ({len(months)} chunks × {args.chunk_days}d)")
    print(f"  Max altitude  : {args.max_alt} ft")
    print(f"  Helicopters   : {len(icao_meta)} unique ICAOs")
    for icao, meta in sorted(icao_meta.items()):
        print(f"    {icao}  {meta.get('registration','?'):<10}  {meta.get('aircraft_desc','?')}")
    print(f"  Bbox          : lat [{bbox['lat_min']:.2f}, {bbox['lat_max']:.2f}]  "
          f"lon [{bbox['lon_min']:.2f}, {bbox['lon_max']:.2f}]")
    print(f"  Output        : {args.output}")
    print()
    print("Connecting to OpenSky Trino...")
    _auth = make_oauth2_auth()
    print()

    sky_conn = trino.dbapi.connect(
        host=OPENSKY_HOST,
        port=OPENSKY_PORT,
        http_scheme="https",
        user=args.user or "",
        auth=_auth,
        catalog=OPENSKY_CATALOG,
        schema=OPENSKY_SCHEMA,
        request_timeout=1800,   # 30-minute per-query timeout
    )

    # Cancel the active Trino query on Ctrl+C so it doesn't linger in the queue.
    _active_cursor = [None]
    def _sigint_handler(sig, frame):
        if _active_cursor[0] is not None:
            try:
                print("\nCancelling Trino query...", flush=True)
                _active_cursor[0].cancel()
            except Exception:
                pass
        sys.exit(1)
    signal.signal(signal.SIGINT, _sigint_handler)

    total_rows   = 0
    icao_totals  = collections.Counter()
    session_start = time.monotonic()
    month_times  = []

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()

        for i, (month_start, month_end) in enumerate(months, 1):
            hour_start = int(month_start.timestamp())
            hour_end   = int(month_end.timestamp())

            template = OPENSKY_QUERY_NO_ICAO_FILTER if args.all_aircraft else OPENSKY_QUERY
            fmt_args = dict(
                catalog=OPENSKY_CATALOG,
                schema=OPENSKY_SCHEMA,
                hour_start=hour_start,
                hour_end=hour_end,
                lat_min=bbox["lat_min"],
                lat_max=bbox["lat_max"],
                lon_min=bbox["lon_min"],
                lon_max=bbox["lon_max"],
                max_alt=args.max_alt,
            )
            if not args.all_aircraft:
                fmt_args["icao_list"] = icao_sql_list
            query = template.format(**fmt_args)

            ts = datetime.now().strftime("%H:%M:%S")
            print(f"  [{i:>2}/{len(months)}] {month_start.strftime('%Y-%m-%d')}  (started {ts})",
                  flush=True)
            t0 = time.monotonic()

            cur = sky_conn.cursor()
            _active_cursor[0] = cur

            # Trino's execute() blocks silently while polling the server for
            # the first result page. Run a heartbeat thread so the terminal
            # shows progress instead of appearing frozen.
            _stop = threading.Event()
            def _heartbeat():
                t0h = time.monotonic()
                while not _stop.is_set():
                    elapsed = time.monotonic() - t0h
                    stats   = cur.stats  # updated live by trino client while polling

                    if stats is None:
                        if elapsed < 20:
                            msg = "waiting for auth — open the URL above in your browser"
                        else:
                            msg = "query submitted, waiting for cluster..."
                    else:
                        state   = stats.get("state", "?")
                        total_s = stats.get("totalSplits", 0)
                        done_s  = stats.get("completedSplits", 0)
                        rows    = stats.get("processedRows", 0)
                        gb      = stats.get("processedBytes", 0) / 1e9
                        if total_s > 0:
                            pct = 100 * done_s / total_s
                            bar = ("█" * int(pct / 5)).ljust(20, "░")
                            msg = (f"{bar} {pct:5.1f}%  "
                                   f"{done_s}/{total_s} splits  "
                                   f"{rows/1e6:.1f}M rows  {gb:.2f} GB")
                        else:
                            msg = f"{state}"

                    print(f"\r    [{elapsed:>4.0f}s] {msg:<72}", end="", flush=True)
                    _stop.wait(5)

            _hb = threading.Thread(target=_heartbeat, daemon=True)
            _hb.start()

            # Retry on QUERY_QUEUE_FULL (previous Ctrl+C left queries queued).
            for attempt in range(1, 6):
                try:
                    cur.execute(query)
                    break
                except Exception as e:
                    if "QUERY_QUEUE_FULL" in str(e) and attempt < 5:
                        _stop_msg = f"queue full, retrying in 60s (attempt {attempt}/5)..."
                        print(f"\n    ⚠️  {_stop_msg}", flush=True)
                        time.sleep(60)
                        cur = sky_conn.cursor()
                        _active_cursor[0] = cur
                    else:
                        _stop.set()
                        _hb.join()
                        raise

            _stop.set()
            _hb.join()
            print()  # end the overwrite line
            print(f"    Query ID : {cur.query_id}", flush=True)

            # fetchmany loop: gives a heartbeat every ~10 000 rows so the
            # terminal doesn't go silent for minutes during large months.
            rows = []
            while True:
                chunk = cur.fetchmany(10_000)
                if not chunk:
                    break
                rows.extend(chunk)
                print(f"    ... {len(rows):,} rows so far", flush=True)

            stats = cur.stats or {}
            scanned_rows  = stats.get("processedRows", "?")
            scanned_bytes = stats.get("processedBytes", 0)
            scanned_gb    = f"{scanned_bytes / 1e9:.1f} GB" if scanned_bytes else "?"
            print(f"    Scanned  : {scanned_rows:,} rows / {scanned_gb}"
                  if isinstance(scanned_rows, int) else
                  f"    Scanned  : {scanned_rows} rows / {scanned_gb}", flush=True)
            cur.close()

            elapsed = time.monotonic() - t0
            month_times.append(elapsed)

            if not rows:
                print(f"  → 0 rows  ({elapsed:.0f}s)")
                continue

            month_icaos = collections.Counter()
            for row in rows:
                icao24, mintime, lat, lon, alt, speed, hdg, n_sensors = row
                meta   = icao_meta.get(icao24, {})
                dt_utc = datetime.fromtimestamp(mintime, tz=timezone.utc)
                writer.writerow({
                    "icao24":          icao24,
                    "registration":    meta.get("registration", ""),
                    "aircraft_desc":   meta.get("aircraft_desc", ""),
                    "time_utc":        dt_utc.isoformat(),
                    "lat":             round(lat, 6),
                    "lon":             round(lon, 6),
                    "alt_ft":          round(alt, 1),
                    "groundspeed_kts": round(speed, 1) if speed is not None else "",
                    "heading":         round(hdg, 1)   if hdg   is not None else "",
                    "n_sensors":       n_sensors,
                })
                month_icaos[icao24] += 1
                icao_totals[icao24] += 1

            total_rows += len(rows)

            # Remaining time estimate
            avg_t = sum(month_times) / len(month_times)
            remaining = avg_t * (len(months) - i)
            eta_str = f"  ETA ~{remaining/60:.0f} min remaining" if i < len(months) else ""

            print(f"  → {len(rows):,} rows  ({elapsed:.0f}s){eta_str}")
            for icao24, cnt in sorted(month_icaos.items(), key=lambda x: -x[1]):
                reg = icao_meta.get(icao24, {}).get("registration", icao24)
                print(f"         {reg:<10} {cnt:>5} positions")

    sky_conn.close()

    total_elapsed = time.monotonic() - session_start
    print(f"\n✅  Done in {total_elapsed/60:.1f} min. "
          f"{total_rows:,} total positions → {args.output}")

    if icao_totals:
        print("\nPositions by aircraft (all months):")
        for icao24, cnt in sorted(icao_totals.items(), key=lambda x: -x[1]):
            meta = icao_meta.get(icao24, {})
            print(f"  {meta.get('registration','?'):<10}  {icao24}  "
                  f"{cnt:>6} pos  {meta.get('aircraft_desc','?')}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
