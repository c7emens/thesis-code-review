# Find vessel positions near wind turbines and report distances.
#
# Queries vessel_data_ais joined spatially against wind_turbines using
# ST_DWithin, and returns a DataFrame with vessel data, turbine metadata,
# and the distance in metres between each position and the nearest turbine.
#
# Usage:
#   python vessels_near_turbines.py --project Block_Island --radius 5000
#   python vessels_near_turbines.py --turbines B1 B2 --output results.csv

import argparse
import psycopg2
import pandas as pd

## Default database connection parameters.
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "windfarm",
    "user": "thesis",
    "password": "thesis2026",
}


def get_vessels_near_turbines(project_name=None, turbine_codes=None, radius_meters=5000, limit=None):
    """
    Find vessel positions within a radius of wind turbines.

    project_name: Filter by project name (e.g. 'Block_Island'). Mutually
                          exclusive with turbine_codes.
    turbine_codes: List of specific turbine codes (e.g. ['B1', 'B2']). Used
                          only when project_name is None.
    radius_meters: Horizontal search radius around each turbine in metres
                          (default 5 000).
    limit: Maximum number of rows to return (None = unlimited).
    Returns: DataFrame with vessel positions, turbine info, and distance.
    """
    conn = psycopg2.connect(**DB_CONFIG)

    # build query with filters
    turbine_filter = ""
    params = {"radius": radius_meters}

    if project_name:
        turbine_filter = "AND t.project_name = %(project_name)s"
        params["project_name"] = project_name
    elif turbine_codes:
        turbine_filter = "AND t.turbine_code = ANY(%(turbine_codes)s)"
        params["turbine_codes"] = turbine_codes

    limit_clause = f"LIMIT {limit}" if limit else ""

    query = f"""
        SELECT
            -- Vessel data
            v.mms_id,
            v.time AS vessel_time,
            v.latitude AS vessel_lat,
            v.longitude AS vessel_lon,
            v.speed_over_ground,
            v.course_over_ground,
            v.heading,
            v.vessel_name,
            v.vessel_type,

            -- Turbine data
            t.turbine_code,
            t.turbine_name,
            t.project_name,
            t.latitude AS turbine_lat,
            t.longitude AS turbine_lon,

            -- Distance in meters
            ROUND(ST_Distance(v.location, t.location)::numeric, 2) AS distance_meters

        FROM vessel_data_ais v
        JOIN wind_turbines t
            ON ST_DWithin(v.location, t.location, %(radius)s)
        WHERE 1=1
        {turbine_filter}
        ORDER BY t.turbine_code, v.time
        {limit_clause}
    """

    print(f"Querying vessels within {radius_meters}m of turbines...")
    df = pd.read_sql(query, conn, params=params)
    conn.close()

    print(f"Found {len(df):,} vessel positions near turbines")
    return df


def main():
    """
    Command-line entry point.

    Returns: Exit code (0 on success, 1 on missing arguments).
    """
    parser = argparse.ArgumentParser(description="Find vessels near wind turbines")
    parser.add_argument("--project", "-p", help="Project name (e.g., Block_Island)")
    parser.add_argument("--turbines", "-t", nargs="+", help="Specific turbine codes (e.g., B1 B2)")
    parser.add_argument("--radius", "-r", type=int, default=5000, help="Radius in meters (default: 5000)")
    parser.add_argument("--limit", "-l", type=int, help="Limit results")
    parser.add_argument("--output", "-o", help="Output CSV file")
    args = parser.parse_args()

    if not args.project and not args.turbines:
        print("Specify --project or --turbines")
        print("Example: python vessels_near_turbines.py --project Block_Island --radius 5000")
        return 1

    df = get_vessels_near_turbines(
        project_name=args.project,
        turbine_codes=args.turbines,
        radius_meters=args.radius,
        limit=args.limit
    )

    if df.empty:
        print("No vessels found")
        return 0

    # Summary
    print(f"\nSummary:")
    print(f"  Unique vessels: {df['mms_id'].nunique():,}")
    print(f"  Turbines with traffic: {df['turbine_code'].nunique()}")
    print(f"  Time range: {df['vessel_time'].min()} to {df['vessel_time'].max()}")
    print(f"  Min distance: {df['distance_meters'].min():.0f}m")
    print(f"  Avg distance: {df['distance_meters'].mean():.0f}m")

    # Output
    if args.output:
        df.to_csv(args.output, index=False)
        print(f"\nSaved to: {args.output}")
    else:
        print(f"\nSample (first 10 rows):")
        print(df.head(10).to_string())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
