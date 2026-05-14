#!/usr/bin/env python3
"""
Helicopter position outlier detection — Stage 2 post-processing.

Reads ADS-B helicopter tracks from a CSV or Parquet file (Stage 2 output),
removes position outliers using a 3D theoretical-velocity approach
(Baumgärtner et al. 2024 §2.2, extended with altitude component),
and writes the cleaned tracks to a new file.

The 3D extension avoids false positives during takeoff and landing:
steep climbs/descents inflate horizontal-only theoretical speed, but the
combined 3D distance reflects the actual displacement more accurately.

Usage examples
--------------
# Basic run — CSV input/output
python run_helicopter_outliers.py --input stage2_helicopters.csv

# Parquet input, custom output path
python run_helicopter_outliers.py --input stage2_helicopters.parquet \\
    --output cleaned_helicopters.parquet

# Dry run — stats only
python run_helicopter_outliers.py --input stage2_helicopters.csv --dry-run

# Custom thresholds + verbose per-aircraft log
python run_helicopter_outliers.py --input stage2_helicopters.csv \\
    --max-speed 180 --abs-threshold 30 --rel-factor 3 --max-alt-jump 4000 --verbose

# Filter to a single aircraft
python run_helicopter_outliers.py --input stage2_helicopters.csv --icao a1b2c3
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from detect_outliers import clean_tracks, MAX_SPEED_HELI_KT, MAX_ALT_JUMP_FT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SUPPORTED_FORMATS = {".csv", ".parquet"}


# CLI

def parse_args():
    parser = argparse.ArgumentParser(
        description="Detect and remove position outliers from ADS-B helicopter tracks.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Input / output
    parser.add_argument(
        "--input", "-i", required=True, metavar="FILE",
        help="Path to Stage 2 helicopter track file (.csv or .parquet).",
    )
    parser.add_argument(
        "--output", "-o", metavar="FILE",
        help="Output file path. Defaults to <input>_cleaned.<ext>.",
    )

    # Optional filters
    parser.add_argument(
        "--icao", metavar="ICAO24",
        help="Process a single aircraft ICAO hex code only (default: all).",
    )
    parser.add_argument(
        "--start", metavar="YYYY-MM-DD",
        help="Filter to records on or after this date.",
    )
    parser.add_argument(
        "--end", metavar="YYYY-MM-DD",
        help="Filter to records before this date.",
    )

    # Thresholds
    parser.add_argument(
        "--max-speed", type=float, default=MAX_SPEED_HELI_KT, metavar="KT",
        help=f"Absolute max realistic speed in knots (default: {MAX_SPEED_HELI_KT}).",
    )
    parser.add_argument(
        "--abs-threshold", type=float, default=30.0, metavar="KT",
        help="Absolute divergence threshold between theoretical and reported speed (default: 30).",
    )
    parser.add_argument(
        "--rel-factor", type=float, default=3.0, metavar="X",
        help="Relative factor: theoretical / reported > X flags outlier (default: 3).",
    )
    parser.add_argument(
        "--max-alt-jump", type=float, default=MAX_ALT_JUMP_FT, metavar="FT",
        help=f"Max altitude change per step in feet before flagging as ADS-B glitch "
             f"(default: {MAX_ALT_JUMP_FT}).",
    )

    # Behaviour
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print statistics only — do not write output file.",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Show per-aircraft outlier counts.",
    )
    parser.add_argument(
        "--format", choices=["csv", "parquet"], dest="out_format",
        help="Force output format (default: match input).",
    )

    return parser.parse_args()


# I/O helpers

def load_file(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        log.error("Input file not found: %s", path)
        sys.exit(1)
    if p.suffix not in SUPPORTED_FORMATS:
        log.error("Unsupported format '%s'. Use .csv or .parquet.", p.suffix)
        sys.exit(1)

    log.info("Loading %s …", path)
    if p.suffix == ".csv":
        df = pd.read_csv(path, parse_dates=["time"])
    else:
        df = pd.read_parquet(path)

    log.info("  Loaded %d records for %d aircraft.", len(df), df["icao24"].nunique())
    return df


def save_file(df: pd.DataFrame, path: str, fmt: str):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "csv":
        df.to_csv(path, index=False)
    else:
        df.to_parquet(path, index=False)
    log.info("Saved cleaned tracks → %s  (%d records)", path, len(df))


def default_output_path(input_path: str, fmt: str) -> str:
    p = Path(input_path)
    return str(p.parent / f"{p.stem}_cleaned.{fmt}")


# Main

def main():
    args = parse_args()

    # Inject thresholds into detect_outliers module
    import detect_outliers as _do
    _do.MAX_SPEED_HELI_KT     = args.max_speed
    _do.ABS_THRESHOLD_HELI_KT = args.abs_threshold
    _do.REL_FACTOR_HELI       = args.rel_factor
    _do.MAX_ALT_JUMP_FT       = args.max_alt_jump

    df = load_file(args.input)

    # Apply filters
    if args.icao:
        df = df[df["icao24"] == args.icao.lower()]
        log.info("Filtered to ICAO %s: %d records.", args.icao, len(df))
    if args.start:
        df = df[df["time"] >= pd.Timestamp(args.start)]
    if args.end:
        df = df[df["time"] < pd.Timestamp(args.end)]

    if df.empty:
        log.warning("No records after filtering. Exiting.")
        sys.exit(0)

    # Ensure time column is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["time"]):
        df["time"] = pd.to_datetime(df["time"])

    # Run detection
    log.info("Running outlier detection (helicopter mode) …")
    cleaned = clean_tracks(df, id_col="icao24", mode="helicopter")

    n_before  = len(df)
    n_after   = len(cleaned)
    n_removed = n_before - n_after

    # Verbose: per-aircraft breakdown
    if args.verbose and n_removed > 0:
        removed = df[~df.set_index(["icao24", "time"]).index.isin(
            cleaned.set_index(["icao24", "time"]).index
        )]
        per_ac = removed.groupby("icao24").size().sort_values(ascending=False)
        log.info("Per-aircraft outliers removed:")
        for icao, count in per_ac.items():
            log.info("    %-8s  %d removed", icao, count)

    log.info(
        "\nSummary: %d outliers removed from %d records (%.3f%%)",
        n_removed, n_before,
        100 * n_removed / n_before if n_before else 0,
    )

    if args.dry_run:
        log.info("Dry run — no output written.")
        return

    # Determine output format and path
    in_ext = Path(args.input).suffix.lstrip(".")
    out_fmt = args.out_format or in_ext
    out_path = args.output or default_output_path(args.input, out_fmt)

    save_file(cleaned, out_path, out_fmt)


if __name__ == "__main__":
    main()
