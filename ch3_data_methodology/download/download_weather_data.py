#!/usr/bin/env python3
# Download ICOADS Global Marine weather data archives from NOAA NCEI.
#
# Files are named YYYYMM.tar.gz and contain CSV files with marine weather
# observations. The script downloads one archive per month for the requested
# range and optionally extracts each archive into a named subfolder.
#
# Usage:
#   python download_weather_data.py --start 2020-01 --end 2025-07 --output /mnt/e/data_lake/weather-marine
#   python download_weather_data.py --start 2024-01 --end 2024-12 --extract --skip-existing

import argparse
import os
import tarfile
from datetime import datetime
from pathlib import Path
import urllib.request

# URL patterns - add new patterns here as needed
URL_PATTERNS = {
    "default": {
        "base_url": "https://www.ncei.noaa.gov/data/global-marine/archive",
        "filename": "{year}{month:02d}.tar.gz",
    },
    # Add alternative patterns here if needed:
    # "alternative": {
    #     "base_url": "https://example.com/weather",
    #     "filename": "weather_{year}_{month:02d}.tar.gz",
    # },
}

## Default URL pattern name.
DEFAULT_PATTERN = "default"


def get_download_url(year: int, month: int, pattern_name: str) -> tuple[str, str]:
    """
    Build the download URL and filename for a given year/month combination.

    year: Four-digit year.
    month: Month number (1–12).
    pattern_name: Key into URL_PATTERNS.
    Returns: Tuple of (full_url, filename).
    @raises ValueError  If pattern_name is not found in URL_PATTERNS.
    """
    if pattern_name not in URL_PATTERNS:
        available = ", ".join(URL_PATTERNS.keys())
        raise ValueError(f"Unknown pattern '{pattern_name}'. Available: {available}")

    pattern = URL_PATTERNS[pattern_name]
    filename = pattern["filename"].format(year=year, month=month)
    url = f"{pattern['base_url']}/{filename}"
    return url, filename


def download_file(url: str, dest: Path) -> bool:
    """
    Download a file from a URL and save it to disk.

    url: Source URL.
    dest: Destination path.
    Returns: True on success, False on HTTP or other error.
    """
    try:
        print(f"  Downloading: {url}")
        urllib.request.urlretrieve(url, dest)
        size_mb = dest.stat().st_size / (1024 * 1024)
        print(f"  Saved: {dest.name} ({size_mb:.1f} MB)")
        return True
    except urllib.error.HTTPError as e:
        print(f"  Failed: {e.code} {e.reason}")
        return False
    except Exception as e:
        print(f"  Error: {e}")
        return False


def extract_file(file_path: Path, extract_to: Path, subfolder: str = None) -> bool:
    """
    Extract a tar.gz archive, optionally into a named subfolder.

    file_path: Path to the tar.gz archive.
    extract_to: Base directory for extraction.
    subfolder: If provided, extraction target is extract_to/subfolder.
    Returns: True on success, False on error.
    """
    try:
        if subfolder:
            extract_to = extract_to / subfolder
            extract_to.mkdir(parents=True, exist_ok=True)

        with tarfile.open(file_path, 'r:gz') as tar:
            tar.extractall(extract_to)
        print(f"  Extracted to: {extract_to}")
        return True
    except Exception as e:
        print(f"  Extract failed: {e}")
        return False


def month_range(start_year: int, start_month: int, end_year: int, end_month: int):
    """
    Generate (year, month) tuples from a start to an end month, inclusive.

    start_year: Four-digit start year.
    start_month: Start month (1–12).
    end_year: Four-digit end year.
    end_month: End month (1–12).
    Returns: Generator of (year, month) int tuples.
    """
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


def main():
    """
    Command-line entry point.

    Returns: Exit code (0 if all downloads succeeded, 1 if any failed).
    """
    parser = argparse.ArgumentParser(description="Download Global Marine weather data from NOAA NCEI")
    parser.add_argument("--start", required=True, help="Start month (YYYY-MM)")
    parser.add_argument("--end", required=True, help="End month (YYYY-MM)")
    parser.add_argument("--output", "-o", default=".", help="Output directory (default: current)")
    parser.add_argument("--extract", "-x", action="store_true", help="Extract tar.gz files after download")
    parser.add_argument("--keep-archive", action="store_true", help="Keep tar.gz files after extraction")
    parser.add_argument("--skip-existing", action="store_true", help="Skip files that already exist")
    parser.add_argument("--pattern", "-p", default=DEFAULT_PATTERN,
                        choices=URL_PATTERNS.keys(),
                        help=f"URL pattern to use (default: {DEFAULT_PATTERN})")
    args = parser.parse_args()

    # Parse start/end dates
    start_date = datetime.strptime(args.start, "%Y-%m")
    end_date = datetime.strptime(args.end, "%Y-%m")
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    months = list(month_range(start_date.year, start_date.month, end_date.year, end_date.month))
    total = len(months)
    downloaded = 0
    skipped = 0
    failed = 0

    print(f"Downloading weather data from {args.start} to {args.end}")
    print(f"Output directory: {output_dir.absolute()}")
    print(f"URL pattern: {args.pattern} ({URL_PATTERNS[args.pattern]['filename']})")
    print(f"Total months: {total}\n")

    for i, (year, month) in enumerate(months, 1):
        url, filename = get_download_url(year, month, args.pattern)
        dest = output_dir / filename

        print(f"[{i}/{total}] {year}-{month:02d} (pattern: {args.pattern})")

        # Skip if already exists (check both archive and extracted folder)
        subfolder = f"{year}{month:02d}"
        extracted_dir = output_dir / subfolder
        if args.skip_existing:
            if dest.exists():
                print(f"  Skipped: {filename} already exists")
                skipped += 1
                continue
            if args.extract and extracted_dir.exists():
                print(f"  Skipped: {subfolder}/ already extracted")
                skipped += 1
                continue

        if download_file(url, dest):
            downloaded += 1

            if args.extract:
                if extract_file(dest, output_dir, subfolder=subfolder):
                    if not args.keep_archive:
                        dest.unlink()
                        print(f"  Removed: {filename}")
        else:
            failed += 1

        print()

    print(f"Done! Downloaded: {downloaded}, Skipped: {skipped}, Failed: {failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
