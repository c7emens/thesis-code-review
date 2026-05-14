#!/usr/bin/env python3
# Download daily AIS data files from NOAA.
#
# Supports two URL patterns: the legacy zip format and the newer zstd-compressed
# CSV format. Files are downloaded for each day in the requested date range and
# optionally extracted in-place.
#
# Usage:
#   python download_ais_data.py --start 2024-01-01 --end 2024-01-31 --output /mnt/e/data_lake/ais
#   python download_ais_data.py --start 2024-01-01 --end 2024-01-31 --pattern zstd --extract

import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path
import urllib.request
import zipfile


# URL patterns - add new patterns here as needed
# Format variables: {year}, {month}, {day}, {date_underscore} (YYYY_MM_DD), {date_dash} (YYYY-MM-DD)
URL_PATTERNS = {
    "legacy": {
        "base_url": "https://coast.noaa.gov/htdata/CMSP/AISDataHandler",
        "filename": "AIS_{date_underscore}.zip",
    },
    "zstd": {
        "base_url": "https://coast.noaa.gov/htdata/CMSP/AISDataHandler",
        "filename": "ais-{date_dash}.csv.zst",
    },
    # Add new patterns here as needed:
    # "new_pattern": {
    #     "base_url": "https://example.com/data",
    #     "filename": "data_{date_dash}.csv.gz",
    # },
}

## Default URL pattern name.
DEFAULT_PATTERN = "legacy"


def get_download_url(date: datetime, pattern_name: str) -> tuple[str, str]:
    """
    Build the download URL and filename for a given date and pattern.

    date: Date for which to generate the URL.
    pattern_name: Key into URL_PATTERNS (e.g. 'legacy' or 'zstd').
    Returns: Tuple of (full_url, filename).
    @raises ValueError  If pattern_name is not found in URL_PATTERNS.
    """
    if pattern_name not in URL_PATTERNS:
        available = ", ".join(URL_PATTERNS.keys())
        raise ValueError(f"Unknown pattern '{pattern_name}'. Available: {available}")

    pattern = URL_PATTERNS[pattern_name]
    filename = pattern["filename"].format(
        year=date.year,
        month=f"{date.month:02d}",
        day=f"{date.day:02d}",
        date_underscore=date.strftime('%Y_%m_%d'),
        date_dash=date.strftime('%Y-%m-%d'),
    )
    url = f"{pattern['base_url']}/{date.year}/{filename}"
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


def extract_file(file_path: Path, extract_to: Path) -> bool:
    """
    Extract a compressed file (zip, zst, or gz) to a directory.

    file_path: Path of the compressed file.
    extract_to: Destination directory for extracted content.
    Returns: True on success, False on error.
    """
    try:
        suffix = file_path.suffix.lower()

        if suffix == '.zip':
            with zipfile.ZipFile(file_path, 'r') as zf:
                zf.extractall(extract_to)
            print(f"  Extracted to: {extract_to}")

        elif suffix == '.zst' or file_path.name.endswith('.csv.zst'):
            try:
                import zstandard as zstd
            except ImportError:
                print("  Error: zstandard library required. Install with: pip install zstandard")
                return False
            # Output filename: remove .zst extension
            out_name = file_path.name.replace('.zst', '')
            out_path = extract_to / out_name
            with open(file_path, 'rb') as compressed:
                dctx = zstd.ZstdDecompressor()
                with open(out_path, 'wb') as decompressed:
                    dctx.copy_stream(compressed, decompressed)
            print(f"  Decompressed to: {out_path}")

        elif suffix == '.gz':
            import gzip
            out_name = file_path.name.replace('.gz', '')
            out_path = extract_to / out_name
            with gzip.open(file_path, 'rb') as compressed:
                with open(out_path, 'wb') as decompressed:
                    decompressed.write(compressed.read())
            print(f"  Decompressed to: {out_path}")

        else:
            print(f"  Unknown compression format: {suffix}")
            return False

        return True
    except Exception as e:
        print(f"  Extract failed: {e}")
        return False


def date_range(start: datetime, end: datetime):
    """
    Generate dates from start to end, inclusive.

    start: First date to yield.
    end: Last date to yield (inclusive).
    Returns: Generator of datetime objects, one per day.
    """
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def main():
    """
    Command-line entry point.

    Returns: Exit code (0 if all downloads succeeded, 1 if any failed).
    """
    parser = argparse.ArgumentParser(description="Download AIS data from NOAA")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", "-o", default=".", help="Output directory (default: current)")
    parser.add_argument("--extract", "-x", action="store_true", help="Extract zip files after download")
    parser.add_argument("--keep-zip", action="store_true", help="Keep zip files after extraction")
    parser.add_argument("--skip-existing", action="store_true", help="Skip files that already exist")
    parser.add_argument("--pattern", "-p", default=DEFAULT_PATTERN,
                        choices=URL_PATTERNS.keys(),
                        help=f"URL pattern to use (default: {DEFAULT_PATTERN})")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    dates = list(date_range(start_date, end_date))
    total = len(dates)
    downloaded = 0
    skipped = 0
    failed = 0

    print(f"Downloading AIS data from {args.start} to {args.end}")
    print(f"Output directory: {output_dir.absolute()}")
    print(f"URL pattern: {args.pattern} ({URL_PATTERNS[args.pattern]['filename']})")
    print(f"Total days: {total}\n")

    for i, date in enumerate(dates, 1):
        url, filename = get_download_url(date, args.pattern)
        dest = output_dir / filename
        # Handle different compression formats for csv name
        csv_name = filename.replace('.zip', '.csv').replace('.csv.zst', '.csv').replace('.csv.gz', '.csv')
        csv_path = output_dir / csv_name

        print(f"[{i}/{total}] {date.strftime('%Y-%m-%d')} (pattern: {args.pattern})")

        # Skip if already exists
        if args.skip_existing:
            if dest.exists():
                print(f"  Skipped: {filename} already exists")
                skipped += 1
                continue
            if args.extract and csv_path.exists():
                print(f"  Skipped: {csv_name} already exists")
                skipped += 1
                continue

        if download_file(url, dest):
            downloaded += 1

            if args.extract:
                if extract_file(dest, output_dir):
                    if not args.keep_zip:
                        dest.unlink()
                        print(f"  Removed: {filename}")
        else:
            failed += 1

        print()

    print(f"Done! Downloaded: {downloaded}, Skipped: {skipped}, Failed: {failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
