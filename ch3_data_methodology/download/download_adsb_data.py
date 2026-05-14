#!/usr/bin/env python3
# Download historical ADS-B flight data from adsblol/globe_history repositories.
#
# Fetches PREFERRED_RELEASES.txt from each year's GitHub repository, resolves
# the download links, and extracts tar archives into a date-based directory
# structure (YYYY/MM/DD). Already-completed days are detected via a .done
# marker file and skipped automatically.
#
# Usage:
#   python download_adsb_data.py
#   python download_adsb_data.py --start-from 2024-01-01
#   python download_adsb_data.py --end-at 2024-06-30
#   python download_adsb_data.py --workers 2

import argparse
import asyncio
import os
import pathlib
import tempfile
from datetime import datetime

import aiohttp
import requests


# Data sources - globe_history releases
SOURCES = [
    "https://raw.githubusercontent.com/adsblol/globe_history_2025/refs/heads/main/PREFERRED_RELEASES.txt",
    "https://raw.githubusercontent.com/adsblol/globe_history_2024/refs/heads/main/PREFERRED_RELEASES.txt",
    "https://raw.githubusercontent.com/adsblol/globe_history_2023/refs/heads/main/PREFERRED_RELEASES.txt",
]

## Default output directory for downloaded ADS-B data.
DEFAULT_OUTPUT_DIR = pathlib.Path("/mnt/e/data_lake/adsb")


def parse_date_from_link(link: str) -> str:
    """
    Extract a YYYY-MM-DD date string from a globe_history download link.

    link: A release download URL or filename.
    Returns: Date string in YYYY-MM-DD format.
    """
    # Example: https://github.com/.../v2025.01.09-planes-readsb-staging-0/v2025.01.09-...
    filename = link.split("/")[-1]
    date_part = filename.split("-")[0].replace("v", "")  # "2025.01.09"
    return date_part.replace(".", "-")  # "2025-01-09"


def date_to_path(date_str: str) -> pathlib.Path:
    """
    Convert a YYYY-MM-DD date string to a YYYY/MM/DD directory path.

    date_str: Date string in YYYY-MM-DD format.
    Returns: Relative Path object (e.g. Path('2025/01/09')).
    """
    return pathlib.Path(date_str.replace("-", "/"))


async def download_chunked(session: aiohttp.ClientSession, url: str, dest_file) -> None:
    """
    Stream-download a file in 16 KB chunks to avoid memory pressure.

    session: Active aiohttp ClientSession.
    url: Source URL to download.
    dest_file: Writable binary file object to write chunks into.
    """
    async with session.get(url) as response:
        response.raise_for_status()
        while chunk := await response.content.read(16 * 1024):
            dest_file.write(chunk)


async def process_link(link_group: str, output_dir: pathlib.Path, session: aiohttp.ClientSession) -> bool:
    """
    Download and extract one date's archive (possibly split across multiple parts).

    Skips the date if a .done marker file already exists in the target directory.

    link_group: Comma-separated list of part URLs for one date.
    output_dir: Root output directory; date subdirectory is created underneath.
    session: Active aiohttp ClientSession.
    Returns: True if the date was downloaded and extracted, False if skipped.
    """
    links = link_group.split(",")
    date_str = parse_date_from_link(links[0])
    date_path = output_dir / date_to_path(date_str)
    done_marker = date_path / ".done"

    # Skip if already processed
    if done_marker.exists():
        print(f"  {date_str}: Already downloaded, skipping")
        return False

    print(f"  {date_str}: Downloading ({len(links)} part(s))...")

    # Download all parts to temp files
    temp_files = {link: tempfile.NamedTemporaryFile(delete=False) for link in links}

    try:
        # Download all parts concurrently
        tasks = [download_chunked(session, link, tf) for link, tf in temp_files.items()]
        await asyncio.gather(*tasks)

        # Concatenate split archives if multiple parts
        main_tf = temp_files[links[0]]
        for link in links[1:]:
            tf = temp_files[link]
            tf.seek(0)
            main_tf.write(tf.read())
            tf.close()
            os.unlink(tf.name)

        # Extract tar archive
        main_tf.seek(0)
        date_path.mkdir(parents=True, exist_ok=True)

        print(f"  {date_str}: Extracting...")
        proc = await asyncio.create_subprocess_shell(
            f"tar -xf {main_tf.name} -C {date_path}",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()

        if proc.returncode != 0:
            raise RuntimeError(f"tar extraction failed: {stderr.decode()}")

        # Mark as complete
        done_marker.touch()
        print(f"  {date_str}: ✓ Complete")

        return True

    finally:
        # Cleanup temp files
        for tf in temp_files.values():
            try:
                tf.close()
                if os.path.exists(tf.name):
                    os.unlink(tf.name)
            except Exception:
                pass


async def main():
    """
    Async command-line entry point.

    Returns: Exit code (0 on success, 1–2 on error).
    """
    parser = argparse.ArgumentParser(
        description="Download ADS-B flight data from globe_history",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Download all available data
  %(prog)s --start-from 2024-01-01            # Start from specific date
  %(prog)s --start-from 2024-01 --end-at 2024-06  # Download date range
  %(prog)s --output-dir /path/to/data         # Custom output directory
  %(prog)s --workers 2                        # Limit concurrent downloads
        """
    )
    parser.add_argument("--output-dir", "-o", type=pathlib.Path, default=DEFAULT_OUTPUT_DIR,
                        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--start-from", type=str, metavar="DATE",
                        help="Start from date containing this string (e.g. '2024-01-01' or '2024-01')")
    parser.add_argument("--end-at", type=str, metavar="DATE",
                        help="End at date containing this string, inclusive")
    parser.add_argument("--workers", "-w", type=int, default=4,
                        help="Max concurrent downloads (default: 4)")
    parser.add_argument("--retry-delay", type=int, default=5,
                        help="Seconds to wait before retrying failed downloads (default: 5)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be downloaded without downloading")
    args = parser.parse_args()

    # Fetch release lists
    print("Fetching release lists...")
    all_links = []
    for src in SOURCES:
        try:
            print(f"  {src.split('/')[-3]}...")
            r = requests.get(src, timeout=30)
            r.raise_for_status()
            links = [line.strip() for line in r.text.split("\n") if line.strip()]
            all_links.extend(links)
        except Exception as e:
            print(f"  ⚠️ Failed to fetch {src}: {e}")

    if not all_links:
        print("❌ No download links found")
        return 1

    # Sort by date (oldest first for chronological processing)
    all_links.sort(key=lambda x: parse_date_from_link(x.split(",")[0]))

    # Apply date filters
    if args.start_from:
        start_idx = None
        for i, link in enumerate(all_links):
            date_str = parse_date_from_link(link.split(",")[0])
            if args.start_from in date_str:
                start_idx = i
                break
        if start_idx is None:
            print(f"❌ No date found matching '{args.start_from}'")
            return 2
        all_links = all_links[start_idx:]
        print(f"Starting from: {parse_date_from_link(all_links[0].split(',')[0])}")

    if args.end_at:
        end_idx = None
        for i in range(len(all_links) - 1, -1, -1):
            date_str = parse_date_from_link(all_links[i].split(",")[0])
            if args.end_at in date_str:
                end_idx = i
                break
        if end_idx is None:
            print(f"❌ No date found matching '{args.end_at}'")
            return 2
        all_links = all_links[:end_idx + 1]
        print(f"Ending at: {parse_date_from_link(all_links[-1].split(',')[0])}")

    print(f"\nFound {len(all_links)} date(s) to process")
    print(f"Output directory: {args.output_dir}")
    print(f"Concurrent downloads: {args.workers}\n")

    if args.dry_run:
        print("Dry run - would download:")
        for link in all_links:
            date_str = parse_date_from_link(link.split(",")[0])
            date_path = args.output_dir / date_to_path(date_str)
            status = "✓ exists" if (date_path / ".done").exists() else "pending"
            print(f"  {date_str}: {status}")
        return 0

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Process with semaphore for rate limiting
    semaphore = asyncio.Semaphore(args.workers)
    downloaded = 0
    skipped = 0
    failed = 0

    async def process_with_retry(link_group: str, session: aiohttp.ClientSession):
        nonlocal downloaded, skipped, failed
        async with semaphore:
            retries = 3
            for attempt in range(retries):
                try:
                    result = await process_link(link_group, args.output_dir, session)
                    if result:
                        downloaded += 1
                    else:
                        skipped += 1
                    return
                except Exception as e:
                    if attempt < retries - 1:
                        date_str = parse_date_from_link(link_group.split(",")[0])
                        print(f"  {date_str}: Retry {attempt + 1}/{retries} after error: {e}")
                        await asyncio.sleep(args.retry_delay)
                    else:
                        date_str = parse_date_from_link(link_group.split(",")[0])
                        print(f"  {date_str}: ❌ Failed after {retries} attempts: {e}")
                        failed += 1

    # Create session with longer timeout for large files
    timeout = aiohttp.ClientTimeout(total=3600, connect=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [process_with_retry(link, session) for link in all_links]
        await asyncio.gather(*tasks)

    print(f"\n{'═' * 50}")
    print(f"✅ Downloaded: {downloaded}")
    print(f"⏭️  Skipped:    {skipped}")
    if failed:
        print(f"❌ Failed:     {failed}")
    print(f"{'═' * 50}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    exit(asyncio.run(main()))
