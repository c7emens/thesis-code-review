#!/usr/bin/env python3
# Compress CSV files into formats supported by the ingestion scripts.
#
# Supports gz (gzip), zst (zstandard), zip, and tar.gz output formats.
# Already-compressed files are silently skipped. --start-from and --end-at
# allow range-based operation over large sorted file lists.
#
# Usage:
#   python compress_files.py data.csv --format zst
#   python compress_files.py './data/*.csv' --format gz
#   python compress_files.py './data/*.csv' --format tar.gz --archive-name data_backup.tar.gz
#   python compress_files.py data.csv -f zst --delete-original

import argparse
import gzip
import glob as glob_module
import os
import shutil
import tarfile
import zipfile
from pathlib import Path


## Compression format names accepted by the --format argument.
SUPPORTED_FORMATS = ['gz', 'zst', 'zip', 'tar.gz']
## File extensions considered already compressed (skipped as input).
COMPRESSED_EXTENSIONS = {'.gz', '.zst', '.zip', '.tgz', '.bz2', '.xz', '.7z', '.rar'}


def is_already_compressed(filepath: Path) -> bool:
    """
    Check whether a file is already in a compressed format.

    filepath: Path to the file to inspect.
    Returns: True if the file extension indicates compression.
    """
    name = filepath.name.lower()
    # Check for .tar.gz specifically
    if name.endswith('.tar.gz'):
        return True
    # Check other compressed extensions
    return filepath.suffix.lower() in COMPRESSED_EXTENSIONS


def compress_gzip(src: Path, dest: Path) -> int:
    """
    Compress a file using gzip at maximum compression.

    src: Source file to compress.
    dest: Destination .gz file path.
    Returns: Compressed file size in bytes.
    """
    with open(src, 'rb') as f_in:
        with gzip.open(dest, 'wb', compresslevel=9) as f_out:
            shutil.copyfileobj(f_in, f_out)
    return dest.stat().st_size


def compress_zstd(src: Path, dest: Path, level: int = 3) -> int:
    """
    Compress a file using zstandard.

    src: Source file to compress.
    dest: Destination .zst file path.
    level: Zstandard compression level (1–22, default 3).
    Returns: Compressed file size in bytes.
    @raises ImportError  If the zstandard package is not installed.
    """
    try:
        import zstandard as zstd
    except ImportError:
        raise ImportError("zstandard library required. Install with: pip install zstandard")

    cctx = zstd.ZstdCompressor(level=level)
    with open(src, 'rb') as f_in:
        with open(dest, 'wb') as f_out:
            cctx.copy_stream(f_in, f_out)
    return dest.stat().st_size


def compress_zip(src: Path, dest: Path) -> int:
    """
    Compress a file into a zip archive using DEFLATE.

    src: Source file to compress.
    dest: Destination .zip file path.
    Returns: Compressed file size in bytes.
    """
    with zipfile.ZipFile(dest, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        zf.write(src, src.name)
    return dest.stat().st_size


def create_tar_gz(files: list[Path], dest: Path) -> int:
    """
    Create a tar.gz archive containing multiple files.

    files: List of source file paths to include.
    dest: Destination .tar.gz file path.
    Returns: Compressed archive size in bytes.
    """
    with tarfile.open(dest, 'w:gz', compresslevel=9) as tar:
        for f in files:
            tar.add(f, arcname=f.name)
            print(f"  Added: {f.name}")
    return dest.stat().st_size


def format_size(size_bytes: int) -> str:
    """
    Format a byte count as a human-readable string.

    size_bytes: Number of bytes.
    Returns: Formatted string (e.g. '12.3 MB').
    """
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def get_output_path(src: Path, fmt: str, output_dir: Path = None) -> Path:
    """
    Generate the output file path for a given source and format.

    src: Source file path.
    fmt: Output format ('gz', 'zst', 'zip', or 'tar.gz').
    output_dir: Optional output directory override; defaults to src's directory.
    Returns: Destination path for the compressed file.
    @raises ValueError  If fmt is not recognised.
    """
    if output_dir:
        base = output_dir / src.name
    else:
        base = src

    if fmt == 'gz':
        return base.with_suffix(base.suffix + '.gz')
    elif fmt == 'zst':
        return base.with_suffix(base.suffix + '.zst')
    elif fmt == 'zip':
        return base.with_suffix('.zip')
    elif fmt == 'tar.gz':
        return base.with_suffix('.tar.gz')
    else:
        raise ValueError(f"Unsupported format: {fmt}")


def main():
    """
    Command-line entry point.

    Returns: Exit code (0 on success, 2 if no files matched).
    """
    parser = argparse.ArgumentParser(
        description="Compress files into formats supported by ingestion scripts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Supported formats: {', '.join(SUPPORTED_FORMATS)}

Examples:
  %(prog)s data.csv --format zst                    # Single file to zstd
  %(prog)s './data/*.csv' --format gz               # All CSVs to gzip
  %(prog)s './data/*.csv' --format tar.gz -a backup # Multiple files to tar.gz
  %(prog)s data.csv -f zst --delete-original        # Compress and remove original
  %(prog)s data.csv -f zst --level 19               # Maximum zstd compression
  %(prog)s './AIS_*.csv' -f zst --start-from AIS_2021_03_07  # Start from specific file
  %(prog)s './AIS_*.csv' -f zst --start-from 2021_03 --end-at 2021_06  # Date range
        """
    )
    parser.add_argument("pattern", help="File path or glob pattern (e.g., '*.csv')")
    parser.add_argument("--format", "-f", required=True, choices=SUPPORTED_FORMATS,
                        help="Output compression format")
    parser.add_argument("--output-dir", "-o", type=Path, default=None,
                        help="Output directory (default: same as source)")
    parser.add_argument("--archive-name", "-a", type=str, default=None,
                        help="Archive filename for tar.gz (without extension)")
    parser.add_argument("--delete-original", "-d", action="store_true",
                        help="Delete original files after successful compression")
    parser.add_argument("--level", "-l", type=int, default=3,
                        help="Compression level for zstd (1-22, default: 3)")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip if output file already exists")
    parser.add_argument("--start-from", type=str, metavar="FILENAME",
                        help="Start from file containing this string (e.g. 'AIS_2021_03_07')")
    parser.add_argument("--end-at", type=str, metavar="FILENAME",
                        help="End at file containing this string, inclusive (e.g. 'AIS_2021_12_31')")
    args = parser.parse_args()

    # Find input files
    p = Path(args.pattern)
    if "*" in args.pattern:
        matches = glob_module.glob(args.pattern, recursive=True)
        files = sorted([Path(f) for f in matches if Path(f).is_file()])
    elif p.exists() and p.is_dir():
        # If a directory is given, find all CSV files inside
        files = sorted([f for f in p.iterdir() if f.is_file() and f.suffix.lower() == '.csv'])
    elif p.exists() and p.is_file():
        files = [p]
    else:
        # Try as a glob pattern from the pattern's parent directory
        if p.is_absolute():
            files = sorted([f for f in p.parent.glob(p.name) if f.is_file()])
        else:
            files = sorted([f for f in Path('.').glob(args.pattern) if f.is_file()])

    if not files:
        print(f"No files found for pattern: {args.pattern}")
        return 2

    # Filter out already-compressed files
    original_count = len(files)
    files = [f for f in files if not is_already_compressed(f)]
    skipped_compressed = original_count - len(files)
    if skipped_compressed > 0:
        print(f"Skipped {skipped_compressed} already-compressed file(s)")

    if not files:
        print("No uncompressed files to process.")
        return 0

    # Create output directory if specified
    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)

    total_files = len(files)

    # Handle --start-from option
    if args.start_from:
        start_idx = None
        for i, f in enumerate(files):
            if args.start_from in f.name:
                start_idx = i
                break
        if start_idx is None:
            print(f"❌ No file found matching '{args.start_from}'")
            print(f"   Available files start with: {files[0].name}")
            return 2
        print(f"Starting from file {start_idx + 1}/{total_files}: {files[start_idx].name}")
        files = files[start_idx:]

    # Handle --end-at option (find LAST matching file)
    if args.end_at:
        end_idx = None
        for i in range(len(files) - 1, -1, -1):  # search backwards
            if args.end_at in str(files[i]):
                end_idx = i
                break
        if end_idx is None:
            print(f"❌ No file found matching '{args.end_at}'")
            print(f"   Available files end with: {files[-1]}")
            return 2
        print(f"Ending at file: {files[end_idx]}")
        files = files[:end_idx + 1]  # inclusive

    print(f"Found {len(files)} file(s) to compress")
    print(f"Format: {args.format}\n")

    # Handle tar.gz specially (combines multiple files)
    if args.format == 'tar.gz':
        if args.archive_name:
            archive_name = args.archive_name
            if not archive_name.endswith('.tar.gz'):
                archive_name += '.tar.gz'
        else:
            archive_name = "archive.tar.gz"

        dest = (args.output_dir or files[0].parent) / archive_name

        if args.skip_existing and dest.exists():
            print(f"Skipped: {dest} already exists")
            return 0

        total_size = sum(f.stat().st_size for f in files)
        print(f"Creating archive: {dest}")
        compressed_size = create_tar_gz(files, dest)

        ratio = (1 - compressed_size / total_size) * 100 if total_size > 0 else 0
        print(f"\n✅ Done! {format_size(total_size)} → {format_size(compressed_size)} ({ratio:.1f}% reduction)")

        if args.delete_original:
            for f in files:
                f.unlink()
                print(f"  Deleted: {f.name}")

        return 0

    # Handle single-file compression formats (gz, zst, zip)
    total_original = 0
    total_compressed = 0
    compressed_count = 0
    skipped_count = 0

    for i, src in enumerate(files, 1):
        dest = get_output_path(src, args.format, args.output_dir)
        original_size = src.stat().st_size

        print(f"[{i}/{len(files)}] {src.name}", end="")

        if args.skip_existing and dest.exists():
            print(" → skipped (exists)")
            skipped_count += 1
            continue

        try:
            if args.format == 'gz':
                compressed_size = compress_gzip(src, dest)
            elif args.format == 'zst':
                compressed_size = compress_zstd(src, dest, level=args.level)
            elif args.format == 'zip':
                compressed_size = compress_zip(src, dest)

            ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
            print(f" → {dest.name} ({ratio:.1f}% smaller)")

            total_original += original_size
            total_compressed += compressed_size
            compressed_count += 1

            if args.delete_original:
                src.unlink()

        except Exception as e:
            print(f" → ERROR: {e}")

    if compressed_count > 0:
        total_ratio = (1 - total_compressed / total_original) * 100 if total_original > 0 else 0
        print(f"\n✅ Done! Compressed {compressed_count} file(s)")
        print(f"   Total: {format_size(total_original)} → {format_size(total_compressed)} ({total_ratio:.1f}% reduction)")
        if skipped_count > 0:
            print(f"   Skipped: {skipped_count} file(s)")
    else:
        print("\nNo files were compressed.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
