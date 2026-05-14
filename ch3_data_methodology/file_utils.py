# Shared utilities for opening compressed CSV files in ingestion scripts.
#
# Provides a single generator interface over multiple compression formats so
# ingestion scripts can read CSVs without caring about the container format.
#
# Supported formats:
# - Single-file compression: .zst, .gz
# - Archives: .tar.gz, .tar, .zip
# - Plain: .csv
#
# Usage:
#   from file_utils import open_csv_files
#
#   for name, file_obj in open_csv_files(Path("data.tar.gz")):
#       df = pd.read_csv(file_obj)
#       # or with chunks:
#       for chunk in pd.read_csv(file_obj, chunksize=100_000):
#           process(chunk)

from __future__ import annotations

import gzip
import io
import tarfile
import zipfile
from pathlib import Path
from typing import BinaryIO, Generator


def _open_zstd(filepath: Path) -> BinaryIO:
    """
    Open a .zst file and return a streaming file-like object.

    filepath: Path to the .zst file.
    Returns: Readable binary stream over the decompressed data.
    @raises ImportError  If the zstandard package is not installed.
    """
    try:
        import zstandard as zstd
    except ImportError:
        raise ImportError("zstandard library required. Install with: pip install zstandard")

    fh = open(filepath, 'rb')
    dctx = zstd.ZstdDecompressor()
    return dctx.stream_reader(fh)


def _open_gzip(filepath: Path) -> BinaryIO:
    """
    Open a .gz file and return a file-like object.

    filepath: Path to the .gz file.
    Returns: Readable binary stream over the decompressed data.
    """
    return gzip.open(filepath, 'rb')


def open_csv_files(filepath: Path) -> Generator[tuple[str, BinaryIO], None, None]:
    """
    Open a (possibly compressed or archived) file and yield CSV streams.

    For single compressed files (.csv, .csv.zst, .csv.gz) yields one tuple.
    For archives (.tar.gz, .zip) yields one tuple per CSV member.

    The yielded file objects can be passed directly to pd.read_csv().

    filepath: Path to the file to open.
    Returns: Generator of (csv_name, file_object) tuples.
    @raises ValueError  If the file extension is not supported.
    """
    suffix = filepath.suffix.lower()
    name = filepath.name

    # Handle .tar.gz (check for double extension first)
    if name.endswith('.tar.gz') or name.endswith('.tgz'):
        with tarfile.open(filepath, 'r:gz') as tar:
            for member in tar.getmembers():
                if member.isfile() and member.name.lower().endswith('.csv'):
                    f = tar.extractfile(member)
                    if f:
                        yield member.name, f

    # Handle .tar (uncompressed)
    elif suffix == '.tar':
        with tarfile.open(filepath, 'r:') as tar:
            for member in tar.getmembers():
                if member.isfile() and member.name.lower().endswith('.csv'):
                    f = tar.extractfile(member)
                    if f:
                        yield member.name, f

    # Handle .zip
    elif suffix == '.zip':
        with zipfile.ZipFile(filepath, 'r') as zf:
            for member in zf.namelist():
                if member.lower().endswith('.csv'):
                    yield member, zf.open(member)

    # Handle .zst (single file compression)
    elif suffix == '.zst' or name.endswith('.csv.zst'):
        csv_name = name.replace('.zst', '')
        yield csv_name, _open_zstd(filepath)

    # Handle .gz (single file compression)
    elif suffix == '.gz' or name.endswith('.csv.gz'):
        csv_name = name.replace('.gz', '')
        yield csv_name, _open_gzip(filepath)

    # Handle plain .csv
    elif suffix == '.csv':
        yield name, open(filepath, 'rb')

    else:
        raise ValueError(f"Unsupported file format: {filepath}")


def count_rows_in_file(filepath: Path) -> int:
    """
    Count data rows in a CSV file, including compressed variants.

    For archives with multiple CSVs, returns the total across all members.
    The header row is excluded from the count.

    filepath: Path to the file (supports all formats handled by open_csv_files).
    Returns: Total number of data rows (header excluded).
    """
    total = 0
    for name, f in open_csv_files(filepath):
        # Read in binary mode and count newlines
        count = sum(1 for _ in f) - 1  # subtract header
        total += max(0, count)

        # Reset if possible (for reuse), otherwise it's consumed
        if hasattr(f, 'seek'):
            try:
                f.seek(0)
            except (io.UnsupportedOperation, OSError):
                pass

    return total


def get_supported_extensions() -> list[str]:
    """
    Return the list of file extensions supported by open_csv_files.

    Returns: List of extension strings (e.g. ['.csv', '.csv.gz', ...]).
    """
    return ['.csv', '.csv.gz', '.csv.zst', '.gz', '.zst', '.zip', '.tar', '.tar.gz', '.tgz']
