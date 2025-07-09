import gzip
import shutil
import subprocess
import tarfile
import time
import zipfile
from pathlib import Path

import requests


def wait_for_qdrant_ready(port: int = 6333, timeout: int = 30) -> bool:
    """Wait for Qdrant service to be ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"http://localhost:{port}/readyz")
            if response.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
    return False


def extract_archive(archive_file: Path, extract_to: Path, cleanup_archive: bool = False) -> Path:
    """General utility function to extract various archive formats.

    Args:
        archive_file: Path to the archive file to extract
        extract_to: Directory where to extract the archive contents
        cleanup_archive: Whether to delete the archive file after extraction

    Returns:
        Path: The extraction directory path

    Raises:
        FileNotFoundError: If archive file doesn't exist
        ValueError: If archive format is unsupported
        RuntimeError: If extraction fails

    Supported formats: .tar.xz, .tar.gz, .tar.bz2, .tgz, .tbz2, .tar, .zip, .gz
    """
    if not archive_file.exists():
        raise FileNotFoundError(f"Archive not found: {archive_file}")

    extract_to.mkdir(parents=True, exist_ok=True)
    file_name = archive_file.name.lower()

    try:
        if file_name.endswith('.gz') and not file_name.endswith(('.tar.gz', '.tgz')):
            # Handle standalone gzip files (like snapshots)
            output_file = extract_to / archive_file.stem
            with gzip.open(archive_file, 'rb') as gz_file:
                with open(output_file, 'wb') as out_file:
                    shutil.copyfileobj(gz_file, out_file)
            print(f"Extracted {archive_file} to {output_file}")

        elif file_name.endswith(('.tar.xz', '.tar.gz', '.tar.bz2', '.tgz', '.tbz2')):
            # Handle compressed tar files
            with tarfile.open(archive_file, 'r:*') as tar:
                tar.extractall(path=extract_to)
                print(f"Extracted {archive_file} to {extract_to}")

        elif file_name.endswith('.tar'):
            # Handle uncompressed tar files
            with tarfile.open(archive_file, 'r:') as tar:
                tar.extractall(path=extract_to)
                print(f"Extracted {archive_file} to {extract_to}")

        elif file_name.endswith('.zip'):
            # Handle zip files
            with zipfile.ZipFile(archive_file, 'r') as zip_file:
                zip_file.extractall(path=extract_to)
                print(f"Extracted {archive_file} to {extract_to}")

        else:
            raise ValueError(f"Unsupported archive format: {archive_file}")

    except Exception as e:
        print(f"Failed to extract archive {archive_file}: {e}")
        # Try fallback to subprocess for tar files
        if file_name.endswith(('.tar.xz', '.tar.gz', '.tar.bz2', '.tgz', '.tbz2', '.tar')):
            try:
                print(f"Trying fallback extraction with tar command...")
                subprocess.run(["tar", "-xf", str(archive_file)], cwd=str(extract_to), check=True)
                print(f"Successfully extracted {archive_file} using tar command")
            except subprocess.CalledProcessError as tar_error:
                raise RuntimeError(f"Failed to extract archive: {tar_error}")
        else:
            raise

    if cleanup_archive:
        archive_file.unlink(missing_ok=True)
        print(f"Cleaned up archive: {archive_file}")

    return extract_to