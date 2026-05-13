"""
Regression tests for GHSA-3v92-w72v-j994.

The snapshot upload endpoints parse the multipart body (writing the snapshot to
a temp file on disk) before the manage-access check runs. A caller holding only
a read-only API key can therefore force the server to allocate disk I/O for an
arbitrarily large upload that ultimately gets rejected with 403 Forbidden.

These tests stream a multipart upload using the read-only API key while a
background thread polls the snapshot upload temp directory. After the request
finishes, they assert:

  1. the response was 403 Forbidden (the auth boundary holds at the end), and
  2. no bytes were observed in the upload temp directory during the request
     (the auth boundary holds *before* any disk I/O).

On a vulnerable build, condition (2) fails: the watcher observes the multipart
temp file growing while the upload is in flight. On a fixed build the request
is rejected before the body is consumed, so the watcher sees nothing.

See: https://github.com/qdrant/qdrant/security/advisories/GHSA-3v92-w72v-j994
"""

import secrets
import threading
import time
from pathlib import Path
from typing import Iterator, Tuple

import pytest
import requests
from consensus_tests import fixtures

from .utils import API_KEY_HEADERS, READ_ONLY_API_KEY, REST_URI, random_str

# Streamed body is ~3 MB delivered in ~50 ms chunks (~2.4 s upload). Large
# enough that a 10 ms-interval poller will reliably see the temp file on a
# vulnerable build, small enough not to noticeably slow the test suite.
_PAYLOAD_SIZE_BYTES = 3 * 1024 * 1024
_CHUNK_SIZE_BYTES = 64 * 1024
_CHUNK_DELAY_SECS = 0.05
_WATCH_POLL_INTERVAL_SECS = 0.01
_REQUEST_TIMEOUT_SECS = 30


class _UploadDirWatcher:
    """Polls a directory in a background thread, recording the largest total
    byte count observed across all files at any single poll."""

    def __init__(self, upload_dir: Path, poll_interval: float = _WATCH_POLL_INTERVAL_SECS):
        self._upload_dir = upload_dir
        self._poll_interval = poll_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._max_total_bytes = 0
        self._files_seen: set[str] = set()

    def __enter__(self) -> "_UploadDirWatcher":
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_exc) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stop.is_set():
            self._poll_once()
            time.sleep(self._poll_interval)
        # Final sweep in case the request finished mid-sleep.
        self._poll_once()

    def _poll_once(self) -> None:
        if not self._upload_dir.exists():
            return
        try:
            entries = list(self._upload_dir.iterdir())
        except (FileNotFoundError, OSError):
            return

        total = 0
        for entry in entries:
            try:
                size = entry.stat().st_size
            except (FileNotFoundError, OSError):
                # File was deleted between iterdir() and stat() — normal race
                # with actix-multipart's TempFile cleanup.
                continue
            total += size
            with self._lock:
                self._files_seen.add(entry.name)

        with self._lock:
            if total > self._max_total_bytes:
                self._max_total_bytes = total

    @property
    def max_total_bytes(self) -> int:
        with self._lock:
            return self._max_total_bytes

    @property
    def files_seen(self) -> set[str]:
        with self._lock:
            return set(self._files_seen)


def _slow_multipart_body(
    field_name: str,
    filename: str,
    payload: bytes,
    boundary: str,
) -> Iterator[bytes]:
    """Yield a multipart/form-data body in slow chunks, so that a vulnerable
    server has time to start writing the temp file while we are still uploading."""
    header = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="{field_name}"; '
        f'filename="{filename}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode()
    footer = f"\r\n--{boundary}--\r\n".encode()

    yield header
    for i in range(0, len(payload), _CHUNK_SIZE_BYTES):
        yield payload[i : i + _CHUNK_SIZE_BYTES]
        time.sleep(_CHUNK_DELAY_SECS)
    yield footer


def _send_slow_upload_with_readonly_key(url: str) -> requests.Response:
    boundary = f"----qdrantghsapoc{secrets.token_hex(8)}"
    payload = b"\x00" * _PAYLOAD_SIZE_BYTES
    body_iter = _slow_multipart_body(
        field_name="snapshot",
        filename="ghsa-3v92-w72v-j994.snapshot",
        payload=payload,
        boundary=boundary,
    )
    return requests.post(
        url,
        data=body_iter,
        headers={
            "api-key": READ_ONLY_API_KEY,
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
        timeout=_REQUEST_TIMEOUT_SECS,
    )


def _watch_and_upload(upload_dir: Path, url: str) -> Tuple[requests.Response, int, set]:
    with _UploadDirWatcher(upload_dir) as watcher:
        response = _send_slow_upload_with_readonly_key(url)
    return response, watcher.max_total_bytes, watcher.files_seen


def _assert_no_unauthorized_disk_writes(
    response: requests.Response,
    bytes_observed: int,
    files_seen: set,
    upload_dir: Path,
    endpoint_label: str,
) -> None:
    assert response.status_code == 403, (
        f"{endpoint_label}: expected 403 Forbidden for read-only credentials "
        f"but got HTTP {response.status_code}: {response.text!r}"
    )
    assert bytes_observed == 0, (
        f"GHSA-3v92-w72v-j994 ({endpoint_label}): read-only credentials caused "
        f"at least {bytes_observed} bytes to be persisted into {upload_dir} "
        f"(files seen: {sorted(files_seen)}) before the 403 was returned. "
        f"The manage-access check must run before the multipart body is accepted."
    )


@pytest.fixture(scope="module")
def upload_auth_collection(jwt_cluster):
    """A dedicated collection (with a shard) for the upload-auth regression
    tests, so we don't perturb the shared jwt_test_collection."""
    collection_name = f"upload_auth_{random_str()}"
    fixtures.create_collection(
        REST_URI,
        collection=collection_name,
        headers=API_KEY_HEADERS,
    )
    try:
        yield collection_name
    finally:
        fixtures.drop_collection(REST_URI, collection_name, headers=API_KEY_HEADERS)


@pytest.fixture(scope="module")
def snapshot_upload_dir(jwt_cluster) -> Path:
    """The directory actix-multipart uses for snapshot upload temp files.

    Matches `TableOfContent::upload_dir()` from
    `lib/storage/src/content_manager/toc/temp_directories.rs`."""
    _peer_api_uris, peer_dirs, _bootstrap_uri = jwt_cluster
    return Path(peer_dirs[0]) / "snapshots" / "tmp" / "upload"


def test_upload_collection_snapshot_does_not_persist_for_readonly_key(
    snapshot_upload_dir: Path, upload_auth_collection: str
):
    url = f"{REST_URI}/collections/{upload_auth_collection}/snapshots/upload"
    response, bytes_observed, files_seen = _watch_and_upload(snapshot_upload_dir, url)
    _assert_no_unauthorized_disk_writes(
        response,
        bytes_observed,
        files_seen,
        snapshot_upload_dir,
        endpoint_label="POST /collections/{collection}/snapshots/upload",
    )


def test_upload_shard_snapshot_does_not_persist_for_readonly_key(
    snapshot_upload_dir: Path, upload_auth_collection: str
):
    url = f"{REST_URI}/collections/{upload_auth_collection}/shards/0/snapshots/upload"
    response, bytes_observed, files_seen = _watch_and_upload(snapshot_upload_dir, url)
    _assert_no_unauthorized_disk_writes(
        response,
        bytes_observed,
        files_seen,
        snapshot_upload_dir,
        endpoint_label="POST /collections/{collection}/shards/{shard}/snapshots/upload",
    )


def test_recover_partial_snapshot_does_not_persist_for_readonly_key(
    snapshot_upload_dir: Path, upload_auth_collection: str
):
    url = (
        f"{REST_URI}/collections/{upload_auth_collection}"
        f"/shards/0/snapshot/partial/recover"
    )
    response, bytes_observed, files_seen = _watch_and_upload(snapshot_upload_dir, url)
    _assert_no_unauthorized_disk_writes(
        response,
        bytes_observed,
        files_seen,
        snapshot_upload_dir,
        endpoint_label="POST /collections/{collection}/shards/{shard}/snapshot/partial/recover",
    )
