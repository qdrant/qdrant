"""
Tests that io_uring handles EINTR (os error 4) transparently.

When a signal arrives during `io_uring_enter(IORING_ENTER_GETEVENTS)`,
the syscall returns EINTR. The code must retry instead of propagating
the error to `.unwrap()` at async_raw_scorer.rs:77.

This test starts a single qdrant node with `async_scorer=true`,
inserts enough data for io_uring to kick in, then sends SIGUSR1
signals while issuing searches. The process must stay alive and
all searches must succeed.
"""

import os
import pathlib
import random
import signal
import threading
import time

import requests

from .utils import (
    assert_project_root,
    make_peer_folders,
    start_first_peer,
    wait_peer_added,
    wait_collection_green,
    processes,
)
from .assertions import assert_http_ok

COLLECTION_NAME = "test_io_uring_eintr"
NUM_POINTS = 5000
VECTOR_DIM = 128
SIGNAL_DURATION_SEC = 5
SEARCH_THREADS = 4


def _search(uri: str, stop_event: threading.Event, results: list):
    """Continuously send search requests until stopped."""
    query = [float(i) / VECTOR_DIM for i in range(VECTOR_DIM)]
    while not stop_event.is_set():
        try:
            r = requests.post(
                f"{uri}/collections/{COLLECTION_NAME}/points/search",
                json={
                    "vector": query,
                    "top": 10,
                    "with_vector": False,
                },
                timeout=5,
            )
            results.append(("ok", r.status_code))
        except requests.exceptions.ConnectionError:
            results.append(("connection_error", None))
            break
        except requests.exceptions.ReadTimeout:
            results.append(("timeout", None))
        except Exception as e:
            results.append(("error", str(e)))


def _signal_bombardment(pid: int, stop_event: threading.Event):
    """Send SIGUSR1 to the qdrant process as fast as possible."""
    while not stop_event.is_set():
        try:
            os.kill(pid, signal.SIGUSR1)
        except ProcessLookupError:
            break
        time.sleep(0.0001)  # 100us between signals


def test_io_uring_eintr_panics(tmp_path: pathlib.Path):
    """
    io_uring must handle EINTR transparently by retrying submit_and_wait.
    Under SIGUSR1 bombardment, the process must stay alive and all searches
    must succeed.
    """
    assert_project_root()
    peer_dirs = make_peer_folders(tmp_path, 1)

    extra_env = {
        "QDRANT__STORAGE__PERFORMANCE__ASYNC_SCORER": "true",
    }

    (api_uri, _) = start_first_peer(
        peer_dirs[0], "peer_0_0.log", extra_env=extra_env
    )
    wait_peer_added(api_uri)

    # Create collection with mmap threshold low enough to force mmap storage
    # (required for io_uring). Use a larger vector dim than the default fixture
    # to generate more io_uring work per search.
    r = requests.put(
        f"{api_uri}/collections/{COLLECTION_NAME}",
        json={
            "vectors": {
                "size": VECTOR_DIM,
                "distance": "Cosine",
            },
            "shard_number": 1,
            "replication_factor": 1,
            "optimizers_config": {
                "memmap_threshold": 100,
            },
        },
        timeout=10,
    )
    assert_http_ok(r)

    # Insert points in batches
    batch_size = 100
    for offset in range(0, NUM_POINTS, batch_size):
        count = min(batch_size, NUM_POINTS - offset)
        points = [
            {
                "id": offset + i,
                "vector": [random.random() for _ in range(VECTOR_DIM)],
            }
            for i in range(count)
        ]
        r = requests.put(
            f"{api_uri}/collections/{COLLECTION_NAME}/points?wait=true",
            json={"points": points},
            timeout=30,
        )
        assert_http_ok(r)

    # Wait for optimizer to create mmap segments (io_uring needs these)
    wait_collection_green(api_uri, COLLECTION_NAME)

    # Verify search works before signal bombardment
    query = [float(i) / VECTOR_DIM for i in range(VECTOR_DIM)]
    r = requests.post(
        f"{api_uri}/collections/{COLLECTION_NAME}/points/search",
        json={"vector": query, "top": 10},
        timeout=5,
    )
    assert_http_ok(r)
    assert len(r.json()["result"]) > 0, "Baseline search returned no results"

    # Get the PID of the qdrant process
    peer_proc = processes[-1]
    pid = peer_proc.pid

    # Start signal bombardment and concurrent searches
    stop = threading.Event()
    search_results = []

    signal_thread = threading.Thread(
        target=_signal_bombardment, args=(pid, stop)
    )
    search_threads = [
        threading.Thread(target=_search, args=(api_uri, stop, search_results))
        for _ in range(SEARCH_THREADS)
    ]

    signal_thread.start()
    for t in search_threads:
        t.start()

    time.sleep(SIGNAL_DURATION_SEC)

    stop.set()
    for t in search_threads:
        t.join(timeout=10)
    signal_thread.join(timeout=5)

    # Analyze results
    connection_errors = sum(1 for kind, _ in search_results if kind == "connection_error")
    ok_count = sum(1 for kind, _ in search_results if kind == "ok")
    error_500 = sum(1 for kind, code in search_results if kind == "ok" and code == 500)

    print(f"\nResults: {ok_count} ok, {error_500} 500s, {connection_errors} connection errors, "
          f"{len(search_results)} total")

    # Check if the process is still alive
    poll_result = peer_proc.proc.poll()
    process_crashed = poll_result is not None

    if process_crashed:
        print(f"QDRANT CRASHED with exit code: {poll_result}")

    # io_uring must handle EINTR transparently. The process must stay alive
    # and all searches must succeed even under signal bombardment.
    assert not process_crashed, (
        f"Qdrant crashed (exit code {poll_result}) under SIGUSR1 bombardment with "
        f"async_scorer=true. io_uring submit_and_wait must retry on EINTR (os error 4) "
        f"instead of propagating the error to .unwrap() at async_raw_scorer.rs:77."
    )

    assert connection_errors == 0, (
        f"Got {connection_errors} connection errors — process likely crashed mid-request. "
        f"io_uring submit_and_wait must retry on EINTR."
    )

    assert error_500 == 0, (
        f"Got {error_500} HTTP 500 errors — io_uring EINTR caused panics. "
        f"submit_and_wait must retry on EINTR instead of returning an error."
    )
