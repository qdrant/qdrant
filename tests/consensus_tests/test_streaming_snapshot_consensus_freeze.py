"""
Test: streaming snapshot READ guards + consensus WRITE = frozen collection.

Deadlock chain:
  1. Streaming snapshots hold OwnedRwLockReadGuard<ShardHolder> (READ)
  2. strict_mode_config update -> consensus -> shards_holder.write() -> BLOCKED
  3. tokio::sync::RwLock is fair: pending writer blocks ALL new readers
  4. Search, scroll, collection info all need shards_holder.read() -> FROZEN

Usage:
    pytest tests/consensus_tests/test_streaming_snapshot_consensus_freeze.py -s -v
"""

import os
import pathlib
import random
import threading
import time

import requests

os.environ.setdefault("PYTEST_CURRENT_TEST", "test_streaming_snapshot_consensus_freeze")

from .utils import (
    assert_http_ok,
    assert_project_root,
    get_collection_cluster_info,
    start_cluster,
    wait_for,
    check_collection_green,
)

COLLECTION = "consensus_freeze_repro"
NUM_POINTS = 50_000
DIM = 128
SEARCH_TIMEOUT = 10


def create_collection(peer_uri: str):
    r = requests.put(
        f"{peer_uri}/collections/{COLLECTION}",
        json={"vectors": {"size": DIM, "distance": "Cosine"}, "shard_number": 1, "replication_factor": 1},
    )
    assert_http_ok(r)


def insert_data(peer_uri: str, num_points: int, batch_size: int = 1000):
    for offset in range(0, num_points, batch_size):
        count = min(batch_size, num_points - offset)
        rng = random.Random(offset)
        points = [
            {"id": offset + i, "vector": [(rng.random() * 2 - 1) for _ in range(DIM)], "payload": {"v": offset + i}}
            for i in range(count)
        ]
        r = requests.put(f"{peer_uri}/collections/{COLLECTION}/points?wait=true", json={"points": points})
        assert_http_ok(r)
    print(f"Inserted {num_points} points")


def find_shard_peer(peer_uris: list):
    for uri in peer_uris:
        info = get_collection_cluster_info(uri, COLLECTION)
        for shard in info.get("local_shards", []):
            if shard["state"] == "Active":
                return uri, shard["shard_id"], info["peer_id"]
    return None, None, None


def slow_download(peer_uri, shard_id, result, stop_event, read_delay=0.1):
    """Download snapshot slowly, keeping server-side READ guard held via backpressure."""
    url = f"{peer_uri}/collections/{COLLECTION}/shards/{shard_id}/snapshot"
    try:
        result["phase"] = "connecting"
        r = requests.get(url, stream=True, timeout=180)
        result["phase"] = "streaming"
        total = 0
        for chunk in r.iter_content(chunk_size=4096):
            total += len(chunk)
            result["bytes"] = total
            if stop_event.is_set():
                result["phase"] = "stopped"
                break
            time.sleep(read_delay)
        result["error"] = None
        if result["phase"] != "stopped":
            result["phase"] = "done"
    except Exception as e:
        result["error"] = str(e)
        result["phase"] = "error"


def timed_request(method, url, timeout, **kwargs):
    """Returns (timed_out, duration, response_or_error)."""
    start = time.time()
    try:
        r = method(url, timeout=timeout, **kwargs)
        return False, time.time() - start, r
    except requests.exceptions.Timeout:
        return True, time.time() - start, None
    except Exception as e:
        return False, time.time() - start, e


def trigger_write_lock(peer_uri, result):
    """PATCH strict_mode_config -> consensus -> shards_holder.write() (blocks if READ held)."""
    try:
        r = requests.patch(
            f"{peer_uri}/collections/{COLLECTION}",
            json={"strict_mode_config": {"enabled": True}},
            timeout=30,
        )
        result["finished"] = time.time()
        result["status"] = r.status_code
    except Exception as e:
        result["finished"] = time.time()
        result["error"] = str(e)


def start_slow_downloads(peer_uri, shard_id, stop_event, n=2):
    """Start n slow snapshot downloads, wait until streaming. Returns (results, threads)."""
    results = [{} for _ in range(n)]
    threads = []
    for i in range(n):
        t = threading.Thread(
            target=slow_download, args=(peer_uri, shard_id, results[i], stop_event), daemon=True,
        )
        threads.append(t)
        t.start()

    deadline = time.time() + 15
    for r in results:
        while time.time() < deadline:
            if r.get("phase") in ("streaming", "done", "error", "stopped"):
                break
            time.sleep(0.2)

    for i, r in enumerate(results):
        print(f"  Download {i}: phase={r.get('phase')}, bytes={r.get('bytes', 0)}")
    streaming = sum(1 for r in results if r.get("phase") == "streaming" and r.get("bytes", 0) > 0)
    assert streaming > 0, f"No downloads streaming: {results}"
    return results, threads


def check_ops_frozen(peer_uri, timeout=SEARCH_TIMEOUT):
    """Check search/info/scroll, return dict of frozen op names -> duration."""
    ops = [
        ("search", requests.post, f"{peer_uri}/collections/{COLLECTION}/points/search",
         {"json": {"vector": [0.1] * DIM, "limit": 5}}),
        ("info", requests.get, f"{peer_uri}/collections/{COLLECTION}", {}),
        ("scroll", requests.post, f"{peer_uri}/collections/{COLLECTION}/points/scroll",
         {"json": {"limit": 5}}),
    ]
    frozen = {}
    for name, method, url, kwargs in ops:
        timedout, dur, _ = timed_request(method, url, timeout, **kwargs)
        print(f"  {name:8s}: timed_out={timedout}, {dur:.2f}s")
        if timedout:
            frozen[name] = dur
    return frozen


def test_streaming_snapshot_freezes_collection_via_consensus(tmp_path: pathlib.Path):
    """Streaming snapshot READ guards + consensus WRITE = frozen collection."""
    assert_project_root()

    # Setup cluster
    peer_uris, _, _ = start_cluster(tmp_path, 3)
    create_collection(peer_uris[0])
    wait_for(check_collection_green, peer_uris[0], COLLECTION, wait_for_timeout=30)
    insert_data(peer_uris[0], NUM_POINTS)
    time.sleep(2)

    shard_peer, shard_id, peer_id = find_shard_peer(peer_uris)
    assert shard_peer, "No active shard found"
    print(f"Shard {shard_id} on peer {peer_id} at {shard_peer}")

    # Baseline search
    timedout, dur, resp = timed_request(
        requests.post, f"{shard_peer}/collections/{COLLECTION}/points/search",
        timeout=10, json={"vector": [0.1] * DIM, "limit": 5},
    )
    assert not timedout and resp.ok, f"Baseline search failed: timedout={timedout}"
    print(f"Baseline search: {dur:.3f}s")

    # Start slow downloads (hold READ guards) + trigger consensus write
    stop_event = threading.Event()
    dl_results, dl_threads = start_slow_downloads(shard_peer, shard_id, stop_event)

    patch_result = {}
    patch_thread = threading.Thread(target=trigger_write_lock, args=(shard_peer, patch_result), daemon=True)
    patch_thread.start()
    time.sleep(5)

    still_streaming = sum(1 for d in dl_results if d.get("phase") == "streaming")
    print(f"PATCH pending: {patch_result.get('finished') is None}, streaming: {still_streaming}")
    assert still_streaming > 0, "Downloads finished before freeze test"

    # Test freeze
    print(f"\nTesting for freeze (timeout={SEARCH_TIMEOUT}s)...")
    frozen = check_ops_frozen(shard_peer)

    # Cleanup
    stop_event.set()
    for t in dl_threads:
        t.join(timeout=30)
    patch_thread.join(timeout=10)

    # Assertions
    assert not frozen, f"Deadlock detected: {len(frozen)} ops frozen: {list(frozen.keys())}"
    print(f"{len(frozen)} operations frozen by consensus deadlock")
