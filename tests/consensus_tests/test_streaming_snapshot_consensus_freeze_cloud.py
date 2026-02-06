#!/usr/bin/env python3
"""
Reproduce streaming snapshot consensus freeze against a live (cloud) cluster.

Deadlock chain:
  1. Streaming snapshots hold OwnedRwLockReadGuard<ShardHolder> (READ)
  2. strict_mode_config update -> consensus -> shards_holder.write() -> BLOCKED
  3. tokio::sync::RwLock is fair: pending writer blocks ALL new readers
  4. Search, scroll, collection info all need shards_holder.read() -> FROZEN

Usage:
    python -m consensus_tests.test_streaming_snapshot_consensus_freeze_cloud --skip-data-gen
    python -m consensus_tests.test_streaming_snapshot_consensus_freeze_cloud --test freeze
"""
import argparse
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from types import SimpleNamespace

import requests

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

PEER_URIS = []


GRPC_URI = ""
API_KEY = None

COLLECTION = "consensus_freeze_repro"
POINTS = 100_000
DIM = 768
SEARCH_TIMEOUT = 10

# ---------------------------------------------------------------------------

_session = requests.Session()


def _setup_session():
    if API_KEY:
        _session.headers["api-key"] = API_KEY


def assert_http_ok(r: requests.Response):
    if not r.ok:
        raise Exception(f"HTTP {r.status_code}: {r.text[:500]}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_bfb(points: int, dim: int, grpc_uri: str):
    cmd = ["docker", "run", "--rm", "--network=host"]
    if API_KEY:
        cmd += ["-e", f"QDRANT_API_KEY={API_KEY}"]
    cmd += [
        "qdrant/bfb:dev", "./bfb",
        "--uri", grpc_uri, "-n", str(points), "--dim", str(dim),
        "--collection-name", COLLECTION, "--shards", "1",
        "--keywords", "1000", "--float-payloads", "true",
        "--int-payloads", "1000", "--timestamp-payload", "--uuid-payloads",
        "--geo-payloads", "--text-payloads", "--indexing-threshold", "1",
    ]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def get_collection_cluster_info(peer_uri: str) -> dict:
    r = _session.get(f"{peer_uri}/collections/{COLLECTION}/cluster")
    assert_http_ok(r)
    return r.json()["result"]


def wait_collection_green(peer_uri: str, timeout: float = 120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            r = _session.get(f"{peer_uri}/collections/{COLLECTION}")
            if r.ok and r.json().get("result", {}).get("status") == "green":
                return
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(f"Collection not green after {timeout}s")


def find_shard_peer():
    for uri in PEER_URIS:
        info = get_collection_cluster_info(uri)
        peer_id = info["peer_id"]
        for shard in info.get("local_shards", []):
            if shard.get("state") == "Active":
                return uri, shard["shard_id"], peer_id
    raise Exception("No peer with an active local shard found")


def slow_download(peer_uri, shard_id, result, stop_event, read_delay=0.1):
    """Download snapshot slowly, keeping server-side READ guard held via backpressure."""
    url = f"{peer_uri}/collections/{COLLECTION}/shards/{shard_id}/snapshot"
    try:
        result["phase"] = "connecting"
        r = _session.get(url, stream=True, timeout=300)
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


def timed_search(peer_uri, timeout=SEARCH_TIMEOUT):
    return timed_request(
        _session.post, f"{peer_uri}/collections/{COLLECTION}/points/search",
        timeout, json={"vector": [0.1] * DIM, "limit": 5},
    )


def trigger_write_lock(peer_uri, result):
    """PATCH strict_mode_config -> consensus -> shards_holder.write() (blocks if READ held)."""
    try:
        r = _session.patch(
            f"{peer_uri}/collections/{COLLECTION}",
            json={"strict_mode_config": {"enabled": True}},
            timeout=60,
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

    deadline = time.time() + 30
    for r in results:
        while time.time() < deadline:
            if r.get("phase") in ("streaming", "done", "error", "stopped"):
                break
            time.sleep(0.5)

    for i, r in enumerate(results):
        print(f"  Download {i}: phase={r.get('phase')}, bytes={r.get('bytes', 0)}")
    streaming = sum(1 for r in results if r.get("phase") == "streaming" and r.get("bytes", 0) > 0)
    assert streaming > 0, f"No downloads streaming: {results}"
    return results, threads


def check_ops_frozen(peer_uri, timeout=SEARCH_TIMEOUT):
    """Check search/info/scroll, return dict of frozen op names -> duration."""
    ops = [
        ("search", _session.post, f"{peer_uri}/collections/{COLLECTION}/points/search",
         {"json": {"vector": [0.1] * DIM, "limit": 5}}),
        ("info", _session.get, f"{peer_uri}/collections/{COLLECTION}", {}),
        ("scroll", _session.post, f"{peer_uri}/collections/{COLLECTION}/points/scroll",
         {"json": {"limit": 5}}),
    ]
    frozen = {}
    for name, method, url, kwargs in ops:
        timedout, dur, _ = timed_request(method, url, timeout, **kwargs)
        print(f"  {name:8s}: timed_out={timedout}, {dur:.2f}s")
        if timedout:
            frozen[name] = dur
    return frozen


@contextmanager
def freeze_scenario(n_downloads=2):
    """Set up deadlock: slow downloads + consensus write. Yields context, cleans up on exit."""
    shard_peer, shard_id, peer_id = find_shard_peer()
    print(f"\nShard {shard_id} on peer {peer_id} at {shard_peer}")

    # Baseline
    timedout, dur, resp = timed_search(shard_peer)
    assert not timedout and resp.ok, "Baseline search failed"
    print(f"Baseline search: {dur:.3f}s")

    # Start slow downloads + trigger consensus write
    stop_event = threading.Event()
    dl_results, dl_threads = start_slow_downloads(shard_peer, shard_id, stop_event, n_downloads)

    patch_result = {}
    patch_thread = threading.Thread(target=trigger_write_lock, args=(shard_peer, patch_result), daemon=True)
    patch_thread.start()
    time.sleep(5)

    still_streaming = sum(1 for d in dl_results if d.get("phase") == "streaming")
    print(f"PATCH pending: {patch_result.get('finished') is None}, streaming: {still_streaming}")
    assert still_streaming > 0, "Downloads finished before freeze test"

    ctx = SimpleNamespace(
        shard_peer=shard_peer, stop_event=stop_event,
        dl_results=dl_results, dl_threads=dl_threads,
        patch_result=patch_result, patch_thread=patch_thread,
    )
    try:
        yield ctx
    finally:
        stop_event.set()
        for t in dl_threads:
            t.join(timeout=30)
        patch_thread.join(timeout=10)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_consensus_freeze():
    """Streaming snapshot READ guards + consensus WRITE = frozen collection."""
    with freeze_scenario() as ctx:
        frozen = check_ops_frozen(ctx.shard_peer)

    if frozen:
        print(f"\n>>> DEADLOCK CONFIRMED: {len(frozen)} ops frozen: {list(frozen.keys())}")
    assert "search" in frozen, f"Expected search timeout: {frozen}"
    assert len(frozen) >= 2, f"Only search froze, expected systemic freeze: {frozen}"
    print(f"Test PASSED: {len(frozen)} operations frozen")


def test_consensus_freeze_persists():
    """Freeze persists as long as downloads hold READ guards (not a transient spike)."""
    with freeze_scenario() as ctx:
        freeze_checks = []
        for i in range(3):
            still_streaming = sum(1 for d in ctx.dl_results if d.get("phase") == "streaming")
            if still_streaming == 0:
                print(f"  Check {i}: downloads finished, stopping")
                break
            timedout, dur, _ = timed_search(ctx.shard_peer)
            freeze_checks.append(timedout)
            print(f"  Check {i}: timed_out={timedout}, {dur:.2f}s, streaming={still_streaming}")

    frozen_count = sum(freeze_checks)
    assert frozen_count >= 2, f"Expected persistent freeze: {freeze_checks}"
    print(f"Test PASSED: freeze persists across {frozen_count} consecutive checks")


def test_freeze_recovery():
    """Collection unfreezes after downloads stop (proving READ guards are the cause)."""
    with freeze_scenario() as ctx:
        # Confirm frozen
        timedout, dur, _ = timed_search(ctx.shard_peer)
        assert timedout, f"Expected freeze but search completed in {dur:.2f}s"
        print(f"Confirmed frozen: search timed out after {dur:.2f}s")

        # Release READ guards
        print("\nStopping downloads (releasing READ guards)...")
        ctx.stop_event.set()
        for t in ctx.dl_threads:
            t.join(timeout=30)
        ctx.patch_thread.join(timeout=30)
        time.sleep(2)

        # Should be unfrozen now
        timedout, dur, _ = timed_search(ctx.shard_peer)
        print(f"After release: search timed_out={timedout}, {dur:.2f}s")
        assert not timedout, f"Still frozen after downloads stopped ({dur:.2f}s)"

    print(f"Test PASSED: collection recovered after READ guards released")


# ---------------------------------------------------------------------------
# CLI runner
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Reproduce streaming snapshot consensus freeze on a live cluster")
    parser.add_argument("--skip-data-gen", action="store_true",
                        help="Skip bfb data generation (collection must exist)")
    parser.add_argument("--test", type=str, default="all",
                        choices=["all", "freeze", "persist", "recovery"],
                        help="Which test to run")
    args = parser.parse_args()

    _setup_session()

    if not args.skip_data_gen:
        print(f"Generating {POINTS} points (dim={DIM}) via bfb...")
        run_bfb(POINTS, DIM, GRPC_URI)
        wait_collection_green(PEER_URIS[0])
        print("Collection is green, waiting for indexing to settle...")
        time.sleep(3)
    else:
        print(f"Skipping data gen, using existing collection '{COLLECTION}'")
        wait_collection_green(PEER_URIS[0], timeout=10)

    tests = {
        "freeze": ("Consensus freeze", test_consensus_freeze),
        "persist": ("Freeze persists", test_consensus_freeze_persists),
        "recovery": ("Freeze recovery", test_freeze_recovery),
    }

    to_run = tests.keys() if args.test == "all" else [args.test]
    passed, failed = 0, 0

    for key in to_run:
        name, fn = tests[key]
        print(f"\n{'=' * 60}")
        print(f"TEST: {name}")
        print('=' * 60)
        try:
            fn()
            passed += 1
            print(f"\n  PASSED")
        except Exception as e:
            failed += 1
            print(f"\n  FAILED: {e}")

    print(f"\n{'=' * 60}")
    print(f"Results: {passed} passed, {failed} failed")
    print('=' * 60)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
