#!/usr/bin/env python3
"""
Reproduce snapshot transfer lock convoy / cluster freeze against a live cluster.

Lock convoy chain:
  1. Slow streaming snapshot downloads hold shards_holder READ + segment locks
  2. bfb upserts create segment-level EXCLUSIVE pressure (backpressure on duplex)
  3. Consensus write (strict_mode) needs shards_holder.write() → BLOCKED
  4. tokio::sync::RwLock FIFO fairness blocks ALL new readers
  5. Search, scroll, cluster info all freeze

Usage:
    python tests/consensus_tests/test_snapshot_transfer_lock_convoy_cloud.py
    python tests/consensus_tests/test_snapshot_transfer_lock_convoy_cloud.py --skip-data-gen
    python tests/consensus_tests/test_snapshot_transfer_lock_convoy_cloud.py --test transfer_freeze
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

PEER_URIS = [
]

GRPC_URI = ""

API_KEY = ""

COLLECTION = "lock_repro"
NUM_POINTS = 100_000
DIM = 768
SEARCH_TIMEOUT = 10
BFB_IMAGE = "qdrant/bfb:dev"

_session = requests.Session()
if API_KEY:
    _session.headers["api-key"] = API_KEY

# ---------------------------------------------------------------------------
# bfb helpers
# ---------------------------------------------------------------------------

def _kill_proc(p):
    if not p:
        return
    p.kill()
    p.wait()


def _cleanup_docker():
    r = subprocess.run(
        ["docker", "ps", "-q", "--filter", f"ancestor={BFB_IMAGE}"],
        capture_output=True, text=True,
    )
    for cid in r.stdout.strip().split("\n"):
        if cid:
            subprocess.run(["docker", "rm", "-f", cid], capture_output=True)


def _bfb(*extra):
    cmd = ["docker", "run", "--rm", "--network=host"]
    if API_KEY:
        cmd += ["-e", f"QDRANT_API_KEY={API_KEY}"]
    cmd += [BFB_IMAGE, "./bfb", "--uri", GRPC_URI]
    cmd += [
        "--collection-name", COLLECTION,
        "--dim", str(DIM),
        "--keywords", "10",
        "--sparse-dim", "3000",
        "--sparse-vectors", "0.05",
        "--quantization", "scalar",
        "--timing-threshold", "1",
        "--timeout", "30",
        "--retry", "4",
        "--retry-interval", "1",
        "--indexing-threshold", "0",
    ]
    cmd += [str(a) for a in extra]
    return cmd


def bfb_init(n=NUM_POINTS):
    print(f"  bfb: creating collection + uploading {n} points...")
    subprocess.run(_bfb(
        "-n", n, "--shards", 3, "--replication-factor", 2,
        "--batch-size", 100, "--threads", 1, "--parallel", 1,
        "--create-if-missing", "--on-disk-vectors", "true",
        "--timestamp-payload",
    ), check=True)


def bfb_upload():
    return subprocess.Popen(_bfb(
        "-n", 1_000_000_000, "--max-id", 200_000, "--delay", 1000,
        "--batch-size", 100, "--threads", 1, "--parallel", 1,
        "--wait-on-upsert", "--skip-create",
        "--on-disk-vectors", "true", "--timestamp-payload",
    ))



# ---------------------------------------------------------------------------
# Cluster helpers
# ---------------------------------------------------------------------------

def wait_collection_green(timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            r = _session.get(f"{PEER_URIS[0]}/collections/{COLLECTION}")
            if r.ok and r.json().get("result", {}).get("status") == "green":
                return
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(f"Collection not green after {timeout}s")


def cluster_info(peer_uri):
    r = _session.get(f"{peer_uri}/collections/{COLLECTION}/cluster")
    r.raise_for_status()
    return r.json()["result"]


def find_shard_peer():
    for uri in PEER_URIS:
        info = cluster_info(uri)
        for s in info.get("local_shards", []):
            if s.get("state") == "Active":
                return uri, s["shard_id"], info["peer_id"]
    raise Exception("No peer with an active local shard")


# ---------------------------------------------------------------------------
# Lock-pressure helpers
# ---------------------------------------------------------------------------

def timed_search(peer_uri, timeout=SEARCH_TIMEOUT):
    t0 = time.time()
    try:
        r = _session.post(
            f"{peer_uri}/collections/{COLLECTION}/points/search",
            json={"vector": [0.1] * DIM, "limit": 5}, timeout=timeout,
        )
        return False, time.time() - t0, r
    except requests.exceptions.Timeout:
        return True, time.time() - t0, None


def check_ops_frozen(peer_uri, timeout=SEARCH_TIMEOUT):
    frozen = {}
    for name, method, url, kwargs in [
        ("search", _session.post,
         f"{peer_uri}/collections/{COLLECTION}/points/search",
         {"json": {"vector": [0.1] * DIM, "limit": 5}}),
        ("cluster_info", _session.get,
         f"{peer_uri}/collections/{COLLECTION}/cluster", {}),
        ("scroll", _session.post,
         f"{peer_uri}/collections/{COLLECTION}/points/scroll",
         {"json": {"limit": 5}}),
    ]:
        t0 = time.time()
        try:
            method(url, timeout=timeout, **kwargs)
            timedout, dur = False, time.time() - t0
        except requests.exceptions.Timeout:
            timedout, dur = True, time.time() - t0
        print(f"    {name:15s}: {'TIMEOUT' if timedout else 'ok':7s} ({dur:.2f}s)")
        if timedout:
            frozen[name] = dur
    return frozen


def slow_download(peer_uri, shard_id, result, stop_event, delay=0.15):
    url = f"{peer_uri}/collections/{COLLECTION}/shards/{shard_id}/snapshot"
    try:
        r = _session.get(url, stream=True, timeout=300)
        result["phase"] = "streaming"
        total = 0
        for chunk in r.iter_content(chunk_size=4096):
            total += len(chunk)
            result["bytes"] = total
            if stop_event.is_set():
                break
            time.sleep(delay)
        result["phase"] = "done"
    except Exception as e:
        result["phase"] = "error"
        result["error"] = str(e)


def start_slow_downloads(peer_uri, shard_id, stop_event, n=3):
    results = [{} for _ in range(n)]
    threads = []
    for i in range(n):
        t = threading.Thread(
            target=slow_download, args=(peer_uri, shard_id, results[i], stop_event),
            daemon=True,
        )
        t.start()
        threads.append(t)

    for _ in range(60):
        if any(r.get("phase") == "streaming" for r in results):
            break
        time.sleep(0.5)

    streaming = sum(1 for r in results if r.get("phase") == "streaming")
    print(f"  {streaming}/{n} downloads streaming")
    assert streaming > 0, f"No downloads streaming: {results}"
    return results, threads


def trigger_strict_mode(peer_uri, result):
    """PATCH strict_mode → consensus → shards_holder.write()."""
    try:
        r = _session.patch(
            f"{peer_uri}/collections/{COLLECTION}",
            json={"strict_mode_config": {"enabled": True}},
            timeout=120,
        )
        result["status"] = r.status_code
    except Exception as e:
        result["error"] = str(e)


@contextmanager
def freeze_scenario(n_downloads=3):
    """Set up lock convoy: bfb load + slow downloads + consensus write."""
    shard_peer, shard_id, _ = find_shard_peer()

    timedout, dur, resp = timed_search(shard_peer)
    assert not timedout and resp.ok, "Baseline search failed"
    print(f"Baseline search: {dur:.3f}s")

    upload_proc = None
    stop_event = threading.Event()
    dl_threads = []
    patch_thread = None
    try:
        upload_proc = bfb_upload()
        print("bfb upload started")
        time.sleep(3)

        dl_results, dl_threads = start_slow_downloads(shard_peer, shard_id, stop_event, n_downloads)

        patch_result = {}
        patch_thread = threading.Thread(
            target=trigger_strict_mode, args=(shard_peer, patch_result), daemon=True,
        )
        patch_thread.start()
        time.sleep(5)

        yield SimpleNamespace(
            shard_peer=shard_peer, stop_event=stop_event,
            dl_results=dl_results, dl_threads=dl_threads,
            patch_thread=patch_thread,
        )
    finally:
        stop_event.set()
        for t in dl_threads:
            t.join(timeout=30)
        if patch_thread:
            patch_thread.join(timeout=30)
        _kill_proc(upload_proc)
        _cleanup_docker()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_downloads_plus_bfb_load_freeze():
    """Slow streaming downloads + consensus write + bfb upload/search → frozen."""
    with freeze_scenario() as ctx:
        frozen = check_ops_frozen(ctx.shard_peer)
    if frozen:
        print(f"\n>>> FREEZE CONFIRMED: {list(frozen.keys())}")
    assert "search" in frozen, f"Expected search timeout: {frozen}"


def test_freeze_recovery():
    """Collection unfreezes after downloads stop."""
    with freeze_scenario() as ctx:
        timedout, dur, _ = timed_search(ctx.shard_peer)
        assert timedout, f"Expected freeze but search completed in {dur:.2f}s"
        print(f"Confirmed frozen ({dur:.2f}s)")

        ctx.stop_event.set()
        for t in ctx.dl_threads:
            t.join(timeout=30)
        ctx.patch_thread.join(timeout=30)
        time.sleep(2)

        timedout, dur, _ = timed_search(ctx.shard_peer)
        print(f"After release: timed_out={timedout}, {dur:.2f}s")
        assert not timedout, f"Still frozen after downloads stopped ({dur:.2f}s)"

    print("Collection recovered after READ guards released")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Reproduce snapshot transfer lock convoy on a live cluster")
    parser.add_argument("--skip-data-gen", action="store_true")
    parser.add_argument("--test", default="all",
                        choices=["all", "downloads_bfb_freeze", "recovery"])
    args = parser.parse_args()

    if not args.skip_data_gen:
        bfb_init()
        wait_collection_green()
        time.sleep(3)
    else:
        wait_collection_green(timeout=10)

    tests = {
        "downloads_bfb_freeze": ("Downloads + bfb + consensus write", test_downloads_plus_bfb_load_freeze),
        "recovery": ("Freeze recovery after downloads stop", test_freeze_recovery),
    }

    to_run = tests.keys() if args.test == "all" else [args.test]
    passed, failed = 0, 0

    for key in to_run:
        name, fn = tests[key]
        print(f"\n{'='*60}\nTEST: {name}\n{'='*60}")
        try:
            fn()
            passed += 1
            print("  PASSED")
        except Exception as e:
            failed += 1
            print(f"  FAILED: {e}")

    print(f"\nResults: {passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()