"""
Reproduce cluster freeze during snapshot streaming under load.

3-node cluster, 100K+ points, continuous bfb uploads+searches,
slow HTTP snapshot downloads + consensus write → collection frozen.

Lock convoy chain (from GDB backtraces):
  Segment level: snapshot_all_segments holds UPGRADABLE READ on SegmentHolder
    → backpressure on duplex(4096) keeps locks held
    → upsert needs EXCLUSIVE → blocked → searches need SHARED → blocked
  shards_holder level: streaming snapshot holds READ (tokio::sync::RwLock)
    → consensus write needs WRITE → blocked → FIFO fairness blocks all new READs

Usage:
    pytest tests/consensus_tests/test_snapshot_transfer_lock_convoy.py -s -v
"""

import os
import pathlib
import shlex
import subprocess
import threading
import time

import requests

os.environ.setdefault("PYTEST_CURRENT_TEST", "test_snapshot_transfer_lock_convoy")

from .utils import (
    assert_project_root,
    get_collection_cluster_info,
    start_cluster,
    wait_for,
    check_collection_green,
    processes,
)

COLLECTION = "lock_convoy_repro"
DIM = 768
NUM_POINTS = 100_000
TIMEOUT = 10
API_KEY = ""


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

BFB_IMAGE = "qdrant/bfb:dev"


def _kill_proc(p):
    """Kill subprocess, then ensure its Docker container is stopped too."""
    if not p:
        return
    p.kill()
    p.wait()


def _cleanup_docker():
    """Kill any leftover bfb Docker containers (PID 1 may ignore SIGTERM)."""
    r = subprocess.run(
        ["docker", "ps", "-q", "--filter", f"ancestor={BFB_IMAGE}"],
        capture_output=True, text=True,
    )
    for cid in r.stdout.strip().split("\n"):
        if cid:
            subprocess.run(["docker", "rm", "-f", cid], capture_output=True)


def _bfb_base():
    if os.environ.get("BFB_PATH"):
        return shlex.split(os.environ["BFB_PATH"])
    cmd = ["docker", "run", "--rm", "--network=host"]
    if API_KEY:
        cmd += ["-e", f"QDRANT_API_KEY={API_KEY}"]
    cmd += [BFB_IMAGE, "./bfb"]
    return cmd


def _grpc_uris(peer_uris):
    """Look up actual gRPC URIs from the process registry (ports are random)."""
    result = []
    for uri in peer_uris:
        http_port = int(uri.rsplit(":", 1)[1])
        host = uri.rsplit(":", 1)[0]
        for p in processes:
            if p.http_port == http_port:
                result.append(f"{host}:{p.grpc_port}")
                break
    return result


def _bfb(grpc_uris, *extra):
    cmd = _bfb_base()
    for u in grpc_uris:
        cmd += ["--uri", u]
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


def bfb_init(grpc_uris, n=NUM_POINTS):
    """Create collection + upload initial data."""
    print(f"  bfb: creating collection + uploading {n} points...")
    subprocess.run(_bfb(grpc_uris,
        "-n", n, "--shards", 3, "--replication-factor", 2,
        "--batch-size", 100, "--threads", 1, "--parallel", 1,
        "--create-if-missing", "--on-disk-vectors", "true",
        "--timestamp-payload",
    ), check=True)


def bfb_upload(grpc_uris):
    """Continuous upserts in background (infinite, --delay 1000ms)."""
    return subprocess.Popen(_bfb(grpc_uris,
        "-n", 1_000_000_000, "--max-id", 200_000, "--delay", 1000,
        "--batch-size", 100, "--threads", 1, "--parallel", 1,
        "--wait-on-upsert", "--skip-create",
        "--on-disk-vectors", "true", "--timestamp-payload",
    ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def bfb_search(grpc_uris, n=1_000_000):
    """Continuous searches in background."""
    return subprocess.Popen(_bfb(grpc_uris,
        "-n", n, "--threads", 1, "--parallel", 1,
        "--skip-create", "--skip-upload", "--skip-wait-index",
        "--search", "--search-limit", 10, "--quantization-rescore", "true",
    ), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def timed_req(method, url, timeout=TIMEOUT, **kw):
    t0 = time.time()
    try:
        r = method(url, timeout=timeout, **kw)
        return False, time.time() - t0, r
    except requests.exceptions.Timeout:
        return True, time.time() - t0, None
    except Exception as e:
        return False, time.time() - t0, e


def check_frozen(peer_uri):
    """Returns dict of op_name→duration for ops that timed out."""
    frozen = {}
    for name, method, url, kw in [
        ("search", requests.post,
         f"{peer_uri}/collections/{COLLECTION}/points/search",
         {"json": {"vector": [0.1] * DIM, "limit": 5}}),
        ("cluster_info", requests.get,
         f"{peer_uri}/collections/{COLLECTION}/cluster", {}),
        ("scroll", requests.post,
         f"{peer_uri}/collections/{COLLECTION}/points/scroll",
         {"json": {"limit": 5}}),
    ]:
        timedout, dur, _ = timed_req(method, url, **kw)
        tag = "TIMEOUT" if timedout else "ok"
        print(f"    {name:15s}: {tag} ({dur:.1f}s)")
        if timedout:
            frozen[name] = dur
    return frozen


def find_shard_peer(peer_uris):
    for uri in peer_uris:
        info = get_collection_cluster_info(uri, COLLECTION)
        for s in info.get("local_shards", []):
            if s["state"] == "Active":
                return uri, s["shard_id"], info["peer_id"]
    return None, None, None


def slow_download(peer_uri, shard_id, result, stop, delay=0.15):
    url = f"{peer_uri}/collections/{COLLECTION}/shards/{shard_id}/snapshot"
    try:
        r = requests.get(url, stream=True, timeout=300)
        result["phase"] = "streaming"
        total = 0
        for chunk in r.iter_content(chunk_size=4096):
            total += len(chunk)
            result["bytes"] = total
            if stop.is_set():
                break
            time.sleep(delay)
        result["phase"] = "done"
    except Exception as e:
        result["phase"] = "error"
        result["error"] = str(e)


def trigger_strict_mode(peer_uri, enabled, result):
    """PATCH strict_mode → consensus → shards_holder.write()."""
    try:
        r = requests.patch(
            f"{peer_uri}/collections/{COLLECTION}",
            json={"strict_mode_config": {"enabled": enabled}},
            timeout=120,
        )
        result["status"] = r.status_code
    except Exception as e:
        result["error"] = str(e)


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

def test_slow_downloads_plus_consensus_write_freezes(tmp_path: pathlib.Path):
    """
    Slow HTTP streaming snapshot downloads + consensus write + bfb load → frozen.

    Slow downloads hold shards_holder READ + create backpressure on segment locks.
    Consensus write queues shards_holder WRITE → FIFO blocks all new reads.
    """
    assert_project_root()
    peer_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3)
    grpc_uris = _grpc_uris(peer_uris)

    bfb_init(grpc_uris)
    wait_for(check_collection_green, peer_uris[0], COLLECTION, wait_for_timeout=60)

    upload_proc = search_proc = None
    stop = threading.Event()
    dl_threads = []
    try:
        upload_proc = bfb_upload(grpc_uris)
        search_proc = bfb_search(grpc_uris)

        src_uri, shard_id, _ = find_shard_peer(peer_uris)
        assert src_uri

        # Slow streaming downloads (hold READ + backpressure on segments)
        dl_results = [{}, {}, {}]
        for i in range(3):
            t = threading.Thread(
                target=slow_download,
                args=(src_uri, shard_id, dl_results[i], stop), daemon=True,
            )
            t.start()
            dl_threads.append(t)

        # Wait for streaming to start
        for _ in range(30):
            if any(d.get("phase") == "streaming" for d in dl_results):
                break
            time.sleep(0.5)
        streaming = sum(1 for d in dl_results if d.get("phase") == "streaming")
        print(f"  {streaming} downloads streaming")

        # Trigger consensus write
        sm_result = {}
        threading.Thread(
            target=trigger_strict_mode, args=(src_uri, True, sm_result), daemon=True,
        ).start()
        time.sleep(5)

        # Check freeze
        print("  checking for freeze...")
        frozen = check_frozen(src_uri)

        if frozen:
            print(f"\n  >>> FREEZE CONFIRMED: {list(frozen.keys())}")
        assert frozen, "Source node did not freeze"

    finally:
        stop.set()
        for t in dl_threads:
            t.join(timeout=15)
        for p in (upload_proc, search_proc):
            _kill_proc(p)
        _cleanup_docker()