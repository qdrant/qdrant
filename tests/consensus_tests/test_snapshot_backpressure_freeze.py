"""
Test that GET /collections/{name}/cluster remains responsive during
slow streaming shard snapshot downloads with concurrent upserts.

3-node cluster, 50K points, continuous upserts running in background,
3 concurrent slow snapshot downloads creating backpressure on the source node.
Verifies cluster_info doesn't time out.
"""

import os
import pathlib
import random
import threading
import time

import requests

os.environ.setdefault("PYTEST_CURRENT_TEST", "test_snapshot_backpressure_freeze")

from .utils import (
    assert_http_ok,
    assert_project_root,
    get_collection_cluster_info,
    start_cluster,
    wait_for,
    check_collection_green,
)

COLLECTION = "backpressure_freeze"
DIM = 128
NUM_POINTS = 50_000
CLUSTER_INFO_TIMEOUT = 10
NUM_SLOW_DOWNLOADS = 3
CONVOY_FORM_DELAY = 10


def create_collection(peer_uri: str):
    r = requests.put(
        f"{peer_uri}/collections/{COLLECTION}",
        json={
            "vectors": {"size": DIM, "distance": "Cosine"},
            "shard_number": 3,
            "replication_factor": 2,
        },
    )
    assert_http_ok(r)


def insert_points(peer_uri: str, num_points: int, batch_size: int = 1000):
    for offset in range(0, num_points, batch_size):
        count = min(batch_size, num_points - offset)
        rng = random.Random(offset)
        points = [
            {
                "id": offset + i,
                "vector": [rng.random() * 2 - 1 for _ in range(DIM)],
                "payload": {"v": offset + i},
            }
            for i in range(count)
        ]
        r = requests.put(
            f"{peer_uri}/collections/{COLLECTION}/points?wait=true",
            json={"points": points},
        )
        assert_http_ok(r)
    print(f"  inserted {num_points} points")


def find_active_shard(peer_uris: list[str]) -> tuple[str, int]:
    for uri in peer_uris:
        info = get_collection_cluster_info(uri, COLLECTION)
        for shard in info.get("local_shards", []):
            if shard["state"] == "Active":
                return uri, shard["shard_id"]
    raise RuntimeError("No peer with an active local shard")


def upsert_loop(peer_uri: str, stop: threading.Event):
    rng = random.Random(42)
    while not stop.is_set():
        points = [
            {
                "id": rng.randint(0, NUM_POINTS * 2),
                "vector": [rng.random() * 2 - 1 for _ in range(DIM)],
            }
            for _ in range(100)
        ]
        try:
            requests.put(
                f"{peer_uri}/collections/{COLLECTION}/points?wait=true",
                json={"points": points},
                timeout=30,
            )
        except requests.RequestException:
            pass


def slow_download(peer_uri: str, shard_id: int, streaming: threading.Event, stop: threading.Event):
    url = f"{peer_uri}/collections/{COLLECTION}/shards/{shard_id}/snapshot"
    try:
        r = requests.get(url, stream=True, timeout=300)
        if r.status_code != 200:
            return
        streaming.set()
        for _ in r.iter_content(chunk_size=4096):
            if stop.is_set():
                break
            time.sleep(0.15)
    except requests.RequestException:
        pass


def test_snapshot_backpressure_freeze(tmp_path: pathlib.Path):
    """Verify cluster_info responds under slow snapshot downloads + upserts."""
    assert_project_root()
    peer_uris, _, _ = start_cluster(tmp_path, 3)

    create_collection(peer_uris[0])
    wait_for(check_collection_green, peer_uris[0], COLLECTION, wait_for_timeout=30)
    insert_points(peer_uris[0], NUM_POINTS)

    src_uri, shard_id = find_active_shard(peer_uris)
    print(f"  target: {src_uri} shard={shard_id}")

    stop = threading.Event()
    streaming = threading.Event()
    threads = []

    try:
        # Continuous upserts — creates SegmentHolder EXCLUSIVE pressure
        t = threading.Thread(target=upsert_loop, args=(src_uri, stop), daemon=True)
        t.start()
        threads.append(t)

        # Slow streaming snapshot downloads — backpressure holds segment locks
        for _ in range(NUM_SLOW_DOWNLOADS):
            t = threading.Thread(
                target=slow_download,
                args=(src_uri, shard_id, streaming, stop),
                daemon=True,
            )
            t.start()
            threads.append(t)

        assert streaming.wait(timeout=30), "No snapshot download started streaming"

        # Let the lock convoy form
        time.sleep(CONVOY_FORM_DELAY)

        # cluster_info calls count_local which needs segments.read() — no remote fallback
        try:
            requests.get(
                f"{src_uri}/collections/{COLLECTION}/cluster",
                timeout=CLUSTER_INFO_TIMEOUT,
            )
            frozen = False
        except requests.exceptions.Timeout:
            frozen = True

        print(f"  cluster_info: {'FREEZE confirmed' if frozen else 'ok'}")
        assert not frozen, "cluster_info froze due to SegmentHolder lock convoy"

    finally:
        stop.set()
        for t in threads:
            t.join(timeout=15)
