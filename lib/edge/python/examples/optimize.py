#!/usr/bin/env python3
"""
Test edge optimize by preparing data in a running local Qdrant, downloading a
shard snapshot, and optimizing the restored edge shard.

Requires a running Qdrant on localhost:6333. The example creates and drops its
own temporary collection, including all points needed for the demo.
"""

import os
import shutil

import requests
from common import DATA_DIR

from qdrant_edge import *

COLLECTION = "edge_optimize_example"
QDRANT_URL = "http://localhost:6333"
SNAPSHOT_DIR = os.path.join(DATA_DIR, "snapshots")
SNAPSHOT_PATH = os.path.join(SNAPSHOT_DIR, "optimize.snapshot")
SHARD_DIR = os.path.join(DATA_DIR, "optimize_test_shard")
VECTOR_SIZE = 384
POINT_COUNT = 1000
DELETED_COUNT = 250
BATCH_SIZE = 100
SEARCH_POINT_ID = POINT_COUNT - 1


def request_json(method: str, path: str, **kwargs):
    response = requests.request(method, f"{QDRANT_URL}{path}", timeout=120, **kwargs)
    response.raise_for_status()
    if response.content:
        return response.json()
    return None


def ensure_qdrant_available():
    try:
        request_json("GET", "/collections")
    except requests.RequestException as err:
        raise SystemExit(
            "Qdrant is not reachable at http://localhost:6333.\n"
            "Start one locally, for example with:\n"
            "docker run -d --name qdrant-local -p 6333:6333 -p 6334:6334 qdrant/qdrant:latest"
        ) from err


def drop_collection():
    response = requests.delete(
        f"{QDRANT_URL}/collections/{COLLECTION}?wait=true",
        timeout=120,
    )
    if response.status_code not in (200, 202, 404):
        response.raise_for_status()


def build_vector(point_id: int) -> list[float]:
    return [
        ((point_id * 17 + dimension * 13) % 1000) / 1000.0
        for dimension in range(VECTOR_SIZE)
    ]


def build_point(point_id: int) -> dict:
    return {
        "id": point_id,
        "vector": build_vector(point_id),
        "payload": {
            "group": f"group-{point_id % 5}",
            "bucket": point_id % 10,
            "city": f"city-{point_id % 17}",
        },
    }


def prepare_collection():
    print(f"Preparing collection '{COLLECTION}' with {POINT_COUNT} points...")
    drop_collection()

    request_json(
        "PUT",
        f"/collections/{COLLECTION}",
        json={
            "vectors": {
                "size": VECTOR_SIZE,
                "distance": "Dot",
            }
        },
    )

    for start in range(1, POINT_COUNT + 1, BATCH_SIZE):
        end = min(start + BATCH_SIZE - 1, POINT_COUNT)
        print(f"  Uploading points {start}..{end}")
        request_json(
            "PUT",
            f"/collections/{COLLECTION}/points?wait=true",
            json={
                "points": [build_point(point_id) for point_id in range(start, end + 1)]
            },
        )

    remote_count = request_json(
        "POST",
        f"/collections/{COLLECTION}/points/count",
        json={"exact": True},
    )["result"]["count"]
    assert remote_count == POINT_COUNT, (
        f"Unexpected point count in Qdrant: expected {POINT_COUNT}, got {remote_count}"
    )
    print(f"Collection ready, exact count: {remote_count}")


def create_shard_snapshot() -> str:
    prepare_collection()

    print(f"Creating shard snapshot for collection '{COLLECTION}' shard 0...")
    response = request_json("POST", f"/collections/{COLLECTION}/shards/0/snapshots")
    snapshot_name = response["result"]["name"]
    print(f"Snapshot created: {snapshot_name}")

    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    if os.path.exists(SNAPSHOT_PATH):
        os.remove(SNAPSHOT_PATH)

    print("Downloading snapshot...")
    download = requests.get(
        f"{QDRANT_URL}/collections/{COLLECTION}/shards/0/snapshots/{snapshot_name}",
        stream=True,
        timeout=120,
    )
    download.raise_for_status()

    with open(SNAPSHOT_PATH, "wb") as file_handle:
        for chunk in download.iter_content(chunk_size=8192):
            file_handle.write(chunk)

    print(f"Downloaded to {SNAPSHOT_PATH}")
    return SNAPSHOT_PATH


def load_edge_shard() -> EdgeShard:
    return EdgeShard.load(
        SHARD_DIR,
        EdgeConfig(
            vectors=EdgeVectorParams(size=VECTOR_SIZE, distance=Distance.Dot),
            optimizers=EdgeOptimizersConfig(
                deleted_threshold=0.2,
                vacuum_min_vector_number=100,
                default_segment_number=2,
            ),
        ),
    )


def main():
    ensure_qdrant_available()

    try:
        snapshot_path = create_shard_snapshot()

        if os.path.exists(SHARD_DIR):
            shutil.rmtree(SHARD_DIR)

        print(f"\nUnpacking snapshot to {SHARD_DIR}...")
        EdgeShard.unpack_snapshot(snapshot_path, SHARD_DIR)

        print("Loading edge shard...")
        shard = load_edge_shard()

        initial_count = shard.count(CountRequest(exact=True))
        print(f"\nInitial exact point count: {initial_count}")
        assert initial_count == POINT_COUNT, (
            f"Unexpected initial point count: expected {POINT_COUNT}, got {initial_count}"
        )

        deleted_ids = list(range(4, POINT_COUNT + 1, 4))
        print(f"Deleting {len(deleted_ids)} points to create a vacuum candidate...")
        shard.update(UpdateOperation.delete_points(deleted_ids))

        info_before = shard.info()
        count_before = shard.count(CountRequest(exact=True))
        expected_count = POINT_COUNT - DELETED_COUNT
        print("\nBefore optimize:")
        print(f"  {info_before}")
        print(f"  Exact point count: {count_before}")
        assert count_before == expected_count, (
            f"Unexpected count before optimize: expected {expected_count}, got {count_before}"
        )

        print("\nRunning optimize...")
        optimized = shard.optimize()
        print(f"  Optimized: {optimized}")
        assert optimized, "Expected optimize() to rebuild the vacuum candidate segment"

        info_after = shard.info()
        count_after = shard.count(CountRequest(exact=True))
        print("\nAfter optimize:")
        print(f"  {info_after}")
        print(f"  Exact point count: {count_after}")

        assert count_after == count_before, (
            f"Point count changed after optimize: {count_before} -> {count_after}"
        )
        print(f"\nPoint count preserved after optimize: {count_after}")

        print("\nTesting search after optimize...")
        results = shard.search(
            SearchRequest(
                query=Query.Nearest(build_vector(SEARCH_POINT_ID)),
                limit=5,
                with_payload=True,
            )
        )
        print(f"  Search returned {len(results)} results")
        for point in results:
            print(f"    {point}")

        assert any(point.id == SEARCH_POINT_ID for point in results), (
            f"Expected point {SEARCH_POINT_ID} in the search results"
        )

        print("\nRunning optimize again (should be no-op)...")
        optimized_again = shard.optimize()
        print(f"  Optimized: {optimized_again}")
        assert not optimized_again, "Second optimize() run should be a no-op"

        print("\nAll checks passed!")
    finally:
        print(f"\nDropping temporary collection '{COLLECTION}'...")
        drop_collection()


if __name__ == "__main__":
    main()
