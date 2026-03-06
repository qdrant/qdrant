#!/usr/bin/env python3
"""
Test edge optimize by downloading a snapshot from a running Qdrant instance.

Requires a running Qdrant on localhost:6333 with a 'bench_dense_384' collection.
"""

import os
import shutil

import requests

from qdrant_edge import *

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
COLLECTION = "bench_dense_384"
QDRANT_URL = "http://localhost:6333"
SNAPSHOT_DIR = os.path.join(DATA_DIR, "snapshots")
SHARD_DIR = os.path.join(DATA_DIR, "optimize_test_shard")


def create_shard_snapshot() -> str:
    """Create a shard-level snapshot on the running Qdrant and download it."""
    print(f"Creating shard snapshot for collection '{COLLECTION}' shard 0...")
    resp = requests.post(
        f"{QDRANT_URL}/collections/{COLLECTION}/shards/0/snapshots", timeout=60
    )
    resp.raise_for_status()
    snapshot_name = resp.json()["result"]["name"]
    print(f"Snapshot created: {snapshot_name}")

    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    local_path = os.path.join(SNAPSHOT_DIR, snapshot_name)

    if not os.path.exists(local_path):
        print(f"Downloading snapshot...")
        dl = requests.get(
            f"{QDRANT_URL}/collections/{COLLECTION}/shards/0/snapshots/{snapshot_name}",
            stream=True,
            timeout=60,
        )
        dl.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in dl.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded to {local_path}")
    return local_path


def main():
    # 1. Create and download shard snapshot
    snapshot_path = create_shard_snapshot()

    # 2. Unpack into shard directory
    if os.path.exists(SHARD_DIR):
        shutil.rmtree(SHARD_DIR)

    print(f"\nUnpacking snapshot to {SHARD_DIR}...")
    EdgeShard.unpack_snapshot(snapshot_path, SHARD_DIR)

    # 3. Load edge shard
    print("Loading edge shard...")
    shard = EdgeShard(SHARD_DIR)

    info_before = shard.info()
    print(f"\nBefore optimize:")
    print(f"  {info_before}")

    # 4. Count points
    count_before = shard.count(CountRequest(exact=True))
    print(f"  Exact point count: {count_before}")

    # 5. Run optimize
    print("\nRunning optimize...")
    optimized = shard.optimize()
    print(f"  Optimized: {optimized}")

    info_after = shard.info()
    print(f"\nAfter optimize:")
    print(f"  {info_after}")

    count_after = shard.count(CountRequest(exact=True))
    print(f"  Exact point count: {count_after}")

    # 6. Verify data integrity
    assert count_after == count_before, (
        f"Point count changed after optimize: {count_before} -> {count_after}"
    )
    print(f"\nPoint count preserved: {count_after}")

    # 7. Test search still works
    print("\nTesting search after optimize...")
    results = shard.search(
        SearchRequest(
            query=Query.Nearest([0.1] * 384),
            limit=5,
            with_payload=True,
        )
    )
    print(f"  Search returned {len(results)} results")
    for p in results:
        print(f"    {p}")

    # 8. Run optimize again - should be no-op
    print("\nRunning optimize again (should be no-op)...")
    optimized_again = shard.optimize()
    print(f"  Optimized: {optimized_again}")

    print("\nAll checks passed!")


if __name__ == "__main__":
    main()
