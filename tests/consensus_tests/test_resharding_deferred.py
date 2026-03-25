import pathlib
import random
import time
from typing import Literal

import pytest
import requests

from .assertions import assert_http_ok
from .utils import *

N_PEERS = 3
N_SHARDS = 3
N_REPLICA = 1
COLLECTION_NAME = "test_resharding_deferred"
VECTOR_DIM = 256
INDEXING_THRESHOLD_KB = 100


def create_deferred_collection(peer_url, shard_number=N_SHARDS, replication_factor=N_REPLICA):
    """Create collection configured to produce deferred points.

    With 256-dim float32 vectors each point is ~1 KB of vector data.
    indexing_threshold=100 KB means ~100 points are visible before deferred
    kicks in.  prevent_unoptimized must be enabled *before* any appendable
    segments are created so that deferred_internal_id is tracked from the start.
    max_optimization_threads=0 disables automatic optimization so deferred
    points stay deferred until we explicitly re-enable optimizers.
    """
    r = requests.put(
        f"{peer_url}/collections/{COLLECTION_NAME}?timeout=10",
        json={
            "vectors": {"size": VECTOR_DIM, "distance": "Cosine"},
            "shard_number": shard_number,
            "replication_factor": replication_factor,
            "optimizers_config": {
                "indexing_threshold": INDEXING_THRESHOLD_KB,
                "prevent_unoptimized": True,
                "max_optimization_threads": 0,
            },
        },
    )
    assert_http_ok(r)


def make_points(start_id, count):
    random.seed(start_id)
    points = []
    for i in range(count):
        pid = start_id + i
        vector = [random.random() for _ in range(VECTOR_DIM)]
        points.append({
            "id": pid,
            "vector": vector,
            "payload": {"value": pid},
        })
    return points


def upsert_points(peer_url, start_id, count, wait=True):
    points = make_points(start_id, count)
    params = f"?wait={'true' if wait else 'false'}"
    r = requests.put(
        f"{peer_url}/collections/{COLLECTION_NAME}/points{params}",
        json={"points": points},
    )
    assert_http_ok(r)


def exact_count(peer_url):
    r = requests.post(
        f"{peer_url}/collections/{COLLECTION_NAME}/points/count",
        json={"exact": True},
    )
    assert_http_ok(r)
    return r.json()["result"]["count"]


def scroll_all(peer_url):
    all_points = []
    offset = None
    while True:
        body = {"limit": 100, "with_vector": False, "with_payload": False}
        if offset is not None:
            body["offset"] = offset
        r = requests.post(
            f"{peer_url}/collections/{COLLECTION_NAME}/points/scroll",
            json=body,
        )
        assert_http_ok(r)
        result = r.json()["result"]
        all_points.extend(result["points"])
        offset = result.get("next_page_offset")
        if offset is None:
            break
    return all_points


def update_collection_config(peer_url, config):
    r = requests.patch(
        f"{peer_url}/collections/{COLLECTION_NAME}",
        json=config,
    )
    assert_http_ok(r)


def start_resharding_op(peer_url, direction="up", peer_id=None):
    return requests.post(f"{peer_url}/collections/{COLLECTION_NAME}/cluster", json={
        "start_resharding": {
            "direction": direction,
            "peer_id": peer_id,
        }
    })


def commit_read_hashring(peer_url):
    return requests.post(f"{peer_url}/collections/{COLLECTION_NAME}/cluster", json={
        "commit_read_hash_ring": {}
    })


def commit_write_hashring(peer_url):
    return requests.post(f"{peer_url}/collections/{COLLECTION_NAME}/cluster", json={
        "commit_write_hash_ring": {}
    })


def finish_resharding(peer_url):
    return requests.post(f"{peer_url}/collections/{COLLECTION_NAME}/cluster", json={
        "finish_resharding": {}
    })


def migrate_points(peer_url, from_peer_id, from_shard_id, to_peer_id, to_shard_id):
    """Migrate resharding points from one shard to another using resharding_stream_records."""
    resp = requests.post(f"{peer_url}/collections/{COLLECTION_NAME}/cluster", json={
        "replicate_shard": {
            "from_peer_id": from_peer_id,
            "to_peer_id": to_peer_id,
            "shard_id": from_shard_id,
            "to_shard_id": to_shard_id,
            "method": "resharding_stream_records",
        }
    })
    assert_http_ok(resp)

    time.sleep(1)
    wait_for_collection_shard_transfers_count(peer_url, COLLECTION_NAME, 0)


def find_replica(shard_id, info, peer_uris, peer_ids):
    for replica in all_replicas(info):
        if replica["shard_id"] == shard_id:
            peer_id = replica["peer_id"]
            peer_uri = peer_uris[peer_ids.index(peer_id)]
            return (peer_id, peer_uri)
    raise Exception(f"replica of shard {shard_id} not found: {info}")


def all_replicas(info):
    for local in info["local_shards"]:
        local["peer_id"] = info["peer_id"]
        yield local
    for remote in info["remote_shards"]:
        yield remote


def count_local_points(peer_uri, shard_id, filter_shard_id=None, exact=True):
    resp = requests.post(
        f"{peer_uri}/collections/{COLLECTION_NAME}/shards/{shard_id}/points/count",
        json={
            "exact": exact,
            "hash_ring_filter": None if filter_shard_id is None else {
                "expected_shard_id": filter_shard_id,
            },
        },
    )
    assert_http_ok(resp)
    return resp.json()["result"]["count"]


@pytest.mark.parametrize("direction", ["up", "down"])
def test_resharding_transfer_deferred_points(tmp_path: pathlib.Path, direction: Literal["up", "down"]):
    """Resharding must preserve deferred points.

    Deferred points live in appendable segments but have an internal offset
    beyond the indexing threshold, making them invisible to reads until the
    segment is optimized. The resharding_stream_records transfer uses
    DeferredBehavior::IncludeAll to read them. The optimizer runs concurrently
    (required for wait=true on the last migration batch) while the migration
    reads from the same segments via the forward proxy.

    For "down", the removed shard's data is permanently deleted — any deferred
    points not transferred during migration are irrecoverably lost.

    For "up", deferred points on source shards must be included when migrating
    the subset of points that belong to the new shard.

    Steps:
    1. Creates a collection with prevent_unoptimized + disabled optimizers
    2. Upserts enough points to create deferred points in every shard
    3. Verifies deferred points exist (only a fraction visible)
    4. Starts resharding while optimizers are still disabled
    5. Enables optimizers right before migration (required for wait=true)
    6. Migrates points between source and target shards
    7. Completes resharding (commit hash rings + finish)
    8. Verifies ALL points are visible after resharding
    """
    assert_project_root()

    peer_api_uris, _, _ = start_cluster(tmp_path, N_PEERS)

    peer_ids = []
    for uri in peer_api_uris:
        peer_ids.append(get_cluster_info(uri)["peer_id"])

    create_deferred_collection(peer_api_uris[0])
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )

    # Upsert enough points to push beyond the indexing threshold on each shard.
    # With 3 shards, ~500 points per shard, well beyond the ~100 point threshold.
    total_points = 1500
    upsert_points(peer_api_uris[0], start_id=1, count=total_points, wait=False)
    time.sleep(3)

    # Verify deferred points exist: only a fraction should be visible
    visible = scroll_all(peer_api_uris[0])
    visible_count = len(visible)
    assert visible_count > 0, "Some points should be visible (within threshold)"
    assert visible_count < total_points, (
        f"Not all points should be visible (most are deferred), "
        f"got {visible_count}/{total_points}"
    )

    # Start resharding while optimizers are still disabled so deferred
    # points remain unresolved during setup.
    wait_for_all_peers_versions(peer_api_uris)

    resp = start_resharding_op(peer_api_uris[0], direction=direction)
    assert_http_ok(resp)
    wait_for_collection_resharding_operations_count(peer_api_uris[0], COLLECTION_NAME, 1)

    # Get cluster info to find replicas
    info = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)

    # Select target shard:
    # - "up": new shard 3 is added
    # - "down": existing shard 2 is being removed
    target_shard_id = N_SHARDS if direction == "up" else N_SHARDS - 1
    target_peer_id, _ = find_replica(target_shard_id, info, peer_api_uris, peer_ids)

    # Record pre-migration point counts per shard (includes deferred points
    # via the shard-local count which also uses DeferredBehavior::Exclude,
    # so this captures only the visible portion — but that's fine as a baseline
    # to verify the surviving shards grew after migration).
    pre_migration_counts = {}
    for shard_id in range(target_shard_id):
        peer_id, peer_uri = find_replica(shard_id, info, peer_api_uris, peer_ids)
        pre_migration_counts[shard_id] = count_local_points(peer_uri, shard_id, exact=True)

    # Enable optimizers right before migration. resharding_stream_records
    # uses wait=true internally on the last batch, which would hang with
    # disabled optimizers when deferred points are present.
    update_collection_config(peer_api_uris[0], {
        "optimizers_config": {"max_optimization_threads": "auto"},
    })

    # Migrate points between source and target shards
    for shard_id in range(target_shard_id):
        peer_id, _ = find_replica(shard_id, info, peer_api_uris, peer_ids)

        if direction == "up":
            # Up: migrate from each source shard to the new target shard
            migrate_points(peer_api_uris[0], peer_id, shard_id, target_peer_id, target_shard_id)
        else:
            # Down: migrate from the target shard (being removed) to each surviving shard
            migrate_points(peer_api_uris[0], target_peer_id, target_shard_id, peer_id, shard_id)

    if direction == "up":
        # Activate the new shard's replica before committing hash rings.
        # Without this, the replica stays in "Resharding" state and can't serve reads.
        resp = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster",
            json={
                "finish_migrating_points": {
                    "peer_id": target_peer_id,
                    "shard_id": target_shard_id,
                }
            },
        )
        assert_http_ok(resp)
        time.sleep(1)

        # Clean up source shards: delete points that were migrated to the new
        # shard and no longer belong on the source under the new hash ring.
        for shard_id in range(target_shard_id):
            peer_id, peer_uri = find_replica(shard_id, info, peer_api_uris, peer_ids)
            resp = requests.post(
                f"{peer_uri}/collections/{COLLECTION_NAME}/shards/{shard_id}/cleanup?wait=true",
            )
            assert_http_ok(resp)

    # Complete resharding: commit hash rings and finish.
    resp = commit_read_hashring(peer_api_uris[0])
    assert_http_ok(resp)

    resp = commit_write_hashring(peer_api_uris[0])
    assert_http_ok(resp)

    resp = finish_resharding(peer_api_uris[0])
    assert_http_ok(resp)

    wait_for_collection_resharding_operations_count(peer_api_uris[0], COLLECTION_NAME, 0)

    # Find peers that still host shards.
    # For "down" with replication_factor=1, the peer that only had shard 2
    # now has no shards and can't serve collection requests.
    # For "up", all peers still have shards (4 shards across 3 peers).
    peers_with_shards = []
    for uri in peer_api_uris:
        cluster_info = get_collection_cluster_info(uri, COLLECTION_NAME)
        if len(cluster_info["local_shards"]) > 0:
            peers_with_shards.append(uri)

    # Wait for optimization to complete on peers that have shards
    for uri in peers_with_shards:
        wait_collection_green(uri, COLLECTION_NAME)

    # After resharding + optimization, ALL points must be visible.
    # For "down": shard 2 removed, no duplicates.
    # For "up": cleanup deleted migrated points from source shards, no duplicates.
    # In both cases, collection-level exact_count should equal total_points.
    for uri in peers_with_shards:
        count = exact_count(uri)
        assert count == total_points, (
            f"Peer {uri} should have {total_points} points after resharding {direction}, got {count}"
        )

    if direction == "down":
        # Verify each surviving shard grew: it must have more points than
        # before migration, proving it received points from the removed shard.
        for shard_id in range(target_shard_id):
            peer_id, peer_uri = find_replica(shard_id, info, peer_api_uris, peer_ids)
            post_count = count_local_points(peer_uri, shard_id, exact=True)
            pre_count = pre_migration_counts[shard_id]
            assert post_count > pre_count, (
                f"Shard {shard_id} should have grown after migration "
                f"(pre={pre_count}, post={post_count})"
            )
    else:
        # Verify the new target shard received points.
        target_peer_uri = None
        for uri in peer_api_uris:
            cluster_info = get_collection_cluster_info(uri, COLLECTION_NAME)
            for shard in cluster_info["local_shards"]:
                if shard["shard_id"] == target_shard_id:
                    target_peer_uri = uri
                    break

        assert target_peer_uri is not None, "Target shard peer not found"

        target_count = count_local_points(target_peer_uri, target_shard_id, exact=True)
        assert target_count > 0, (
            "Target shard should have received migrated points"
        )

    # Cross-check: scroll all points and collect unique IDs.
    # For "up", scroll may return duplicates (same point on source + target),
    # but the set of unique IDs must equal all original IDs.
    scrolled = scroll_all(peers_with_shards[0])
    scrolled_ids = {p["id"] for p in scrolled}
    expected_ids = set(range(1, total_points + 1))
    assert scrolled_ids == expected_ids, (
        f"All original point IDs should be present after resharding. "
        f"Missing: {expected_ids - scrolled_ids}"
    )
