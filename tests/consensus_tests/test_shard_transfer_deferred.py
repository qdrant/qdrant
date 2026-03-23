import pathlib
import random
import time

import pytest
import requests

from .assertions import assert_http_ok
from .utils import *

N_PEERS = 2
N_SHARDS = 1
N_REPLICA = 1
COLLECTION_NAME = "test_collection"
VECTOR_DIM = 256
INDEXING_THRESHOLD_KB = 100


def create_deferred_collection(peer_url):
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
            "shard_number": N_SHARDS,
            "replication_factor": N_REPLICA,
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


def upsert_points(peer_url, start_id, count, wait=True, client_timeout=None):
    points = make_points(start_id, count)
    params = f"?wait={'true' if wait else 'false'}"
    r = requests.put(
        f"{peer_url}/collections/{COLLECTION_NAME}/points{params}",
        json={"points": points},
        timeout=client_timeout,
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


def trigger_upsert_wait_true(source_uri, total_points):
    """Upsert a point with wait=true, retrying until the optimizer resolves deferred points.

    The config change (enabling optimizers) propagates through Raft and restarts
    the update workers, cancelling the old worker's deferred wait loop. Retries
    handle the transition period where the old worker may return wait_timeout.
    """
    for attempt in range(10):
        try:
            r = requests.put(
                f"{source_uri}/collections/{COLLECTION_NAME}/points?wait=true",
                json={"points": make_points(total_points + 1, 1)},
                timeout=30,
            )
        except requests.exceptions.ReadTimeout:
            # Server still processing, retry — the update is durably applied regardless
            time.sleep(1)
            continue
        assert_http_ok(r)
        result_status = r.json().get("result", {}).get("status")
        if result_status == "completed":
            return
        # "wait_timeout" = update applied but deferred visibility not confirmed (old worker cancelled)
        assert result_status == "wait_timeout", (
            f"Unexpected result status '{result_status}' on attempt {attempt}: {r.text}"
        )
        time.sleep(1)
    raise AssertionError("wait=true upsert did not succeed after retries")


@pytest.mark.parametrize("transfer_method", ["snapshot", "stream_records"])
def test_shard_transfer_includes_deferred_points(tmp_path: pathlib.Path, transfer_method: str):
    """Shard transfer must include deferred points regardless of transfer method.

    Deferred points live in an appendable segment but have an internal offset
    beyond the indexing threshold, making them invisible to reads until the
    segment is optimized.  A shard transfer must capture these deferred points
    so they appear on the target node after optimization.
    """
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_deferred_collection(peer_api_uris[0])
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )

    total_points = 500

    # Find source (peer with the local shard) and target (peer without)
    source_idx, target_idx = None, None
    for i, uri in enumerate(peer_api_uris):
        info = get_collection_cluster_info(uri, COLLECTION_NAME)
        if len(info["local_shards"]) > 0:
            source_idx = i
        else:
            target_idx = i
    assert source_idx is not None, "No peer has a local shard"
    assert target_idx is not None, "All peers already have local shards"

    source_uri = peer_api_uris[source_idx]
    target_uri = peer_api_uris[target_idx]

    # Insert points with wait=False so points land in the deferred section
    # without triggering optimization
    upsert_points(source_uri, start_id=1, count=total_points, wait=False)
    time.sleep(3)

    # Verify that deferred points exist: only a fraction should be visible
    source_visible = scroll_all(source_uri)
    visible_count = len(source_visible)
    assert visible_count > 0, "Some points should be visible (within threshold)"
    assert visible_count < total_points, (
        f"Not all points should be visible (most are deferred), "
        f"got {visible_count}/{total_points}"
    )

    # With optimizers disabled, wait=true hangs because deferred points
    # can never be resolved without optimizers running. The client times out.
    try:
        requests.put(
            f"{source_uri}/collections/{COLLECTION_NAME}/points?wait=true",
            json={"points": make_points(total_points + 1, 1)},
            timeout=5,
        )
        raise AssertionError("Expected timeout for wait=true with optimizers disabled")
    except requests.exceptions.ReadTimeout:
        pass  # Expected: server blocks forever, client times out

    # Enable optimizers to resolve deferred points.
    # This must happen before the transfer because stream_records uses wait=true
    # internally on the last batch, which would hang with disabled optimizers.
    update_collection_config(source_uri, {
        "optimizers_config": {"max_optimization_threads": "auto"},
    })

    src_info = get_collection_cluster_info(source_uri, COLLECTION_NAME)
    dst_info = get_collection_cluster_info(target_uri, COLLECTION_NAME)

    from_peer_id = src_info["peer_id"]
    to_peer_id = dst_info["peer_id"]
    shard_id = src_info["local_shards"][0]["shard_id"]

    r = requests.post(
        f"{source_uri}/collections/{COLLECTION_NAME}/cluster",
        json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": from_peer_id,
                "to_peer_id": to_peer_id,
                "method": transfer_method,
            }
        },
    )
    assert_http_ok(r)

    # Wait for the transfer to complete
    wait_for_collection_shard_transfers_count(source_uri, COLLECTION_NAME, 0)

    # Verify the target now has the shard
    dst_info_after = get_collection_cluster_info(target_uri, COLLECTION_NAME)
    assert len(dst_info_after["local_shards"]) == 1, (
        f"Target should have 1 local shard, got {len(dst_info_after['local_shards'])}"
    )

    # Verify deferred points were transferred: target should have hidden points.
    # For snapshot, the target gets a raw segment copy — deferred state is preserved exactly.
    # For stream_records, points are re-inserted on the target and may not be deferred.
    target_visible = scroll_all(target_uri)
    target_visible_count = len(target_visible)
    if transfer_method == "snapshot":
        assert target_visible_count == visible_count, (
            f"Snapshot transfer should preserve deferred state: "
            f"target visible={target_visible_count}, source visible={visible_count}"
        )
    else:
        assert target_visible_count >= visible_count, (
            f"Target should have at least as many visible points as source: "
            f"target={target_visible_count}, source={visible_count}"
        )

    # Trigger optimization with wait=true to ensure deferred points are resolved
    trigger_upsert_wait_true(source_uri, total_points)

    # Wait for optimization to complete on both peers
    wait_collection_green(source_uri, COLLECTION_NAME)
    wait_collection_green(target_uri, COLLECTION_NAME)

    # After optimization ALL points (including previously deferred) must be visible
    expected_total = total_points + 1

    source_count = exact_count(source_uri)
    target_count = exact_count(target_uri)

    assert source_count == expected_total, (
        f"Source should have {expected_total} points after optimization, "
        f"got {source_count}"
    )
    assert target_count == expected_total, (
        f"Target should have {expected_total} points after optimization, "
        f"got {target_count}"
    )

    # Cross-check: scroll both peers and verify identical point sets
    source_all = scroll_all(source_uri)
    target_all = scroll_all(target_uri)

    source_ids = {p["id"] for p in source_all}
    target_ids = {p["id"] for p in target_all}

    assert len(source_ids) == expected_total
    assert source_ids == target_ids, (
        "Source and target should have identical point sets after optimization"
    )


def test_shard_wal_delta_transfer_includes_deferred_points(tmp_path: pathlib.Path):
    """WAL delta transfer must include deferred points in the diff.

    wal_delta requires both peers to already have the shard. This test:
    1. Creates a collection with replication_factor=1 (only peer0 has shard)
    2. Inserts a small initial batch (within threshold, no deferred points)
    3. Replicates to peer1 via snapshot (both in sync)
    4. Inserts more points on peer0 that become deferred (beyond threshold)
    5. Uses wal_delta to sync the diff — the delta contains deferred points
    6. Verifies both peers see all points after optimization
    """
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_deferred_collection(peer_api_uris[0])
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )

    peer0_uri = peer_api_uris[0]
    peer1_uri = peer_api_uris[1]

    # Insert a small initial batch within the indexing threshold (no deferred points)
    initial_points = 50
    upsert_points(peer0_uri, start_id=1, count=initial_points, wait=False)
    time.sleep(2)

    # Verify all initial points are visible (within threshold, none deferred)
    visible = scroll_all(peer0_uri)
    assert len(visible) == initial_points, (
        f"All initial points should be visible, got {len(visible)}/{initial_points}"
    )

    # Find source and target peers
    source_idx, target_idx = None, None
    for i, uri in enumerate(peer_api_uris):
        info = get_collection_cluster_info(uri, COLLECTION_NAME)
        if len(info["local_shards"]) > 0:
            source_idx = i
        else:
            target_idx = i
    assert source_idx is not None, "No peer has a local shard"
    assert target_idx is not None, "All peers already have local shards"

    source_uri = peer_api_uris[source_idx]
    target_uri = peer_api_uris[target_idx]

    src_info = get_collection_cluster_info(source_uri, COLLECTION_NAME)
    dst_info = get_collection_cluster_info(target_uri, COLLECTION_NAME)
    from_peer_id = src_info["peer_id"]
    to_peer_id = dst_info["peer_id"]
    shard_id = src_info["local_shards"][0]["shard_id"]

    # Initial replication via snapshot so both peers have the shard
    r = requests.post(
        f"{source_uri}/collections/{COLLECTION_NAME}/cluster",
        json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": from_peer_id,
                "to_peer_id": to_peer_id,
                "method": "snapshot",
            }
        },
    )
    assert_http_ok(r)
    wait_for_collection_shard_transfers_count(source_uri, COLLECTION_NAME, 0)

    # Both peers now have the shard with the initial points.
    # Insert more points that push beyond the indexing threshold, creating deferred points.
    deferred_points = 500
    upsert_points(source_uri, start_id=initial_points + 1, count=deferred_points, wait=False)
    time.sleep(3)

    # Verify deferred points exist on source
    source_visible = scroll_all(source_uri)
    total_inserted = initial_points + deferred_points
    assert len(source_visible) < total_inserted, (
        f"Not all points should be visible (some are deferred), "
        f"got {len(source_visible)}/{total_inserted}"
    )

    # Enable optimizers before wal_delta transfer (internal writes use wait=true
    # which would hang with disabled optimizers)
    update_collection_config(source_uri, {
        "optimizers_config": {"max_optimization_threads": "auto"},
    })

    # Use wal_delta to sync the deferred points from source to target
    r = requests.post(
        f"{source_uri}/collections/{COLLECTION_NAME}/cluster",
        json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": from_peer_id,
                "to_peer_id": to_peer_id,
                "method": "wal_delta",
            }
        },
    )
    assert_http_ok(r)
    wait_for_collection_shard_transfers_count(source_uri, COLLECTION_NAME, 0)

    # Trigger optimization with wait=true
    trigger_upsert_wait_true(source_uri, total_inserted)

    # Wait for optimization to complete on both peers
    wait_collection_green(source_uri, COLLECTION_NAME)
    wait_collection_green(target_uri, COLLECTION_NAME)

    # After optimization ALL points must be visible on both peers
    expected_total = total_inserted + 1  # +1 from trigger upsert

    source_count = exact_count(source_uri)
    target_count = exact_count(target_uri)

    assert source_count == expected_total, (
        f"Source should have {expected_total} points, got {source_count}"
    )
    assert target_count == expected_total, (
        f"Target should have {expected_total} points, got {target_count}"
    )

    # Cross-check: both peers have identical point sets
    source_all = scroll_all(source_uri)
    target_all = scroll_all(target_uri)

    source_ids = {p["id"] for p in source_all}
    target_ids = {p["id"] for p in target_all}

    assert len(source_ids) == expected_total
    assert source_ids == target_ids, (
        "Both peers should have identical point sets after optimization"
    )

    # Cross-check: both peers have identical point sets
    all_0 = scroll_all(peer0_uri)
    all_1 = scroll_all(peer1_uri)

    ids_0 = {p["id"] for p in all_0}
    ids_1 = {p["id"] for p in all_1}

    assert len(ids_0) == expected_total
    assert ids_0 == ids_1, (
        "Both peers should have identical point sets after optimization"
    )
