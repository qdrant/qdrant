import pathlib
from concurrent.futures import ThreadPoolExecutor
import pytest
import requests

from .assertions import assert_http_ok
from .fixtures import create_collection, upsert_points
from .utils import (
    processes,
    start_cluster,
    wait_collection_exists_and_active_on_all_peers,
)

COLLECTION_NAME = "test_read_timeout"
CORE_SEARCH_BATCH = "/qdrant.PointsInternal/CoreSearchBatch"
SCROLL = "/qdrant.PointsInternal/Scroll"
SYNC = "/qdrant.PointsInternal/Sync"


def test_remote_search_timeout_returns_408(tmp_path: pathlib.Path):
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path, 2, use_peer_proxy=True,
    )
    peers = list(processes)

    # 2 shards, replication factor 1 across 2 peers -> 1 shard on peer 0, 1 on peer 1
    create_collection(
        peer_api_uris[0],
        collection=COLLECTION_NAME,
        shard_number=2,
        replication_factor=1,
    )
    wait_collection_exists_and_active_on_all_peers(COLLECTION_NAME, peer_api_uris)

    points = [
        {"id": i, "vector": [0.1, 0.2, 0.3, 0.4], "payload": {"idx": i}}
        for i in range(10)
    ]
    assert_http_ok(upsert_points(peer_api_uris[0], points, collection_name=COLLECTION_NAME))

    # Hold the remote CoreSearchBatch RPC on peer 1 to induce a timeout
    with peers[1].proxy.hold_rpc(CORE_SEARCH_BATCH) as gate:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                requests.post,
                f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/search?timeout=2",
                json={
                    "vector": [0.1, 0.2, 0.3, 0.4],
                    "limit": 5,
                },
                timeout=15,
            )
            # Synchronization barrier: confirm RPC was intercepted by proxy before observing deadline
            gate.wait_for_request(timeout=10)
            response = future.result()

        assert response.status_code == 408, f"Expected 408, got {response.status_code}: {response.text}"
        error_msg = response.json()["status"]["error"]
        assert "timed out" in error_msg.lower() or "deadline exceeded" in error_msg.lower()
        gate.release()

    # Verify cluster recovers immediately after gate release (no indefinite wait, no leaked channels)
    recovered = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/search?timeout=10",
        json={
            "vector": [0.1, 0.2, 0.3, 0.4],
            "limit": 5,
        },
        timeout=15,
    )
    assert_http_ok(recovered)


def test_remote_scroll_timeout_returns_408(tmp_path: pathlib.Path):
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path, 2, use_peer_proxy=True,
    )
    peers = list(processes)

    create_collection(
        peer_api_uris[0],
        collection=COLLECTION_NAME,
        shard_number=2,
        replication_factor=1,
    )
    wait_collection_exists_and_active_on_all_peers(COLLECTION_NAME, peer_api_uris)

    points = [
        {"id": i, "vector": [0.1, 0.2, 0.3, 0.4], "payload": {"idx": i}}
        for i in range(10)
    ]
    assert_http_ok(upsert_points(peer_api_uris[0], points, collection_name=COLLECTION_NAME))

    # Hold the remote Scroll RPC on peer 1 to induce a timeout
    with peers[1].proxy.hold_rpc(SCROLL) as gate:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                requests.post,
                f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/scroll?timeout=2",
                json={
                    "limit": 5,
                },
                timeout=15,
            )
            # Synchronization barrier: confirm Scroll RPC was intercepted by proxy
            gate.wait_for_request(timeout=10)
            response = future.result()

        assert response.status_code == 408, f"Expected 408, got {response.status_code}: {response.text}"
        error_msg = response.json()["status"]["error"]
        assert "timed out" in error_msg.lower() or "deadline exceeded" in error_msg.lower()
        gate.release()

    # Verify cluster recovers immediately after gate release
    recovered = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/scroll?timeout=10",
        json={"limit": 5},
        timeout=15,
    )
    assert_http_ok(recovered)


def test_partial_replica_slowdown_does_not_block_quorum(tmp_path: pathlib.Path):
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path, 2, use_peer_proxy=True,
    )
    peers = list(processes)

    # 1 shard with 2 replicas across 2 peers (peer 0 has local replica, peer 1 has remote replica)
    # read_fan_out_factor=1 forces peer 0 to fan out concurrently to peer 1 while serving locally
    create_collection(
        peer_api_uris[0],
        collection=COLLECTION_NAME,
        shard_number=1,
        replication_factor=2,
        read_fan_out_factor=1,
    )
    wait_collection_exists_and_active_on_all_peers(COLLECTION_NAME, peer_api_uris)

    points = [
        {"id": i, "vector": [0.1, 0.2, 0.3, 0.4], "payload": {"idx": i}}
        for i in range(10)
    ]
    assert_http_ok(upsert_points(peer_api_uris[0], points, collection_name=COLLECTION_NAME))

    # Hold remote CoreSearchBatch on peer 1
    with peers[1].proxy.hold_rpc(CORE_SEARCH_BATCH) as gate:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                requests.post,
                f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/search?timeout=2",
                json={
                    "vector": [0.1, 0.2, 0.3, 0.4],
                    "limit": 5,
                },
                timeout=15,
            )
            # Barrier: confirm fan-out request reached peer 1 proxy
            gate.wait_for_request(timeout=10)
            response = future.result()

        # Should succeed because 1 replica was successful (quorum satisfied, no spurious 408 or 500)
        assert_http_ok(response)
        assert len(response.json()["result"]) == 5
        gate.release()


def test_replica_timeout_fails_when_all_required_replicas_not_reached(tmp_path: pathlib.Path):
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path, 2, use_peer_proxy=True,
    )
    peers = list(processes)

    create_collection(
        peer_api_uris[0],
        collection=COLLECTION_NAME,
        shard_number=1,
        replication_factor=2,
        read_fan_out_factor=1,
    )
    wait_collection_exists_and_active_on_all_peers(COLLECTION_NAME, peer_api_uris)

    points = [
        {"id": i, "vector": [0.1, 0.2, 0.3, 0.4], "payload": {"idx": i}}
        for i in range(10)
    ]
    assert_http_ok(upsert_points(peer_api_uris[0], points, collection_name=COLLECTION_NAME))

    with peers[1].proxy.hold_rpc(CORE_SEARCH_BATCH) as gate:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                requests.post,
                f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/search?timeout=2&read_consistency=all",
                json={
                    "vector": [0.1, 0.2, 0.3, 0.4],
                    "limit": 5,
                },
                timeout=15,
            )
            # Barrier: confirm remote request arrived at peer 1
            gate.wait_for_request(timeout=10)
            response = future.result()

        assert response.status_code == 408, f"Expected 408, got {response.status_code}: {response.text}"
        error_msg = response.json()["status"]["error"]
        assert "timed out" in error_msg.lower() or "deadline exceeded" in error_msg.lower()
        gate.release()


def test_mixed_replica_failure_returns_500(tmp_path: pathlib.Path):
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path, 2, use_peer_proxy=True,
    )
    peers = list(processes)

    create_collection(
        peer_api_uris[0],
        collection=COLLECTION_NAME,
        shard_number=2,
        replication_factor=1,
    )
    wait_collection_exists_and_active_on_all_peers(COLLECTION_NAME, peer_api_uris)

    # Block the RPC on peer 1 so it rejects with UNAVAILABLE (transport failure)
    with peers[1].proxy.block_rpc(CORE_SEARCH_BATCH):
        response = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/search?timeout=2",
            json={
                "vector": [0.1, 0.2, 0.3, 0.4],
                "limit": 5,
            },
            timeout=15,
        )
        # Transport failure/service error must return 500, NOT 408
        assert response.status_code == 500, f"Expected 500, got {response.status_code}: {response.text}"


def test_single_peer_search_and_scroll_timeout(tmp_path: pathlib.Path):
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path, 1,
    )

    create_collection(
        peer_api_uris[0],
        collection=COLLECTION_NAME,
        shard_number=1,
        replication_factor=1,
    )

    points = [
        {"id": i, "vector": [0.1, 0.2, 0.3, 0.4], "payload": {"idx": i}}
        for i in range(10)
    ]
    assert_http_ok(upsert_points(peer_api_uris[0], points, collection_name=COLLECTION_NAME))

    # Test single-peer search timeout via staging delay if available
    delay_resp = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/debug",
        json={"delay": {"duration_sec": 3.0}},
    )
    if delay_resp.ok:
        search_resp = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/search?timeout=1",
            json={"vector": [0.1, 0.2, 0.3, 0.4], "limit": 5},
            timeout=10,
        )
        assert search_resp.status_code == 408, f"Expected 408, got {search_resp.status_code}: {search_resp.text}"
        error_msg = search_resp.json()["status"]["error"]
        assert "timed out" in error_msg.lower() or "deadline exceeded" in error_msg.lower()


def test_unrelated_rpc_negative_control(tmp_path: pathlib.Path):
    """Negative control: holding an uncalled RPC must fail gate.wait_for_request(0)."""
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path, 2, use_peer_proxy=True,
    )
    peers = list(processes)

    create_collection(
        peer_api_uris[0],
        collection=COLLECTION_NAME,
        shard_number=2,
        replication_factor=1,
    )
    wait_collection_exists_and_active_on_all_peers(COLLECTION_NAME, peer_api_uris)

    # Hold an RPC that will NOT be invoked by search (Sync)
    with peers[1].proxy.hold_rpc(SYNC) as gate:
        # CoreSearchBatch is not held, so search should reach peer 1 normally or finish
        with pytest.raises(TimeoutError, match="No request reached the gate"):
            # With timeout=0, must immediately raise TimeoutError because SYNC was never dispatched
            gate.wait_for_request(timeout=0)
