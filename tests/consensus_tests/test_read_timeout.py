import pathlib
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
        response = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/search?timeout=1",
            json={
                "vector": [0.1, 0.2, 0.3, 0.4],
                "limit": 5,
            },
            timeout=10,
        )
        assert response.status_code == 408, f"Expected 408, got {response.status_code}: {response.text}"
        error_msg = response.json()["status"]["error"]
        assert "timed out" in error_msg.lower() or "deadline exceeded" in error_msg.lower()
        gate.release()


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
        response = requests.post(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/scroll?timeout=1",
            json={
                "limit": 5,
            },
            timeout=10,
        )
        assert response.status_code == 408, f"Expected 408, got {response.status_code}: {response.text}"
        error_msg = response.json()["status"]["error"]
        assert "timed out" in error_msg.lower() or "deadline exceeded" in error_msg.lower()
        gate.release()
