import pathlib

from .fixtures import upsert_random_points, create_collection
from .utils import *
from .assertions import assert_http_ok

N_PEERS = 3
N_SHARDS = 3
N_REPLICA = 2

NUM_POINTS = 500


def create_collection_from(
        peer_url,
        from_collection_name,
        collection="test_collection",
        shard_number=1,
        replication_factor=1,
        timeout=10
):
    # Create collection in first peer
    r_batch = requests.put(
        f"{peer_url}/collections/{collection}?timeout={timeout}", json={
            "vectors": {
                "size": 4,
                "distance": "Dot"
            },
            "sparse_vectors": {
                "sparse-text": {}
            },
            "shard_number": shard_number,
            "replication_factor": replication_factor,
            "init_from": {
                "collection": from_collection_name
            }
        })
    assert_http_ok(r_batch)


def create_payload_index(peer_url, collection_name):
    r_batch = requests.put(
        f"{peer_url}/collections/{collection_name}/index?wait=false", json={
            "field_name": "city",
            "field_schema": "keyword"
        })
    assert_http_ok(r_batch)


def test_init_collection_from(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA)
    wait_collection_exists_and_active_on_all_peers(
        collection_name="test_collection",
        peer_api_uris=peer_api_uris
    )

    create_payload_index(peer_api_uris[0], "test_collection")
    upsert_random_points(peer_api_uris[0], collection_name="test_collection", num=NUM_POINTS)

    create_collection_from(peer_api_uris[1], from_collection_name="test_collection", collection="test_collection_2")

    wait_collection_exists_and_active_on_all_peers(
        collection_name="test_collection_2",
        peer_api_uris=peer_api_uris
    )

    wait_collection_vectors_count(
        peer_api_uris[0],
        "test_collection_2",
        NUM_POINTS * 2  # 1 dense + 1 sparse per point
    )

    collection_info = get_collection_info(peer_api_uris[0], "test_collection_2")

    assert "city" in collection_info["payload_schema"]
