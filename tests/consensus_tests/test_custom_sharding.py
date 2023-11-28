import pathlib

from .fixtures import upsert_random_points, create_collection
from .utils import *

N_PEERS = 3
N_SHARDS = 3
N_REPLICAS = 1

COLLECTION_NAME = "test_collection"


def create_collection_with_custom_sharding(
        peer_url,
        collection=COLLECTION_NAME,
        shard_number=1,
        replication_factor=1,
        write_consistency_factor=1,
        timeout=10
):
    # Create collection in peer_url
    r_batch = requests.put(
        f"{peer_url}/collections/{collection}?timeout={timeout}", json={
            "vectors": {
                "size": 4,
                "distance": "Dot"
            },
            "shard_number": shard_number,
            "replication_factor": replication_factor,
            "write_consistency_factor": write_consistency_factor,
            "sharding_method": "custom",
        })
    assert_http_ok(r_batch)


def create_shard(
        peer_url,
        collection,
        shard_key,
        shard_number=1,
        replication_factor=1,
        placement=None,
        timeout=10
):
    r_batch = requests.put(
        f"{peer_url}/collections/{collection}/shards?timeout={timeout}", json={
            "shard_key": shard_key,
            "shards_number": shard_number,
            "replication_factor": replication_factor,
            "placement": placement,
        })
    assert_http_ok(r_batch)


def test_shard_consistency(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection_with_custom_sharding(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICAS)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris)

    # Create shards
    create_shard(
        peer_api_uris[0],
        COLLECTION_NAME,
        shard_key="cats",
        shard_number=1,
        replication_factor=1
    )

    create_shard(
        peer_api_uris[0],
        COLLECTION_NAME,
        shard_key="dogs",
        shard_number=1,
        replication_factor=1
    )

    # Insert data

    # Create points in first peer's collection
    r = requests.put(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points?wait=true", json={
            "shard_key": "cats",
            "points": [
                {"id": 1, "vector": [0.29, 0.81, 0.75, 0.11], "payload": {"name": "Barsik"}},
                {"id": 2, "vector": [0.19, 0.11, 0.15, 0.21], "payload": {"name": "Murzik"}},
                {"id": 3, "vector": [0.99, 0.81, 0.75, 0.31], "payload": {"name": "Vaska"}},
                {"id": 4, "vector": [0.29, 0.01, 0.05, 0.91], "payload": {"name": "Chubais"}},
            ]
        })
    assert_http_ok(r)

    r = requests.put(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points?wait=true", json={
            "shard_key": "dogs",
            "points": [
                {"id": 5, "vector": [0.29, 0.81, 0.75, 0.11], "payload": {"name": "Sharik"}},
                {"id": 6, "vector": [0.19, 0.11, 0.15, 0.21], "payload": {"name": "Tuzik"}},
                {"id": 7, "vector": [0.99, 0.81, 0.75, 0.31], "payload": {"name": "Bobik"}},
                {"id": 8, "vector": [0.29, 0.01, 0.05, 0.91], "payload": {"name": "Muhtar"}},
            ]
        })
    assert_http_ok(r)

    # Check total number of points
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/count",
        json={
            "exact": True,
        }
    )
    assert_http_ok(r)
    assert r.json()["result"]["count"] == 8