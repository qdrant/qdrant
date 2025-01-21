import pathlib

from .utils import *

N_PEERS = 3
N_SHARDS = 1
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


def delete_shard(
        peer_url,
        collection,
        shard_key,
        timeout=10
):
    r_batch = requests.post(
        f"{peer_url}/collections/{collection}/shards/delete?timeout={timeout}",
        json={
            "shard_key": shard_key,
        }
    )
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

    # Search points within the shard
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/search",
        json={
            "vector": [0.29, 0.81, 0.75, 0.11],
            "shard_key": "cats",
            "limit": 10,
            "with_payload": True,
        }
    )

    assert_http_ok(r)
    result = r.json()["result"]
    assert len(result) == 4
    for point in result:
        assert point["payload"]["name"] in ["Barsik", "Murzik", "Vaska", "Chubais"]
        assert point["shard_key"] == "cats"

    # Search points within 2 shards
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/search",
        json={
            "vector": [0.29, 0.81, 0.75, 0.11],
            "shard_key": ["cats", "dogs"],
            "limit": 10,
            "with_payload": True,
        }
    )

    assert_http_ok(r)
    result = r.json()["result"]
    assert len(result) == 8
    for point in result:
        assert point["shard_key"] in ["cats", "dogs"]

    # Search across all the shards
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/search",
        json={
            "vector": [0.29, 0.81, 0.75, 0.11],
            "limit": 10,
        }
    )

    assert_http_ok(r)
    result = r.json()["result"]
    assert len(result) == 8
    for point in result:
        assert point["shard_key"] in ["cats", "dogs"]

    delete_shard(
        peer_api_uris[0],
        COLLECTION_NAME,
        shard_key="cats",
    )

    create_shard(
        peer_api_uris[0],
        COLLECTION_NAME,
        shard_key="birds",
        shard_number=1,
        replication_factor=1
    )

    r = requests.put(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points?wait=true", json={
            "shard_key": "birds",
            "points": [
                {"id": 9, "vector": [0.29, 0.81, 0.75, 0.11], "payload": {"name": "Kesha"}},
                {"id": 10, "vector": [0.19, 0.11, 0.15, 0.21], "payload": {"name": "Gosha"}},
            ]
        })
    assert_http_ok(r)

    # Search across all the shards
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/search",
        json={
            "vector": [0.29, 0.81, 0.75, 0.11],
            "limit": 10,
        }
    )

    assert_http_ok(r)
    result = r.json()["result"]

    assert len(result) == 6
    for point in result:
        assert point["shard_key"] in ["dogs", "birds"]


def test_shard_key_storage(tmp_path: pathlib.Path):
    """
    Creates cluster with custom sharding. Asserts custom sharding keys are
    loaded correctly on node restart.

    Tests bug: <https://github.com/qdrant/qdrant/pull/5838>
    """
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection_with_custom_sharding(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_PEERS)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris)

    # Create shards with string and number shard keys
    create_shard(
        peer_api_uris[0],
        COLLECTION_NAME,
        shard_key="cats",
        shard_number=1,
        replication_factor=N_PEERS,
    )
    create_shard(
        peer_api_uris[0],
        COLLECTION_NAME,
        shard_key="dogs",
        shard_number=1,
        replication_factor=N_PEERS,
    )
    create_shard(
        peer_api_uris[0],
        COLLECTION_NAME,
        shard_key=123,
        shard_number=1,
        replication_factor=N_PEERS,
    )
    create_shard(
        peer_api_uris[0],
        COLLECTION_NAME,
        shard_key=456,
        shard_number=1,
        replication_factor=N_PEERS,
    )

    # Shards must show keys in correct format
    info = get_collection_cluster_info(peer_api_uris[-1], COLLECTION_NAME)
    assert len(info['local_shards']) == 4
    for shard in info['local_shards']:
        assert shard['shard_key'] in ["cats", "dogs", 123, 456]

    # Kill the last peer
    processes.pop().kill()

    # Restart the last peer
    restarted_peer_url = start_peer(peer_dirs[-1], "peer_1_restarted.log", bootstrap_uri)
    peer_api_uris[-1] = restarted_peer_url

    wait_for_peer_online(peer_api_uris[-1])

    # After restart, shards must show keys in correct format
    info = get_collection_cluster_info(peer_api_uris[-1], COLLECTION_NAME)
    assert len(info['local_shards']) == 4
    for shard in info['local_shards']:
        # This was previously broken, changing numbers into strings on restart
        assert shard['shard_key'] in ["cats", "dogs", 123, 456]
