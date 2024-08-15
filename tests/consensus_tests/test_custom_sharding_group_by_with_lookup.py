import pathlib

from .utils import *

N_PEERS = 3
N_SHARDS = 1
N_REPLICAS = 1

COLLECTION_NAME = "test_collection"
LOOKUP_COLLECTION_NAME = "test_lookup_collection"

# https://github.com/qdrant/qdrant/issues/4425
def test_shard_group_by_with_lookup(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection_with_custom_sharding(
        peer_api_uris[0],
        collection=COLLECTION_NAME,
        size=1,
        shard_number=N_SHARDS,
        replication_factor=N_REPLICAS
    )
    create_collection_with_custom_sharding(
        peer_api_uris[0],
        collection=LOOKUP_COLLECTION_NAME,
        size=1,
        shard_number=N_SHARDS,
        replication_factor=N_REPLICAS
    )
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris)
    wait_collection_exists_and_active_on_all_peers(collection_name=LOOKUP_COLLECTION_NAME, peer_api_uris=peer_api_uris)

    # Create shards
    create_shard(
        peer_api_uris[0],
        COLLECTION_NAME,
        shard_key="first",
        shard_number=1,
        replication_factor=1
    )

    create_shard(
        peer_api_uris[0],
        LOOKUP_COLLECTION_NAME,
        shard_key="second",
        shard_number=1,
        replication_factor=1
    )

    create_shard(
        peer_api_uris[0],
        LOOKUP_COLLECTION_NAME,
        shard_key="third",
        shard_number=1,
        replication_factor=1
    )


    # Insert data

    # Create points in first peer's collection
    r = requests.put(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points?wait=true", json={
            "shard_key": "first",
            "points": [
                {"id": 1, "vector": [1.0], "payload": {"document_id": 1}},
                {"id": 2, "vector": [2.0], "payload": {"document_id": 2}},
                {"id": 3, "vector": [3.0], "payload": {"document_id": 2}},
                {"id": 4, "vector": [4.0], "payload": {"document_id": 1}},
                {"id": 5, "vector": [5.0], "payload": {"document_id": 3}},
            ]
        })
    assert_http_ok(r)

    r = requests.put(
        f"{peer_api_uris[0]}/collections/{LOOKUP_COLLECTION_NAME}/points?wait=true", json={
            "shard_key": "second",
            "points": [
                {"id": 1, "vector": [0.0], "payload": {"document_title": "I am number one"}},
                {"id": 2, "vector": [0.0], "payload": {"document_title": "Second"}},
            ]
    })
    assert_http_ok(r)

    r = requests.put(
        f"{peer_api_uris[0]}/collections/{LOOKUP_COLLECTION_NAME}/points?wait=true", json={
            "shard_key": "third",
            "points": [
                {"id": 3, "vector": [0.0], "payload": {"document_title": "On a different shard"}},
            ]
    })
    assert_http_ok(r)

    # Search points within the shard without explicit shard key for lookup
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/search/groups",
        json={
            "vector": [2.5],
            "shard_key": "first",
            "group_by": "document_id",
            "group_size": 2,
            "limit": 10,
            "with_lookup": {
                "collection": LOOKUP_COLLECTION_NAME,
                "with_payload": True,
            },
        }
    )
    assert_http_ok(r)
    groups = r.json()["result"]["groups"]
    assert(len(groups) == 3)
    for group in groups:
        assert group["lookup"]
        assert group["lookup"]["payload"]
        assert group["lookup"]["payload"]["document_title"] in ["I am number one", "Second", "On a different shard"]

    # Search points within the shard with explicit shard key for lookup
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points/search/groups",
        json={
            "vector": [2.5],
            "shard_key": "first",
            "group_by": "document_id",
            "group_size": 2,
            "limit": 10,
            "with_lookup": {
                "shard_key": "second",
                "collection": LOOKUP_COLLECTION_NAME,
                "with_payload": True,
            },
        }
    )
    assert_http_ok(r)
    groups = r.json()["result"]["groups"]
    assert(len(groups) == 3)
    groups_with_lookup = [group for group in groups if "lookup" in group]
    assert(len(groups_with_lookup) == 2)
    for group in groups_with_lookup:
        assert group["lookup"]["payload"]
        assert group["lookup"]["payload"]["document_title"] in ["I am number one", "Second"]
