import pathlib

from .fixtures import create_collection, upsert_random_points
from .utils import *

N_PEERS = 3
COLLECTION_NAME = "test_collection"


def create_index(peer_api_uri: str, field_name: str, field_schema, timeout: int = 10):
    return requests.put(
        f"{peer_api_uri}/collections/{COLLECTION_NAME}/index?wait=true&timeout={timeout}",
        json={"field_name": field_name, "field_schema": field_schema},
    )


def test_create_existing_payload_index_without_consensus(tmp_path: pathlib.Path):
    """
    Creating a payload index that already exists with the same schema must succeed
    without consensus, so applications that "ensure" their indexes on every write keep
    working while the cluster can't commit.
    """
    assert_project_root()

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    # A single shard with a single replica, so that writes to it need no other peer.
    # The strict mode limit is already reached by the one index we create.
    create_collection(
        peer_api_uris[0],
        shard_number=1,
        replication_factor=1,
        strict_mode={"enabled": True, "max_payload_index_count": 1},
    )
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )
    upsert_random_points(peer_api_uris[0], 100)

    assert_http_ok(create_index(peer_api_uris[0], "city", "keyword"))
    wait_for_same_applied_commit(peer_api_uris)

    # Re-creating an existing index is not a new index, so the strict mode limit
    # doesn't apply to it
    for peer_api_uri in peer_api_uris:
        assert_http_ok(create_index(peer_api_uri, "city", "keyword"))

    # A new index is still refused by the limit
    resp = create_index(peer_api_uris[0], "country", "keyword")
    assert resp.status_code == 400, resp.text
    assert "maximum number of payload indices" in resp.text, resp.text

    # Keep only the peer holding the shard; without the other two it has no quorum
    shard_peer = next(
        idx
        for idx, uri in enumerate(peer_api_uris)
        if len(get_collection_cluster_info(uri, COLLECTION_NAME)["local_shards"]) > 0
    )
    for idx in sorted(range(N_PEERS), reverse=True):
        if idx != shard_peer:
            processes.pop(idx).kill()
    peer_api_uri = peer_api_uris[shard_peer]

    # The existing index is ensured without consensus
    resp = create_index(peer_api_uri, "city", "keyword")
    assert_http_ok(resp)
    assert resp.json()["result"]["status"] == "completed", resp.text

    # The same schema with default params spelled out is the same index
    resp = create_index(peer_api_uri, "city", {"type": "keyword"})
    assert_http_ok(resp)

    # A different schema for the same field needs consensus, which can't commit
    resp = create_index(peer_api_uri, "city", "integer", timeout=2)
    assert not resp.ok, resp.text

    # Filtering on the index still works
    resp = requests.post(
        f"{peer_api_uri}/collections/{COLLECTION_NAME}/points/scroll",
        json={"filter": {"must": [{"key": "city", "match": {"value": "London"}}]}},
    )
    assert_http_ok(resp)
