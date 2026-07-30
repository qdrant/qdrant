import pathlib

from .custom_sharding import create_collection_with_custom_sharding, create_shard
from .test_custom_sharding import wait_for_peer_metadata
from .test_resharding import start_resharding
from .fixtures import *
from .utils import *

N_PEERS = 3
N_REPLICAS = 2

COLLECTION_NAME = "test_collection"
SHARD_KEY = "cats"

POINTS = [
    {"id": 1, "vector": [0.29, 0.81, 0.75, 0.11], "payload": {"name": "Barsik"}},
    {"id": 2, "vector": [0.19, 0.11, 0.15, 0.21], "payload": {"name": "Murzik"}},
    {"id": 3, "vector": [0.99, 0.81, 0.75, 0.31], "payload": {"name": "Vaska"}},
    {"id": 4, "vector": [0.29, 0.01, 0.05, 0.91], "payload": {"name": "Chubais"}},
]


def test_search_during_resharding_with_shard_keys(tmp_path: pathlib.Path):
    """
    Creates a custom sharded collection with a shard key and 2 replicas,
    then starts resharding (initialization only, no progress) and asserts
    that searches with and without shard key selector keep working.

    Expected to fail due to a known bug: after resharding is started on
    a collection with shard keys, searches break.
    """
    assert_project_root()

    # Allow resharding
    env = {
        "QDRANT__CLUSTER__RESHARDING_ENABLED": "true",
    }

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, N_PEERS, extra_env=env)

    # Wait until all peers submit their metadata to consensus
    wait_for_peer_metadata(peer_api_uris[0])

    create_collection_with_custom_sharding(
        peer_api_uris[0],
        shard_number=1,
        replication_factor=N_REPLICAS,
    )
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )

    create_shard(
        peer_api_uris[0],
        COLLECTION_NAME,
        shard_key=SHARD_KEY,
        shard_number=1,
        replication_factor=N_REPLICAS,
    )

    # Upload a minimal amount of data
    r = requests.put(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points?wait=true", json={
            "shard_key": SHARD_KEY,
            "points": POINTS,
        })
    assert_http_ok(r)

    # Searches must work before resharding
    for peer_api_uri in peer_api_uris:
        assert_search_works(peer_api_uri, with_shard_key=True)
        assert_search_works(peer_api_uri, with_shard_key=False)

    # Start resharding, initialization only, no transfer progress needed
    resp = start_resharding(peer_api_uris[0], COLLECTION_NAME, shard_key=SHARD_KEY)
    assert_http_ok(resp)

    wait_for_collection_resharding_operations_count(peer_api_uris[0], COLLECTION_NAME, 1)

    # Searches must still work after resharding is initialized
    for peer_api_uri in peer_api_uris:
        assert_search_works(peer_api_uri, with_shard_key=True)
        assert_search_works(peer_api_uri, with_shard_key=False)


def assert_search_works(peer_api_uri: str, with_shard_key: bool):
    request = {
        "vector": [0.29, 0.81, 0.75, 0.11],
        "limit": 10,
        "with_payload": True,
    }
    if with_shard_key:
        request["shard_key"] = SHARD_KEY

    r = requests.post(
        f"{peer_api_uri}/collections/{COLLECTION_NAME}/points/search",
        json=request,
    )
    assert_http_ok(r)

    result = r.json()["result"]
    selector = f"shard_key={SHARD_KEY!r}" if with_shard_key else "no shard_key"
    assert len(result) == len(POINTS), \
        f"search on {peer_api_uri} with {selector} returned {len(result)} points, expected {len(POINTS)}"
    for point in result:
        assert point["shard_key"] == SHARD_KEY
