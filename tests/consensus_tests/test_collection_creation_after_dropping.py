import pathlib
from random import randrange

from .assertions import assert_http_ok
from .fixtures import upsert_random_points
from .utils import *

N_PEERS = 5

COLLECTION_CONFIG = {
    "vectors": {
        "size": 4,
        "distance": "Dot",
    },
    "shard_number": 8,
    "replication_factor": 2,
}


def test_collection_creation_after_dropping(tmp_path: pathlib.Path):
    assert_project_root()
    peer_dirs = make_peer_folders(tmp_path, N_PEERS)

    # Gathers REST API uris
    peer_api_uris = []

    (bootstrap_api_uri, bootstrap_uri) = start_first_peer(peer_dirs[0], "peer_0_0.log")
    peer_api_uris.append(bootstrap_api_uri)

    # Wait for leader
    leader = wait_peer_added(bootstrap_api_uri)

    for i in range(1, len(peer_dirs)):
        peer_api_uris.append(start_peer(peer_dirs[i], f"peer_0_{i}.log", bootstrap_uri))

    # Wait for cluster
    wait_for_uniform_cluster_status(peer_api_uris, leader)

    # Check that there are no collections on all peers
    for uri in peer_api_uris:
        r = requests.get(f"{uri}/collections")
        assert_http_ok(r)
        assert len(r.json()["result"]["collections"]) == 0

    # Create collection
    r = requests.put(
        f"{peer_api_uris[randrange(N_PEERS)]}/collections/test_collection",
        json=COLLECTION_CONFIG,
    )
    assert_http_ok(r)

    for i in range(10):
        # Drop collection
        r = requests.delete(
            f"{peer_api_uris[randrange(N_PEERS)]}/collections/test_collection"
        )
        assert_http_ok(r)

        # Create collection
        r = requests.put(
            f"{peer_api_uris[randrange(N_PEERS)]}/collections/test_collection",
            json=COLLECTION_CONFIG,
        )
        assert_http_ok(r)

        # Upload some points to every peer
        for _ in range(20):
            for peer_api_uri in peer_api_uris:
                upsert_random_points(
                    peer_api_uri,
                    collection_name="test_collection",
                    num=50,
                    wait="false",
                    with_sparse_vector=False
                )

    # Get collections on all peers
    for i in range(N_PEERS):
        r = requests.get(f"{peer_api_uris[i]}/collections/test_collection")
        assert_http_ok(r)

    # Check that it exists on all peers
    wait_collection_exists_and_active_on_all_peers(
        collection_name="test_collection", peer_api_uris=peer_api_uris
    )
