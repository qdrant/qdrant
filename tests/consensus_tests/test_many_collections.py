import pathlib

from .utils import *

N_PEERS = 5
N_COLLECTIONS = 20


def test_many_collections(tmp_path: pathlib.Path):
    assert_project_root()
    peer_dirs = make_peer_folders(tmp_path, N_PEERS)

    # Gathers REST API uris
    peer_api_uris = []

    # Start bootstrap
    (bootstrap_api_uri, bootstrap_uri) = start_first_peer(
        peer_dirs[0], "peer_0_0.log")
    peer_api_uris.append(bootstrap_api_uri)

    # Wait for leader
    leader = wait_peer_added(bootstrap_api_uri)

    # Start other peers
    for i in range(1, len(peer_dirs)):
        peer_api_uris.append(start_peer(
            peer_dirs[i], f"peer_0_{i}.log", bootstrap_uri))

    # Wait for cluster
    wait_for_uniform_cluster_status(peer_api_uris, leader)

    # Check that there are no collections on all peers
    for uri in peer_api_uris:
        r = requests.get(f"{uri}/collections")
        assert_http_ok(r)
        assert len(r.json()["result"]["collections"]) == 0

    # Create N_COLLECTIONS on different peers
    for i in range(1, N_COLLECTIONS + 1):
        peer = peer_api_uris[i % N_PEERS]
        print(f"creating test_collection_{i} on {peer}")
        r = requests.put(
            f"{peer}/collections/test_collection_{i}", json={
                "vectors": {
                    "size": 4,
                    "distance": "Dot"
                },
                "shard_number": 1  # single shard
            })
        assert_http_ok(r)

    # Check that all collections exist on all peers
    for i in range(1, N_COLLECTIONS + 1):
        wait_for_uniform_collection_existence(f"test_collection_{i}", peer_api_uris)
