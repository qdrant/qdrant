import pathlib

from .utils import *

N_PEERS = 5


def test_collection_after_peers_added(tmp_path: pathlib.Path):
    assert_project_root()
    peer_dirs = make_peer_folders(tmp_path, N_PEERS)

    # Gathers REST API uris
    peer_api_uris = []

    (bootstrap_api_uri, bootstrap_uri) = start_first_peer(
        peer_dirs[0], "peer_0_0.log")
    peer_api_uris.append(bootstrap_api_uri)

    # Wait for leader
    wait_for_leader_setup(bootstrap_api_uri)

    for i in range(1, len(peer_dirs)):
        peer_api_uris.append(start_peer(
            peer_dirs[i], f"peer_0_{i}.log", bootstrap_uri))
        # Add peers one by one sequentially
        wait_for_leader_setup(peer_api_uris[i])

    # Wait for cluster
    wait_for_uniform_cluster_size(peer_api_uris)

    # Check that there are no collections on all peers
    for uri in peer_api_uris:
        r = requests.get(f"{uri}/collections")
        assert_http_ok(r)
        assert len(r.json()["result"]["collections"]) == 0

    # Create collection
    r = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection", json={
            "vector_size": 4,
            "distance": "Dot",
        })
    assert_http_ok(r)

    # Check that it exists on all peers
    wait_for_uniform_collection_existence("test_collection", peer_api_uris)
