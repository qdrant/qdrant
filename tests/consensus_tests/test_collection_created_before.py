import pathlib

from .utils import *

N_PEERS = 5
N_SHARDS = 6


def test_collection_before_peers_added(tmp_path: pathlib.Path):
    assert_project_root()
    peer_dirs = make_peer_folders(tmp_path, N_PEERS)

    # Gathers REST API uris
    peer_api_uris = []

    (bootstrap_api_uri, bootstrap_uri) = start_first_peer(
        peer_dirs[0], "peer_0_0.log")
    peer_api_uris.append(bootstrap_api_uri)

    # Wait for leader
    leader = wait_peer_added(bootstrap_api_uri)

    # Create collection
    r = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection", json={
            "vector_size": 4,
            "distance": "Dot",
            "shard_number": N_SHARDS
        })
    assert_http_ok(r)

    # Start other peers
    for i in range(1, len(peer_dirs)):
        peer_api_uris.append(start_peer(
            peer_dirs[i], f"peer_0_{i}.log", bootstrap_uri))
        # Add peers one by one sequentially
        wait_peer_added(peer_api_uris[i])

    # Wait for cluster
    wait_for_uniform_cluster_status(peer_api_uris, leader)

    # Check that it exists on all peers
    assert_collection_exists_on_all_peers("test_collection", peer_api_uris)
