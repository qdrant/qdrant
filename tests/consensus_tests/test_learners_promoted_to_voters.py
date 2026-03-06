import pathlib
import requests

from .utils import *

N_PEERS = 5


def all_peers_are_voters(peer_api_uris: [str]) -> bool:
    try:
        for uri in peer_api_uris:
            if not get_cluster_info(uri)["raft_info"]["is_voter"]:
                return False
        return True
    except requests.exceptions.ConnectionError:
        return False


def test_collection_after_peers_added(tmp_path: pathlib.Path):
    assert_project_root()
    peer_dirs = make_peer_folders(tmp_path, N_PEERS)

    # Gathers REST API uris
    peer_api_uris = []

    (bootstrap_api_uri, bootstrap_uri) = start_first_peer(
        peer_dirs[0], "peer_0_0.log")
    peer_api_uris.append(bootstrap_api_uri)

    # Wait for leader
    leader = wait_peer_added(bootstrap_api_uri)

    for i in range(1, len(peer_dirs)):
        peer_api_uris.append(start_peer(
            peer_dirs[i], f"peer_0_{i}.log", bootstrap_uri))

    wait_for(all_peers_are_voters, peer_api_uris)
