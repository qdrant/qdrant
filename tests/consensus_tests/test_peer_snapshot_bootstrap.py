import pathlib
import urllib
import time

from .utils import *
from .assertions import assert_http_ok

N_PEERS = 4
COLLECTION_NAME = "test_collection"


def test_peer_snapshot_bootstrap(tmp_path: pathlib.Path):
    assert_project_root()

    peer_dirs = make_peer_folders(tmp_path, N_PEERS)

    # Gathers REST API uris
    peer_api_uris = []

    # Start bootstrap
    (bootstrap_api_uri, bootstrap_uri) = start_first_peer(peer_dirs[0], "peer_0_0.log")
    peer_api_uris.append(bootstrap_api_uri)

    # Wait for leader
    leader = wait_peer_added(bootstrap_api_uri)

    # Start other peers
    for i in range(1, len(peer_dirs) - 1):
        peer_api_uris.append(start_peer(peer_dirs[i], f"peer_0_{i}.log", bootstrap_uri))

    # Wait for cluster
    wait_for_uniform_cluster_status(peer_api_uris, leader)

    # Check that there are no collections on all peers
    for uri in peer_api_uris:
        r = requests.get(f"{uri}/collections")
        assert_http_ok(r)
        assert len(r.json()["result"]["collections"]) == 0

    # Create collection on first peer
    r = requests.put(f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}", json = {
        "vectors": {
            "size": 4,
            "distance": "Dot"
        }
    })
    assert_http_ok(r)

    # Check that it exists on all peers
    wait_collection_exists_and_active_on_all_peers(
        collection_name = COLLECTION_NAME,
        peer_api_uris = peer_api_uris,
    )

    # Recover all follower peers
    for peer_api_uri in peer_api_uris[1:]:
        r = requests.post(f"{peer_api_uri}/cluster/recover")
        assert_http_ok(r)

    # Kill leader
    processes.pop(0).kill()

    # Wait for new leader
    wait_for(leader_is_reelected, peer_api_uris[1], leader)

    # Restart killed peer
    (bootstrap_api_uri, bootstrap_uri) = start_first_peer(peer_dirs[0], "peer_0_0_restarted.log")
    peer_api_uris[0] = bootstrap_api_uri

    # Wait for restarted peer
    leader = wait_peer_added(bootstrap_api_uri, expected_size = N_PEERS - 1)

    # Recover restarted peer
    r = requests.post(f"{bootstrap_api_uri}/cluster/recover")
    assert_http_ok(r)

    # Start new peer
    peer_api_uris.append(start_peer(
        peer_dirs[-1], f"peer_0_{len(peer_dirs) - 1}.log", bootstrap_uri))

    # Wait for new peer
    wait_peer_added(bootstrap_api_uri, expected_size = N_PEERS)

    # Check that collection exists on new peer
    wait_collection_exists_and_active_on_all_peers(
        collection_name = COLLECTION_NAME,
        peer_api_uris = peer_api_uris,
    )

def leader_is_reelected(uri, previous_leader):
    r = requests.get(f"{uri}/cluster")
    assert_http_ok(r)
    leader = r.json()["result"]["raft_info"]["leader"]
    return not leader is None and leader != previous_leader
