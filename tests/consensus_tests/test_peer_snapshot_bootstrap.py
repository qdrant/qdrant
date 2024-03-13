import pathlib

from .utils import *
from .assertions import assert_http_ok

N_PEERS = 4
COLLECTION_NAME = "test_collection"


def test_peer_snapshot_bootstrap(tmp_path: pathlib.Path):
    assert_project_root()

    peer_dirs = make_peer_folders(tmp_path, N_PEERS)

    # Gathers REST API uris
    peer_api_uris = []

    # Get stable port for first peer
    #
    # NOTE:
    #
    # This peer will be killed and restarted later in the test.
    #
    # If the port changes and the peer have to report URI change,
    # *4 out of 5 times it will fail to do so*, and the test will fail.

    # Start bootstrap
    (bootstrap_api_uri, bootstrap_uri) = start_first_peer(
        peer_dirs[0], "peer_0_0.log")
    
    first_peer_port = processes[0].http_port

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
    #
    # NOTE: `leader_is_reelected(3)` is not an error! It returns a stateful closure.
    wait_for(leader_is_reelected(3), peer_api_uris[1], leader)

    # Restart killed peer
    (bootstrap_api_uri, bootstrap_uri) = start_first_peer(
        peer_dirs[0], "peer_0_0_restarted.log", port = first_peer_port)

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
    wait_for_peer_online(peer_api_uris[-1])

    # Check that collection exists on new peer
    wait_collection_exists_and_active_on_all_peers(
        collection_name = COLLECTION_NAME,
        peer_api_uris = peer_api_uris,
    )

def leader_is_reelected(count):
    current_leader = None
    current_leader_count = 0

    def leader_is_reelected(uri, previous_leader):
        r = requests.get(f"{uri}/cluster")
        assert_http_ok(r)

        leader = r.json()["result"]["raft_info"]["leader"]

        if leader != previous_leader and not leader is None:
            nonlocal current_leader
            nonlocal current_leader_count

            if leader == current_leader:
                current_leader_count += 1
            else:
                current_leader = leader
                current_leader_count = 1

            if current_leader_count >= count:
                return True

        return False

    return leader_is_reelected
