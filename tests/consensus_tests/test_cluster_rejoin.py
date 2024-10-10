import io
import pathlib
from typing import Any

from consensus_tests.fixtures import create_collection, upsert_random_points, drop_collection
import requests
from .utils import *

N_PEERS = 3
N_REPLICA = 2
N_SHARDS = 3


def test_rejoin_cluster(tmp_path: pathlib.Path):
    assert_project_root()
    # Start cluster
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, port_seed=10000)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA)
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)
    upsert_random_points(peer_api_uris[0], 100)

    # Stop last node
    p = processes.pop()
    p.kill()

    # Validate upsert works with the dead node
    upsert_random_points(peer_api_uris[0], 100)

    # Assert that there are dead replicas
    wait_for_some_replicas_not_active(peer_api_uris[0], "test_collection")

    # Repeatedly drop, re-create collection and add data to it to accumulate Raft log entries
    for i in range(0, 2):
        print(f"creating collection {i}")
        # Drop test_collection
        drop_collection(peer_api_uris[0], "test_collection", timeout=5)
        # Re-create test_collection
        create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA, timeout=3)
        # Collection might not be ready yet, we don't care
        upsert_random_points(peer_api_uris[0], 100)
        print(f"before recovery end {i}")
        res = requests.get(f"{peer_api_uris[1]}/collections")
        print(res.json())

    # Create new collection unknown to the dead node
    create_collection(
        peer_api_uris[0],
        "test_collection2",
        shard_number=N_SHARDS,
        replication_factor=N_REPLICA,
        timeout=3
    )

    # Restart last node
    new_url = start_peer(peer_dirs[-1], "peer_0_restarted.log", bootstrap_uri, port=20000)

    peer_api_uris[-1] = new_url

    # Wait for restarted node to be up and ready
    wait_all_peers_up([new_url])

    # Repeatedly drop, re-create collection and add data to it to accumulate Raft log entries
    for i in range(0, 5):
        print(f"after recovery start {i}")
        # Drop test_collection
        drop_collection(peer_api_uris[0], "test_collection", timeout=5)
        # Re-create test_collection
        create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA, timeout=3)
        upsert_random_points(peer_api_uris[0], 500, fail_on_error=False)
        print(f"after recovery end {i}")
        res = requests.get(f"{new_url}/collections")
        print(res.json())

    wait_for_all_replicas_active(peer_api_uris[0], "test_collection2")
    # Assert that the restarted node has recovered the new collection
    wait_for_all_replicas_active(new_url, "test_collection2")


def test_rejoin_origin_from_wal(tmp_path: pathlib.Path):
    """
    This test checks that origin peer (first peer of the cluster) commits its own peer ID to consensus.

    - remove origin peer from cluster
    - modify second peer's `raft_state.json`, so that it does *not* provide origin peer ID and URL
      when bootstrapping new peer
    - add new peer to the cluster (bootstrapping from second peer), and check that it has valid
      state after it syncs with consensus
    - if new peer has valid state at the end of the test, it means it received correct origin peer
      ID and URL from consensus
    """

    # Overwrite `first_voter` peer
    def overwrite_first_voter(state: dict[str, Any], _: Any):
        state["first_voter"] = state["this_peer_id"]
        return state

    rejoin_cluster_test(tmp_path, start_cluster, overwrite_first_voter)


def rejoin_cluster_test(
    tmp_path: pathlib.Path,
    start_cluster: Callable[[pathlib.Path, int], tuple[list[str], list[pathlib.Path], str]],
    raft_state: Callable[[dict[str, Any], int], Any | None],
    collection: str = "test_collection",
    peers: int = 3,
    shards: int = 3,
    expected_shards: int = 3,
):
    """
    Parameterized test body, that tests adding new peer after origin peer was removed from the cluster.
    See: <https://github.com/qdrant/qdrant/issues/5138>
    """

    # Start cluster
    peer_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, peers)

    # Get origin peer ID
    origin_peer_id = get_cluster_info(peer_uris[0])["peer_id"]

    # Create collection, move all shards from first peer, remove first peer from cluster
    create_collection(peer_uris[0], collection, shards, 1)
    move_all_shards_from_peer(peer_uris[0], collection)
    remove_peer(peer_uris[0])
    processes.pop(0).kill()

    # Generally, we could use *any* (second/third/random/last/etc) peer to bootstrap new peer from,
    # but using second peer allows to (trivially) catch a single additional corner case in how we
    # initialize consensus state when bootstrapping new peer.

    # Kill second peer
    second_peer = processes.pop(0)
    second_peer.kill()

    # Check/modify last peer `raft_state.json`
    with open(f"{peer_dirs[1]}/storage/raft_state.json", "r+") as file:
        state = json.load(file)

        if new_state := raft_state(state, origin_peer_id):
            file.seek(0, io.SEEK_SET)
            file.truncate()
            json.dump(new_state, file)

    # Restart second peer with the same URI and ports
    second_peer_uri, bootstrap_uri = start_first_peer(peer_dirs[1], "peer_0_1_restarted.log", second_peer.p2p_port)
    wait_for_peer_online(second_peer_uri)

    # Add new peer to cluster
    new_peer_uri, new_peer_dir = add_new_peer(tmp_path, peers, bootstrap_uri, collection)

    # Assert that new peer observe expected number of remote shards
    info = get_collection_cluster_info(new_peer_uri, collection)
    assert len(info["remote_shards"]) == expected_shards


def move_all_shards_from_peer(peer_uri: str, collection: str = "test_collection") -> tuple[int, int]:
    """
    Moves all shards from peer at `peer_uri` to another (random) peer in the cluster.
    """

    # Find peer to move shards to
    info = get_cluster_info(peer_uri)

    current_peer_id = info["peer_id"]
    other_peer_id = None

    for peer_id, info in info["peers"].items():
        peer_id = int(peer_id)

        if peer_id != current_peer_id:
            other_peer_id = peer_id
            break

    assert other_peer_id

    # Move all shards from first peer to second peer
    info = get_collection_cluster_info(peer_uri, collection)

    for shard in info["local_shards"]:
        resp = requests.post(f"{peer_uri}/collections/{collection}/cluster", json={
            "move_shard": {
                "from_peer_id": current_peer_id,
                "to_peer_id": other_peer_id,
                "shard_id": shard["shard_id"],
            }
        })

        assert_http_ok(resp)

    # Wait until all transfers finished
    wait_for_collection_shard_transfers_count(peer_uri, collection, 0)

    return current_peer_id, other_peer_id

def remove_peer(peer_uri: str, peer_id: int | None = None):
    if peer_id is None:
        info = get_cluster_info(peer_uri)
        peer_id = info["peer_id"]

    resp = requests.delete(f"{peer_uri}/cluster/peer/{peer_id}")
    assert_http_ok(resp)

def add_new_peer(tmp_path: pathlib.Path, peer_idx: int, bootstrap_uri: str, collection: str | None = None):
    peer_dir = make_peer_folder(tmp_path, peer_idx)
    peer_uri = start_peer(peer_dir, f"peer_0_{peer_idx}.log", bootstrap_uri)

    wait_for_peer_online(peer_uri)

    if collection is not None:
        wait_collection_on_all_peers(collection, [peer_uri])

    return peer_uri, peer_dir
