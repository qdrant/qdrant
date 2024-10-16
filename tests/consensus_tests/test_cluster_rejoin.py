import io
import pathlib
import shutil
from time import sleep
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

def test_rejoin_origin_from_state(tmp_path: pathlib.Path):
    """
    This test checks that Qdrant persists origin peer ID (`first_voter` field in `raft_state.json`)
    and propagates fake origin peer URL when bootstrapping new peer.

    - start cluster using *preconfigured* origin peer that does *not* have origin peer ID and URL
      committed to consensus
    - remove origin peer from cluster
    - assert that second peer's `raft_state.json` contains valid origin peer ID
    - add new peer to the cluster (bootstrapping from second peer), and check that it has valid
      state after it syncs with consensus
    - if new peer has valid state at the end of the test, it means it received correct origin peer
      ID and (fake) URL from second peer during bootstrap
    """

    # Assert origin peer ID is persisted as `first_voter`
    def assert_first_voter(state: dict[str, Any], origin_peer_id: int):
        assert state["first_voter"] == origin_peer_id

    rejoin_cluster_test(tmp_path, start_preconfigured_cluster, assert_first_voter)

@pytest.mark.skip("this test simulates and asserts past, incorrect behavior")
def test_rejoin_no_origin(tmp_path: pathlib.Path):
    """
    This test checks that `rejoin_cluster_test` is sufficient to reproduce "missing origin peer" bug.

    It simulates *earlier* behavior of Qdrant (bypassing all fixes to commit/persist/recover origin
    peer ID/URL), and then checks that new peer added to such cluster has *invalid* state.

    This test is disabled by default, but it's useful to "test the tests" and reproduce original bug.
    """

    # Overwrite `first_voter` peer
    def overwrite_first_voter(state: dict[str, Any], _: Any):
        state["first_voter"] = 1337
        return state

    rejoin_cluster_test(tmp_path, start_preconfigured_cluster, overwrite_first_voter, expected_shards=2)


def test_rejoin_recover_origin(tmp_path: pathlib.Path):
    """
    This test checks that Qdrant recovers origin peer ID from WAL, if origin peer was not yet
    removed from the cluster.
    """

    collection = "test_collection"
    peers = 3
    shards = 3

    # Start cluster
    peer_uris, peer_dirs, bootstrap_uri = start_preconfigured_cluster(tmp_path, peers)

    # Get origin peer ID
    origin_peer_id = get_cluster_info(peer_uris[0])["peer_id"]

    # Wait a few seconds for consensus to catch up
    sleep(5)

    # Kill second peer
    second_peer = processes.pop(1)
    second_peer.kill()

    # Remove `first_voter` from `raft_state.json`
    with open(f"{peer_dirs[1]}/storage/raft_state.json", "r+") as file:
        state = json.load(file)

        del state["first_voter"]

        file.seek(0, io.SEEK_SET)
        file.truncate()
        json.dump(state, file)

    # Restart second peer with the same URI and ports
    second_peer_uri, bootstrap_uri = start_first_peer(peer_dirs[1], "peer_0_1_restarted.log", second_peer.p2p_port)
    wait_for_peer_online(second_peer_uri)

    # Assert second peer recovered `first_voter` from WAL
    with open(f"{peer_dirs[1]}/storage/raft_state.json", "r") as file:
        state = json.load(file)
        assert state["first_voter"] == origin_peer_id

    # Create collection, move all shards from first peer, remove first peer from cluster
    create_collection(peer_uris[0], collection, shards, 1)
    move_all_shards_from_peer(peer_uris[0], collection)
    remove_peer(peer_uris[0])
    processes.pop(0).kill()

    # Wait a few seconds for new leader
    sleep(5)

    # Add new peer to cluster
    new_peer_uri, new_peer_dir = add_new_peer(tmp_path, peers, bootstrap_uri, collection)

    # Assert that new peer observe expected number of remote shards
    info = get_collection_cluster_info(new_peer_uri, collection)
    assert len(info["remote_shards"]) == shards


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

def start_preconfigured_cluster(tmp_path: pathlib.Path, peers: int = 3):
    assert_project_root()

    # Collect peer URIs
    peer_uris = []

    # Create peer directories
    peer_dirs = make_peer_folders(tmp_path, peers)

    # Copy first peer Raft state and WAL from `test_cluster_rejoin_data`.
    #
    # It's just an "empty" peer, but its peer ID is *not* committed into WAL. We can use this peer to
    # test that first peer ID is correctly recovered/propagated, even when it's not committed into WAL.
    shutil.copytree("tests/consensus_tests/test_cluster_rejoin_data", f"{peer_dirs[0]}/storage")

    # Modify peer URI in Raft state to prevent URI change on startup 🙄
    p2p_port = get_port()
    grpc_port = get_port()
    http_port = get_port()

    with open(f"{peer_dirs[0]}/storage/raft_state.json", "r+") as file:
        state = json.load(file)

        state["peer_address_by_id"][str(state["this_peer_id"])] = f"http://127.0.0.1:{p2p_port}"

        file.seek(0, io.SEEK_SET)
        file.truncate()
        json.dump(state, file)

    # Start first peer
    first_peer_uri, bootstrap_uri = start_first_peer(peer_dirs[0], "peer_0_0.log", p2p_port)
    peer_uris.append(first_peer_uri)

    wait_for_peer_online(first_peer_uri)

    # Bootstrap other peers
    for peer_idx in range(1, peers):
        peer_uri = start_peer(peer_dirs[peer_idx], f"peer_0_{peer_idx}.log", bootstrap_uri)
        peer_uris.append(peer_uri)

    wait_all_peers_up(peer_uris)

    return peer_uris, peer_dirs, bootstrap_uri


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
