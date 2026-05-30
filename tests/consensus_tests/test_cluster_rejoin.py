import pathlib

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
