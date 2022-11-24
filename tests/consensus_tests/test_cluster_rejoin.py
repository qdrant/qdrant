import pathlib

from consensus_tests.fixtures import create_collection, upsert_random_points, drop_collection
import requests
from .utils import *

N_PEERS = 3
N_REPLICA = 2
N_SHARDS = 3


def test_rejoin_cluster(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA)
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)
    upsert_random_points(peer_api_uris[0], 100)

    p = processes.pop()
    p.kill()

    # Validate upsert works with the dead node
    upsert_random_points(peer_api_uris[0], 100)

    # Assert that there are dead replicas
    wait_for_some_replicas_not_active(peer_api_uris[0], "test_collection")

    for i in range(0, 2):
        print(f"creating collection {i}")
        drop_collection(peer_api_uris[0], timeout=1)
        create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA, timeout=1)
        # Collection might not be ready yet, we don't care
        upsert_random_points(peer_api_uris[0], 100)
        print(f"after recovery end {i}")
        res = requests.get(f"{peer_api_uris[1]}/collections")
        print(res.json())

    create_collection(
        peer_api_uris[0],
        "test_collection2",
        shard_number=N_SHARDS,
        replication_factor=N_REPLICA,
        timeout=1
    )

    new_url = start_peer(peer_dirs[-1], f"peer_0_restarted.log", bootstrap_uri)

    for i in range(0, 5):
        print(f"after recovery start {i}")
        drop_collection(peer_api_uris[0], timeout=1)
        create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA, timeout=1)
        upsert_random_points(peer_api_uris[0], 500, fail_on_error=False)
        print(f"after recovery end {i}")
        res = requests.get(f"{new_url}/collections")
        print(res.json())

    wait_for_all_replicas_active(peer_api_uris[0], "test_collection2")
    wait_for_all_replicas_active(new_url, "test_collection2")
