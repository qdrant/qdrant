import pathlib
from .utils import *
from .fixtures import create_collection, upsert_random_points

N_PEERS = 3
N_SHARDS = 1
N_REPLICAS = 3
COLLECTION_NAME = "test_collection"

def get_local_shards(peer_api_uri):
    r = requests.get(f"{peer_api_uri}/collections/{COLLECTION_NAME}/cluster")
    assert_http_ok(r)
    return r.json()["result"]['local_shards']

def test_upserts_in_recovery_mode(tmp_path: pathlib.Path):
    # Start cluster in recovery mode a transfer to dummy shard works if triggered by the user
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path,
        N_PEERS,
    )

    create_collection(
        peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICAS
    )
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )
    wait_for_same_commit(peer_api_uris=peer_api_uris)

    n_points = 100
    upsert_random_points(peer_api_uris[0], n_points)

    # Restart last peer with recovery mode
    extra_env = {
        "QDRANT__STORAGE__RECOVERY_MODE": "Custom recovery msg"
    }
    p = processes.pop()
    p.kill()
    peer_api_uris[-1] = start_peer(
        peer_dirs[-1], f"peer_{N_PEERS}_restarted.log", bootstrap_uri,
        extra_env=extra_env
    )

    # Qdrant loads a dummy shard but doesn't try to recover it from other replicas by default
    wait_peer_added(peer_api_uris[-1], 3)
    wait_for_all_replicas_active(peer_api_uris[-1], COLLECTION_NAME)

    [local_shard] = get_local_shards(peer_api_uris[-1])
    assert local_shard["shard_id"] == 0
    assert local_shard["state"] == "Active"
    assert local_shard["points_count"] == 0

    # Upsert 1 vector
    upsert_random_points(peer_api_uris[0], 1)

    # An update marks replicas of the node in recovery mode as Dead
    # However, Qdrant will not initiate a transfer to such replicas
    wait_for_same_commit(peer_api_uris)
    wait_for_collection_shard_transfers_count(peer_api_uris[-1], COLLECTION_NAME, 0)

    [local_shard] = get_local_shards(peer_api_uris[-1])
    assert local_shard["shard_id"] == 0
    assert local_shard["state"] == "Dead" # marked dead
    assert local_shard["points_count"] == 0
