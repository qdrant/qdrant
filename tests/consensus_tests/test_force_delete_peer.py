import pathlib
from .utils import *
from .fixtures import upsert_random_points, create_collection
from time import sleep

COLLECTION_NAME = "test_collection"
N_PEERS = 3
N_REPLICAS = 3
N_SHARDS = 2

def force_delete_peer(peer_api_uri: str, peer_id: int):
    response = requests.delete(
        f"{peer_api_uri}/cluster/peer/{peer_id}?force=true",
    )
    assert response.status_code == 200, f"Failed to force delete peer: {response.text}"


def get_peer_id(peer_api_uri: str) -> int:
    response = requests.get(f"{peer_api_uri}/cluster")
    assert response.status_code == 200, f"Failed to get peer ID: {response.text}"
    return response.json()["result"]["peer_id"]

def test_force_delete_source_peer_during_transfers(tmp_path: pathlib.Path):
    assert_project_root()

    # seed port to reuse the same port for the restarted nodes
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, 20000)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICAS)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )

    peer_id_to_url = {}
    for peer_api_uri in peer_api_uris:
        peer_id = get_peer_id(peer_api_uri)
        peer_id_to_url[peer_id] = peer_api_uri

    # Insert some initial number of points
    upsert_random_points(peer_api_uris[0], 1000)

    # Now
    # Kill last peer
    p = processes.pop()
    p.kill()
    sleep(1)  # Give killed peer time to release WAL lock

    # Upsert points to mark dummy replica as dead, that will trigger recovery transfer
    upsert_random_points(peer_api_uris[0], 100)

    # Restart same peer
    peer_api_uris[-1] = start_peer(
        peer_dirs[-1],
        f"peer_{N_PEERS}_restarted.log",
        bootstrap_uri,
    )

    # Wait for start of shard transfer
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 1)

    transfers = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)[
        "shard_transfers"
    ]
    assert len(transfers) == 1
    assert transfers[0]["to"] == list(peer_id_to_url.keys())[-1] # last peer was restarted
    from_peer_id = transfers[0]["from"]

    # Stop the 'source' node to simulate an unreachable node
    source_peer_url = peer_id_to_url[from_peer_id]
    peer_idx = peer_api_uris.index(source_peer_url)
    p = processes.pop(peer_idx)
    url = peer_api_uris.pop(peer_idx)
    sleep(1)  # Give killed peer time to release WAL lock

    # Force delete 'from' peer ID by requesting remaining peers to do so
    force_delete_peer(peer_api_uris[0], from_peer_id)

    sleep(1)

    # We expect transfers to be aborted
    transfers = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)[
        "shard_transfers"
    ]
    assert len(transfers) == 0
