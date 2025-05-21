import pathlib
from .utils import *
from .fixtures import upsert_random_points, create_collection
from time import sleep

COLLECTION_NAME = "test_collection"
N_PEERS = 3

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

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0], shard_number=2, replication_factor=3)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )

    peer_url_to_id = {}
    for peer_api_uri in peer_api_uris:
        peer_id = get_peer_id(peer_api_uri)
        peer_url_to_id[peer_api_uri] = peer_id

    # Insert some initial number of points
    upsert_random_points(peer_api_uris[0], 3000)

    # Start a transfer from first peer to last peer ID
    from_peer_id = peer_url_to_id[peer_api_uris[-1]]
    to_peer_id = peer_url_to_id[peer_api_uris[0]]
    replicate_shard(peer_api_uris[-1], COLLECTION_NAME, 0, from_peer_id, to_peer_id)

    # Force delete 'from' peer ID by requesting remaining peers to do so
    force_delete_peer(peer_api_uris[0], from_peer_id)

    # We expect all transfers to be aborted (peer was force deleted before transfer finished) or finished
    wait_for_collection_shard_transfers_count(
        peer_api_uris[0], COLLECTION_NAME, 0
    )
