import pathlib
from .utils import *
from .fixtures import upsert_random_points, create_collection

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


def _no_transfers_involve_peer(peer_api_uri: str, collection_name: str, peer_id: int) -> bool:
    """True when no in-flight transfer lists `peer_id` as source or destination."""
    info = get_collection_cluster_info(peer_api_uri, collection_name)
    for transfer in info["shard_transfers"]:
        if transfer["from"] == peer_id or transfer["to"] == peer_id:
            return False
    return True


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

    # Start a sync transfer from the last peer to the first peer
    from_peer_uri = peer_api_uris[-1]
    from_peer_id = peer_url_to_id[from_peer_uri]
    to_peer_id = peer_url_to_id[peer_api_uris[0]]
    replicate_shard(from_peer_uri, COLLECTION_NAME, 0, from_peer_id, to_peer_id)

    # Catch the transfer while it is still in flight
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 1)

    # Force delete the transfer source while the transfer is ongoing
    force_delete_peer(peer_api_uris[0], from_peer_id)

    # Stop the removed peer so it cannot keep campaigning after being dropped from Raft
    processes.pop().kill()
    remaining_uris = peer_api_uris[:-1]

    # Intent: transfers involving the deleted peer must be aborted.
    # Do not require the global transfer count to hit 0 — aborting a sync transfer
    # marks the destination Dead, which can trigger a recovery transfer between the
    # remaining peers that is unrelated to the deleted peer (and can race with the
    # aborted snapshot restore, leaving a stuck recovery transfer).
    wait_for(
        _no_transfers_involve_peer,
        remaining_uris[0],
        COLLECTION_NAME,
        from_peer_id,
    )
    wait_for(check_cluster_size, remaining_uris[0], N_PEERS - 1)
