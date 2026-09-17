import pathlib
import pytest
from .utils import *
from .fixtures import upsert_points, create_collection

COLLECTION_NAME = "test_collection"
N_PEERS = 3
RECOVERY_POINT = "/qdrant.CollectionsInternal/GetShardRecoveryPoint"

def force_delete_peer(peer_api_uri: str, peer_id: int):
    response = requests.delete(
        f"{peer_api_uri}/cluster/peer/{peer_id}?force=true",
    )
    assert response.status_code == 200, f"Failed to force delete peer: {response.text}"


def get_peer_id(peer_api_uri: str) -> int:
    response = requests.get(f"{peer_api_uri}/cluster")
    assert response.status_code == 200, f"Failed to get peer ID: {response.text}"
    return response.json()["result"]["peer_id"]


def peer_is_removed_from_cluster_and_transfers(peer_api_uri: str, removed_peer_id: int) -> bool:
    peers = get_cluster_info(peer_api_uri)["peers"]
    transfers = get_collection_cluster_info(peer_api_uri, COLLECTION_NAME)["shard_transfers"]
    remaining_transfers = [
        transfer for transfer in transfers
        if removed_peer_id in (transfer["from"], transfer["to"])
    ]
    if str(removed_peer_id) in peers or remaining_transfers:
        print(f"Waiting for peer {removed_peer_id} removal on {peer_api_uri}: peers={list(peers)}, transfers={remaining_transfers}")
        return False
    return True


@pytest.mark.parametrize("transfer_method", ["snapshot", "wal_delta"])
@pytest.mark.parametrize("stop_source", [False, True], ids=["source-running", "source-stopped"])
def test_force_delete_source_peer_during_transfers(tmp_path: pathlib.Path, transfer_method, stop_source):
    peer_api_uris, _, _ = start_cluster(tmp_path, N_PEERS, use_peer_proxy=True)
    source = processes[-1]

    create_collection(peer_api_uris[0], shard_number=2, replication_factor=3, write_consistency_factor=3)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )

    from_peer_id = get_peer_id(peer_api_uris[-1])
    to_peer_id = get_peer_id(peer_api_uris[0])
    points = [
        {
            "id": index,
            "vector": {
                "": [float(index), 1.0, 0.0, 0.0],
                "sparse-text": {"indices": [index % 1000], "values": [1.0]},
            },
            "payload": {"index": index},
        }
        for index in range(3000)
    ]
    assert_http_ok(upsert_points(peer_api_uris[0], points))
    survivors = peer_api_uris[:-1]
    initial_receiver = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
    assert initial_receiver["shard_transfers"] == []
    initial_shard_points = next(
        shard["points_count"] for shard in initial_receiver["local_shards"] if shard["shard_id"] == 0
    )
    assert initial_shard_points > 0

    proxy = processes[0].proxy
    if transfer_method == "snapshot":
        # Pause after the receiver clears its shard, before snapshot data arrives.
        gate_context = proxy.hold_snapshot_download(peer_api_uris[-1], COLLECTION_NAME, 0)
    else:
        # This is the only transfer. Pause the source's request for the receiver's
        # recovery point, before resolving or copying a WAL delta.
        gate_context = proxy.hold_rpc(RECOVERY_POINT)

    with gate_context as gate:
        replicate_shard(peer_api_uris[-1], COLLECTION_NAME, 0, from_peer_id, to_peer_id, method=transfer_method)
        gate.wait_for_request()
        receiver = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
        transfer, = receiver["shard_transfers"]
        assert (transfer["from"], transfer["to"], transfer["shard_id"], transfer["method"]) == (
            from_peer_id, to_peer_id, 0, transfer_method,
        )
        shard = next(shard for shard in receiver["local_shards"] if shard["shard_id"] == 0)
        assert shard["state"] == "Recovery"
        assert shard["points_count"] == (0 if transfer_method == "snapshot" else initial_shard_points)
        assert not gate.cancelled.is_set()

        force_delete_peer(peer_api_uris[0], from_peer_id)
        # Survivor-to-survivor recovery is allowed while the old request is held.
        for uri in survivors:
            wait_for(peer_is_removed_from_cluster_and_transfers, uri, from_peer_id)

        if stop_source:
            # Membership removal does not prove that the source has stopped work.
            # Wait for process exit before releasing the request so recovery cannot
            # use a snapshot or WAL delta from the removed source.
            source.kill()
            processes.remove(source)
        else:
            # Also cover removal while the old source can still finish in-flight work.
            assert source.proc.poll() is None

    # Removal alone is not success. Both survivors must finish recovery and keep
    # every point, including dense vectors, sparse vectors, and payloads.
    for uri in survivors:
        wait_for_collection_shard_transfers_count(uri, COLLECTION_NAME, 0)
        wait_for_all_replicas_active(uri, COLLECTION_NAME, min_local_replicas=2)
        cluster = get_collection_cluster_info(uri, COLLECTION_NAME)
        assert len(cluster["local_shards"]) == 2
        assert sum(shard["points_count"] for shard in cluster["local_shards"]) == len(points)
        assert all(shard["peer_id"] != from_peer_id for shard in cluster["remote_shards"])
        response = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/scroll?consistency=all",
            json={"limit": len(points), "with_payload": True, "with_vector": True}, timeout=10,
        )
        assert_http_ok(response)
        assert response.json()["result"]["points"] == points
        assert response.json()["result"]["next_page_offset"] is None
