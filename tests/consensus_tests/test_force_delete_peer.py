import pathlib
from contextlib import ExitStack
import grpc
import pytest
from .utils import *
from .fixtures import upsert_points, create_collection
from .raft_messages import decode_raft_message
from .points_messages import is_upsert_batch_for

COLLECTION_NAME = "test_collection"
N_PEERS = 3
RECOVERY_POINT = "/qdrant.CollectionsInternal/GetShardRecoveryPoint"
RECOVER_SNAPSHOT = "/qdrant.ShardSnapshots/Recover"
UPDATE_BATCH = "/qdrant.PointsInternal/UpdateBatch"
RAFT_SEND = "/qdrant.Raft/Send"

def force_delete_peer(peer_api_uri: str, peer_id: int):
    response = requests.delete(
        f"{peer_api_uri}/cluster/peer/{peer_id}?force=true",
        timeout=WAIT_TIME_SEC,
    )
    assert response.status_code == 200, f"Failed to force delete peer: {response.text}"


def get_peer_id(peer_api_uri: str) -> int:
    response = requests.get(f"{peer_api_uri}/cluster", timeout=10)
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


@pytest.fixture
def transfer_cluster(tmp_path: pathlib.Path):
    peer_api_uris, _, _ = start_cluster(tmp_path, N_PEERS, use_peer_proxy=True)

    create_collection(peer_api_uris[0], shard_number=2, replication_factor=3, write_consistency_factor=3)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )

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
    return peer_api_uris, points


@pytest.mark.parametrize("transfer_method", ["snapshot", "wal_delta"])
def test_force_delete_stopped_source_during_transfer(transfer_cluster, transfer_method):
    peer_api_uris, points = transfer_cluster
    source = processes[-1]
    from_peer_id = get_peer_id(peer_api_uris[-1])
    to_peer_id = get_peer_id(peer_api_uris[0])
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

        # Wait for process exit so the held request cannot use the removed source.
        source.kill()
        processes.remove(source)

    assert_survivors_recovered(survivors, from_peer_id, points)


@pytest.mark.parametrize("transfer_method", ["snapshot", "wal_delta"])
def test_force_delete_source_before_late_transfer(transfer_cluster, transfer_method):
    peer_api_uris, points = transfer_cluster
    leader = get_leader(peer_api_uris[0])
    source_index = next(
        index for index in (2, 1) if get_peer_id(peer_api_uris[index]) != leader
    )
    source = processes[source_index]
    source_uri = peer_api_uris[source_index]
    receiver_uri = peer_api_uris[0]
    survivors = [uri for uri in peer_api_uris if uri != source_uri]
    from_peer_id = get_peer_id(source_uri)
    to_peer_id = get_peer_id(receiver_uri)
    proxy = processes[0].proxy

    if transfer_method == "wal_delta":
        # Keep writes possible while the receiver is in Recovery, so its WAL
        # falls behind and the old transfer has actual updates to send.
        assert_http_ok(requests.patch(
            f"{source_uri}/collections/{COLLECTION_NAME}",
            json={"params": {"write_consistency_factor": 2}}, timeout=10,
        ))

    with ExitStack() as gates:
        if transfer_method == "snapshot":
            completion = gates.enter_context(proxy.hold_rpc_response(RECOVER_SNAPSHOT))
            pending = gates.enter_context(proxy.hold_snapshot_download(source_uri, COLLECTION_NAME, 0))
        else:
            pending = gates.enter_context(proxy.hold_rpc(RECOVERY_POINT))

        replicate_shard(source_uri, COLLECTION_NAME, 0, from_peer_id, to_peer_id, method=transfer_method)
        pending.wait_for_request()
        transfer, = get_collection_cluster_info(receiver_uri, COLLECTION_NAME)["shard_transfers"]
        assert (transfer["from"], transfer["to"], transfer["shard_id"], transfer["method"]) == (
            from_peer_id, to_peer_id, 0, transfer_method,
        )

        if transfer_method == "wal_delta":
            for point in points:
                point["payload"]["updated"] = True
            assert_http_ok(requests.put(
                f"{source_uri}/collections/{COLLECTION_NAME}/points?wait=true",
                json={"points": points}, timeout=10,
            ))
            batch = gates.enter_context(proxy.hold_rpc(
                UPDATE_BATCH, matches=lambda request: is_upsert_batch_for(request, COLLECTION_NAME, 0),
            ))
            pending.release()
            batch.wait_for_request()
            pending = batch

        # Isolate the source's Raft traffic in both directions: it must miss
        # removal without disrupting the survivors with election requests.
        assert get_cluster_info(source_uri)["raft_info"]["leader"] != from_peer_id
        gates.enter_context(source.proxy.block_rpc(RAFT_SEND))
        for peer in processes:
            if peer is not source:
                gates.enter_context(peer.proxy.block_rpc(
                    RAFT_SEND, matches=lambda request: decode_raft_message(request).from_peer == from_peer_id,
                ))
        force_delete_peer(receiver_uri, from_peer_id)
        for uri in survivors:
            wait_for(peer_is_removed_from_cluster_and_transfers, uri, from_peer_id)
        assert str(from_peer_id) in get_cluster_info(source_uri)["peers"]
        assert not pending.cancelled.is_set()

        if transfer_method == "wal_delta":
            for uri in survivors:
                wait_for_all_replicas_active(uri, COLLECTION_NAME, min_local_replicas=2)
            # Replaying the old batch must not overwrite newer survivor writes.
            points = [
                {**point, "payload": {**point["payload"], "updated_after_removal": True}}
                for point in points
            ]
            assert_http_ok(requests.put(
                f"{receiver_uri}/collections/{COLLECTION_NAME}/points?wait=true",
                json={"points": points}, timeout=10,
            ))
            assert_survivors_recovered(survivors, from_peer_id, points)
            # Arm this after survivor recovery so its batches cannot take the gate.
            completion = gates.enter_context(proxy.hold_rpc_response(
                UPDATE_BATCH, matches=lambda request: is_upsert_batch_for(request, COLLECTION_NAME, 0),
            ))
        pending.release()
        try:
            completion.wait_for_request()
        except grpc.RpcError as error:
            # Explicit rejection is safe. A transport failure does not prove it.
            assert error.code() in (grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.NOT_FOUND)
        completion.release()

        source.kill()
        processes.remove(source)
    assert_survivors_recovered(survivors, from_peer_id, points)


def assert_survivors_recovered(survivors, from_peer_id, points):
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
