import pathlib
from time import sleep

from .fixtures import create_collection, upsert_random_points
from .utils import *  # includes `time` module, `requests`, `processes`, etc.

# The receiver now pre-clears its shard data on disk before downloading the
# snapshot (for SnapshotPriority::ShardTransfer only). These tests pin down the
# externally-observable behavior of that flow: the cluster keeps serving
# queries while the receiver is in `PartialSnapshot`, and a mid-transfer crash
# of the receiver heals via the dummy-shard → dead → retransfer path.

N_PEERS = 3
N_SHARDS = 3
N_REPLICA = 2
COLLECTION_NAME = "test_collection"


def _count_points(peer_url):
    r = requests.post(
        f"{peer_url}/collections/{COLLECTION_NAME}/points/count",
        json={"exact": True},
    )
    assert_http_ok(r)
    return r.json()["result"]["count"]


def _pick_transfer_endpoints(peer_api_uris):
    src = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
    dst = get_collection_cluster_info(peer_api_uris[2], COLLECTION_NAME)
    dst_shard_ids = {s["shard_id"] for s in dst["local_shards"]}
    # Pick a shard that lives on peer 0 but NOT on peer 2, so replicate_shard
    # actually sends a snapshot across rather than being a no-op.
    src_shard = next(
        s for s in src["local_shards"] if s["shard_id"] not in dst_shard_ids
    )
    return src["peer_id"], dst["peer_id"], src_shard["shard_id"]


def _start_snapshot_replicate(peer_api_uris, from_peer_id, to_peer_id, shard_id):
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/cluster",
        json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": from_peer_id,
                "to_peer_id": to_peer_id,
                "method": "snapshot",
            }
        },
    )
    assert_http_ok(r)


# While a shard snapshot transfer is in progress, the other replicas must keep
# serving read traffic — the receiver's shard is being cleared + refilled on
# disk, but consensus has it in `PartialSnapshot` so requests should route to
# healthy replicas. Verifies the happy path end-to-end with consistency.
def test_cluster_queryable_during_shard_snapshot_transfer(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(
        tmp_path, N_PEERS, 25000
    )

    create_collection(
        peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA
    )
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )

    upsert_random_points(peer_api_uris[0], 3000)
    baseline = _count_points(peer_api_uris[0])
    assert baseline == 3000

    from_peer_id, to_peer_id, shard_id = _pick_transfer_endpoints(peer_api_uris)

    _start_snapshot_replicate(peer_api_uris, from_peer_id, to_peer_id, shard_id)

    # Poll until the transfer is finished, continuously querying every peer.
    # Every count request must succeed and return the full count — with 2
    # replicas per shard, the receiver going through PartialSnapshot must not
    # impact availability.
    saw_transfer_in_progress = False
    deadline = time.time() + 30
    while time.time() < deadline:
        info = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
        transfers = info.get("shard_transfers", [])
        if transfers:
            saw_transfer_in_progress = True
        for uri in peer_api_uris:
            assert _count_points(uri) == baseline
        if not transfers and saw_transfer_in_progress:
            break
        sleep(RETRY_INTERVAL_SEC)

    assert saw_transfer_in_progress, (
        "Expected to observe at least one in-progress transfer"
    )

    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 0)
    wait_for_all_replicas_active(peer_api_uris[0], COLLECTION_NAME)

    for uri in peer_api_uris:
        assert _count_points(uri) == baseline

    # The destination now carries the extra replica — 2 original shards + the
    # replicated one.
    dst_info = get_collection_cluster_info(peer_api_uris[2], COLLECTION_NAME)
    assert len(dst_info["local_shards"]) == 3
    assert any(
        s["shard_id"] == shard_id and s["state"] == "Active"
        for s in dst_info["local_shards"]
    )


# Kill the receiving node while a snapshot transfer is in progress — deliberately
# after the pre-clear has had time to run. On restart the receiver must surface
# the shard as dirty (via the init flag written before clearing), the consensus
# layer must abort the interrupted transfer and recover the replica, and the
# final cluster must be consistent.
def test_receiver_restart_during_shard_snapshot_transfer(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path, N_PEERS, 25500
    )

    create_collection(
        peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA
    )
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )

    # Enough points that the snapshot transfer is not instantaneous, so we
    # have a window to crash the receiver after pre-clear has started.
    upsert_random_points(peer_api_uris[0], 20000, batch_size=500)
    baseline = _count_points(peer_api_uris[0])
    assert baseline == 20000

    from_peer_id, to_peer_id, shard_id = _pick_transfer_endpoints(peer_api_uris)

    # Capture ports/dir of the receiver so we can restart at the same address
    # and keep the same peer id. `start_peer` lays ports out as port+0 (p2p),
    # port+1 (grpc), port+2 (http), so we pass the p2p_port as the seed.
    receiver_proc = processes[2]
    receiver_port = receiver_proc.p2p_port
    receiver_dir = pathlib.Path(peer_dirs[2])
    receiver_url = peer_api_uris[2]

    _start_snapshot_replicate(peer_api_uris, from_peer_id, to_peer_id, shard_id)

    # Wait until the transfer is actually active from the sender's POV, then
    # crash the receiver. This ensures the clear-and-download flow has been
    # entered on the receiver (or is about to be).
    wait_for_collection_shard_transfers_count(peer_api_uris[0], COLLECTION_NAME, 1)
    sleep(1.0)

    receiver_proc.kill()
    processes.remove(receiver_proc)

    # Restart the receiver at the same HTTP/gRPC/p2p ports so it rejoins the
    # cluster with the same peer id.
    restarted_url = start_peer(
        receiver_dir,
        "peer_2_restarted.log",
        bootstrap_uri,
        port=receiver_port,
    )
    assert restarted_url == receiver_url
    wait_for_peer_online(restarted_url)

    # The sender's transfer attempt either completed before the kill or was
    # aborted by it. Either way, the cluster should converge: any stray
    # transfer drains, the dummy-shard-on-restart triggers recovery of the
    # cleared replica, and eventually all replicas end up Active again.
    # Give the cluster a generous budget for this — snapshot recovery on
    # a dirty shard goes through a full retransfer.
    wait_for(
        lambda: get_collection_cluster_info(
            peer_api_uris[0], COLLECTION_NAME
        ).get("shard_transfers", []) == [],
        wait_for_timeout=90,
    )
    wait_for_all_replicas_active(
        peer_api_uris[0], COLLECTION_NAME
    )

    # Data on every live peer (including the restarted one) must match the
    # baseline. The point count check implicitly verifies each peer can route
    # to a healthy replica of every shard.
    for uri in [peer_api_uris[0], peer_api_uris[1], restarted_url]:
        assert _count_points(uri) == baseline, (
            f"Point count mismatch at {uri}"
        )
