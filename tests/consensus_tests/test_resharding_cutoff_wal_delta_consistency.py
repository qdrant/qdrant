"""A resharding stream sets the target replica's cutoff from the source shard's clocks.
After that, a WAL delta recovery of the target must still bring back every write it missed.
"""

import pathlib

from .fixtures import create_collection, random_dense_vector
from .test_resharding import (
    abort_resharding,
    all_replicas,
    get_local_points,
    scroll_local_points,
    start_resharding,
)
from .utils import *

COLLECTION = "test_resharding_cutoff_wal_delta"

ENV = {
    # A freshly restarted peer does not know peer versions yet and would pick a snapshot transfer
    "QDRANT__STORAGE__SHARD_TRANSFER_METHOD": "wal_delta",
    # Clocks only reach disk on a flush
    "QDRANT__STORAGE__OPTIMIZERS__FLUSH_INTERVAL_SEC": "1",
}
FLUSH_WAIT_SEC = 3


def _upsert(uri: str, point_id: int, n: int) -> requests.Response:
    return requests.put(f"{uri}/collections/{COLLECTION}/points?wait=true", json={
        "points": [{"id": point_id, "vector": random_dense_vector(), "payload": {"n": n}}],
    })


def _clock_tick(uri: str, shard_id: int, peer_id: int) -> int:
    resp = requests.get(f"{uri}/collections/{COLLECTION}/shards/{shard_id}/recovery_point")
    assert_http_ok(resp)
    ticks = [
        tag["clock_tick"] for tag in resp.json()["result"]
        if tag["peer_id"] == peer_id and tag["clock_id"] == 0
    ]
    return max(ticks, default=0)


def test_resharding_cutoff_does_not_break_wal_delta_recovery(tmp_path: pathlib.Path):
    assert_project_root()

    peer_uris, peer_dirs, _ = start_cluster(tmp_path, 3, extra_env=ENV)
    skip_if_no_feature(peer_uris[0], "staging")
    wait_for(all_peers_are_voters, peer_uris)
    peer_ids = [get_cluster_info(uri)["peer_id"] for uri in peer_uris]
    peer_procs = list(processes)  # aligned to peer index; `processes` is mutated below

    def kill_target(port: int | None = None) -> int:
        proc = peer_procs[target] if port is None else next(p for p in processes if p.p2p_port == port)
        processes.remove(proc)
        proc.kill()
        return proc.p2p_port

    create_collection(
        peer_uris[0], collection=COLLECTION, shard_number=2, replication_factor=2, sparse_vectors=False,
    )
    wait_collection_exists_and_active_on_all_peers(COLLECTION, peer_uris)

    # Shard 0 has a `target` and a `source` replica. Shard 1 gets streamed into the target.
    info = get_collection_cluster_info(peer_uris[0], COLLECTION)
    holders = lambda shard_id: [r["peer_id"] for r in all_replicas(info) if r["shard_id"] == shard_id]
    target_id, source_id = holders(0)
    stream_from_id = next(peer_id for peer_id in holders(1) if peer_id != target_id)
    target = peer_ids.index(target_id)
    source_uri = peer_uris[peer_ids.index(source_id)]
    stream_from_uri = peer_uris[peer_ids.index(stream_from_id)]

    # All writes go through the source peer, so both shards use its clock 0.
    # Write until both shards hold a point.
    point_id = 0
    while True:
        assert_http_ok(_upsert(source_uri, point_id, 0))
        point_id += 1
        shard_0_points = scroll_local_points(source_uri, 0, collection=COLLECTION)["points"]
        shard_1_points = scroll_local_points(stream_from_uri, 1, collection=COLLECTION)["points"]
        if shard_0_points and shard_1_points:
            break
    shard_0_id = shard_0_points[0]["id"]
    shard_1_id = shard_1_points[0]["id"]

    # Shard 1's clock runs well ahead of shard 0's
    for n in range(50):
        assert_http_ok(_upsert(source_uri, shard_1_id, n))

    shard_0_tick = _clock_tick(source_uri, 0, source_id)
    shard_1_tick = _clock_tick(stream_from_uri, 1, source_id)
    assert shard_1_tick >= shard_0_tick + 40, (
        f"setup: shard 1 clock ({shard_1_tick}) is not ahead of shard 0 clock ({shard_0_tick})"
    )

    # Resharding down streams shard 1 into the target replica of shard 0, then gets aborted
    assert_http_ok(start_resharding(source_uri, COLLECTION, direction="down"))
    for uri in peer_uris:
        wait_for_collection_resharding_operations_count(uri, COLLECTION, 1)

    resp = requests.post(f"{source_uri}/collections/{COLLECTION}/cluster", json={
        "replicate_shard": {
            "from_peer_id": stream_from_id, "to_peer_id": target_id,
            "shard_id": 1, "to_shard_id": 0,
            "method": "resharding_stream_records",
        }
    })
    assert_http_ok(resp)
    for uri in peer_uris:
        wait_for_collection_shard_transfers_count(uri, COLLECTION, 0)

    assert_http_ok(abort_resharding(source_uri, COLLECTION))
    for uri in peer_uris:
        wait_for_collection_resharding_operations_count(uri, COLLECTION, 0)
        wait_for_all_replicas_active(uri, COLLECTION)

    # Restart the target once with no writes in between, so its tick is the one it has on disk
    target_tick_in_memory = _clock_tick(peer_uris[target], 0, source_id)
    time.sleep(FLUSH_WAIT_SEC)

    target_port = kill_target()
    peer_uris[target] = start_peer(
        peer_dirs[target], f"peer_restart_{target}_0.log", source_uri, port=target_port, extra_env=ENV,
    )
    wait_for_peer_online(peer_uris[target])
    wait_for_all_replicas_active(source_uri, COLLECTION)

    target_tick = _clock_tick(peer_uris[target], 0, source_id)
    source_tick = _clock_tick(source_uri, 0, source_id)
    print(f"shard 0 clock tick before the outage: target {target_tick}, source {source_tick}")
    assert target_tick >= target_tick_in_memory, (
        f"setup: target clocks were not flushed ({target_tick} on disk, {target_tick_in_memory} in memory)"
    )

    # Target goes down and misses writes to shard 0
    target_port = kill_target(target_port)

    _upsert(source_uri, shard_0_id, 1)  # may fail while the target gets deactivated
    wait_for(check_some_replicas_not_active, source_uri, COLLECTION)

    n = 1
    while _clock_tick(source_uri, 0, source_id) < max(target_tick, source_tick + 5):
        n += 1
        assert_http_ok(_upsert(source_uri, shard_0_id, n))

    # Target comes back and recovers shard 0 from the source
    peer_uris[target] = start_peer(
        peer_dirs[target], f"peer_restart_{target}_1.log", source_uri, port=target_port, extra_env=ENV,
    )
    wait_for_peer_online(peer_uris[target])
    wait_for_all_replicas_active(source_uri, COLLECTION)
    wait_for_collection_shard_transfers_count(source_uri, COLLECTION, 0)

    [source_point] = get_local_points(source_uri, 0, [shard_0_id], collection=COLLECTION)
    [target_point] = get_local_points(peer_uris[target], 0, [shard_0_id], collection=COLLECTION)
    assert target_point["payload"] == source_point["payload"], (
        f"target replica is stale after recovery: {target_point['payload']} != {source_point['payload']} "
        f"(target clock tick {target_tick}, source clock tick {source_tick} before the outage)"
    )
