"""SetShardReplicaState(Resharding -> Dead) must clear resharding_state on every peer.

Reorder fix in `Collection::set_shard_replica_state`: when this entry deactivates
a `Resharding` replica, the local `abort_resharding` runs *before* the durable
replica_state write. With the old order (persist first, abort after), a crash
between the two left `resharding_state` stuck `Some` on the replaying peer; the
replay's gate read `current_state` back as `Dead` from disk and silently
skipped the abort. With the reorder, that intermediate state is unreachable —
either the abort completes (and resharding_state is cleared) before the persist
runs, or the entry hasn't been "applied" from consensus' point of view and
re-runs from the top on replay.

The test exercises the natural failure-detection path: kill a peer holding a
`Resharding` replica, drive updates that fail on it, and verify every peer's
`resharding_operations` clears — including target after it catches up.
"""

import json
import pathlib

import requests

from .assertions import assert_http_ok
from .fixtures import create_collection, upsert_random_points
from .utils import *

COLLECTION = "test_resharding_set_replica_dead_replay"


def _all_replicas(info):
    for local in info["local_shards"]:
        yield {**local, "peer_id": info["peer_id"]}
    for remote in info["remote_shards"]:
        yield remote


def test_set_replica_dead_clears_resharding_state(tmp_path: pathlib.Path):
    peer_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3)

    target_idx = 2
    target_dir = peer_dirs[target_idx]
    peer_ids = [get_cluster_info(uri)["peer_id"] for uri in peer_uris]
    target_peer_id = peer_ids[target_idx]

    # 2 shards (RF=1). Resharding up adds shard 2 on the target peer in
    # `Resharding` state. We then drive a resharding-stream-records transfer
    # from an existing shard to shard 2 on a non-target peer so shard 2 has
    # *two* `Resharding` replicas — without the second replica, the
    # failure-detection path's "last active peer" guard refuses to propose
    # `SetShardReplicaState(Dead)` and the deactivation never happens.
    create_collection(peer_uris[0], collection=COLLECTION, shard_number=2, replication_factor=1)
    wait_collection_exists_and_active_on_all_peers(COLLECTION, peer_uris)

    new_shard_dir = target_dir / "storage" / "collections" / COLLECTION / "2"
    new_replica_state = new_shard_dir / "replica_state.json"

    resp = requests.post(
        f"{peer_uris[0]}/collections/{COLLECTION}/cluster",
        json={"start_resharding": {"direction": "up", "peer_id": target_peer_id, "shard_key": None}},
    )
    assert_http_ok(resp)

    for uri in peer_uris:
        wait_for_collection_resharding_operations_count(uri, COLLECTION, 1)
    wait_for(lambda: new_replica_state.exists())
    wait_for(
        lambda: json.loads(new_replica_state.read_text())["peers"].get(str(target_peer_id))
        == "Resharding"
    )

    # Pick a transfer source on a non-target peer (auto-distribution may put
    # shard 0 on the target peer). Stream from that peer's existing shard
    # into shard 2 on the third peer, giving shard 2 a second Resharding
    # replica there.
    info = get_collection_cluster_info(peer_uris[0], COLLECTION)
    source_idx = source_peer_id = source_shard = None
    for r in _all_replicas(info):
        replica_idx = peer_ids.index(r["peer_id"])
        if replica_idx == target_idx:
            continue
        source_idx = replica_idx
        source_peer_id = r["peer_id"]
        source_shard = r["shard_id"]
        break
    assert source_idx is not None, "no non-target peer holds an existing shard"
    extra_idx = next(i for i in range(3) if i != target_idx and i != source_idx)
    extra_peer_id = peer_ids[extra_idx]
    driver_uri = peer_uris[source_idx]

    resp = requests.post(
        f"{peer_uris[0]}/collections/{COLLECTION}/cluster",
        json={
            "replicate_shard": {
                "from_peer_id": source_peer_id,
                "to_peer_id": extra_peer_id,
                "shard_id": source_shard,
                "to_shard_id": 2,
                "method": "resharding_stream_records",
            }
        },
    )
    assert_http_ok(resp)
    wait_for_collection_shard_transfers_count(peer_uris[0], COLLECTION, 0)

    # Kill target. Updates routed to shard 2 will fail on target's Resharding
    # replica; the second peer's Resharding replica keeps the shard "live"
    # in active_or_resharding_peers so the failure handler is permitted to
    # propose `SetShardReplicaState(Resharding -> Dead)` for target.
    p = processes.pop(target_idx)
    restart_port = p.p2p_port
    p.kill()

    # Running peers apply `SetShardReplicaState(Dead)`; the local
    # `abort_resharding` runs first (per the reorder), clearing
    # `resharding_state` before the replica state write.
    #
    # With weak ordering the upsert returns as soon as the local replica
    # acks, leaving the fan-out failure to target's dead replica to land
    # asynchronously — a single batch sometimes returns 200 without ever
    # observing the dead target. Drive a fresh batch on each poll until
    # every running peer has applied the deactivation and cleared its
    # `resharding_state`.
    #
    # Cap each upsert at 5s so a stuck gRPC channel to the dead peer can't
    # block a single iteration past the wait_for budget — without this,
    # `requests.put` has no timeout and a hung fan-out leaves wait_for
    # unable to iterate at all (test hangs until pytest's outer deadline
    # rather than failing within 60s).
    running_uris = [uri for idx, uri in enumerate(peer_uris) if idx != target_idx]

    def all_running_peers_aborted_resharding() -> bool:
        try:
            upsert_random_points(
                driver_uri,
                num=100,
                collection_name=COLLECTION,
                fail_on_error=False,
                ordering="weak",
                timeout=5,
            )
        except requests.RequestException:
            pass
        return all(
            not get_collection_cluster_info(uri, COLLECTION).get("resharding_operations")
            for uri in running_uris
        )

    wait_for(
        all_running_peers_aborted_resharding,
        wait_for_timeout=60,
        wait_for_interval=1,
    )

    # Restart target. It catches up via raft, applies the same entry, and
    # the same reorder makes it clear `resharding_state` and drop shard 2
    # on this peer too.
    peer_uris[target_idx] = start_peer(
        target_dir, f"peer_catchup_{target_idx}.log", bootstrap_uri, port=restart_port
    )
    catchup_uri = peer_uris[target_idx]
    wait_for_peer_online(catchup_uri)
    wait_for_collection_resharding_operations_count(catchup_uri, COLLECTION, 0)

    # All peers must agree — no divergence in `resharding_operations`.
    for uri in peer_uris:
        info = get_collection_cluster_info(uri, COLLECTION)
        assert not info.get("resharding_operations"), (
            f"Resharding state divergence on peer {uri}: "
            f"resharding_operations={info.get('resharding_operations')!r}"
        )
