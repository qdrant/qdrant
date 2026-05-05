"""SetShardReplicaState(ReshardingScaleDown -> Dead) replay must clear resharding_state.

Down-direction counterpart to test_resharding_set_replica_dead_replay.py.

`Collection::set_shard_replica_state` captures `current_state` BEFORE the durable
`replica_state.write` and uses it to gate the local `abort_resharding` side-effect
at the bottom of the function. On replay (after a crash that persisted the new
replica state but didn't run the side-effect), `current_state` reads back from
disk as Dead and `is_resharding(Dead)` evaluates false — so the local
abort_resharding is silently skipped. resharding_state.json then stays stale on
the replaying peer while peers that completed a normal apply have it cleared.

`ReplicaState::is_resharding()` returns true for both `Resharding` (up) and
`ReshardingScaleDown` (down), so the same gate misfires in either direction.
The Down case is harder to set up because replicas stay `Active` until a
resharding-stream-records transfer transitions them — this test drives that
transfer first to land a replica in `ReshardingScaleDown`.
"""

import json
import pathlib
import time

import requests

from .assertions import assert_http_ok
from .fixtures import create_collection, upsert_random_points
from .utils import (
    get_collection_cluster_info,
    get_cluster_info,
    processes,
    start_cluster,
    start_peer,
    wait_collection_exists_and_active_on_all_peers,
    wait_for,
    wait_for_collection_resharding_operations_count,
    wait_for_collection_shard_transfers_count,
    wait_for_peer_online,
)

COLLECTION = "test_resharding_set_replica_dead_replay_down"


def _all_replicas(info):
    for local in info["local_shards"]:
        yield {**local, "peer_id": info["peer_id"]}
    for remote in info["remote_shards"]:
        yield remote


def _peers_for_shard(info, shard_id):
    return [r["peer_id"] for r in _all_replicas(info) if r["shard_id"] == shard_id]


def test_set_replica_dead_replay_clears_resharding_state_down(tmp_path: pathlib.Path):
    peer_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3)

    peer_ids = [get_cluster_info(uri)["peer_id"] for uri in peer_uris]

    # 2 shards with replication_factor=2 — the surviving shard (shard 0) needs
    # a replica on at least two peers so the last-source-of-truth check
    # doesn't block deactivating one of them. The dying shard (shard 1) needs
    # a replica on a peer different from the resharding-down receiver so we
    # have a transfer source.
    create_collection(
        peer_uris[0],
        collection=COLLECTION,
        shard_number=2,
        replication_factor=2,
        # write_consistency_factor=2 forces upserts to wait for both replicas
        # of each shard. Once we kill the target, fan-out to its replica
        # times out — the request fails synchronously and the failure handler
        # proposes `SetShardReplicaState(ReshardingScaleDown -> Dead)`.
        write_consistency_factor=2,
    )
    wait_collection_exists_and_active_on_all_peers(COLLECTION, peer_uris)

    # Auto-distribution may place shards anywhere, so look up the actual
    # placement and pick a target peer that holds a replica of the surviving
    # shard 0 (it'll become the resharding-down receiver) and a separate
    # source peer that holds a replica of the dying shard 1.
    info = get_collection_cluster_info(peer_uris[0], COLLECTION)
    surviving_shard = 0
    dying_shard = 1

    surviving_peers = _peers_for_shard(info, surviving_shard)
    dying_peers = _peers_for_shard(info, dying_shard)
    assert len(surviving_peers) >= 2, surviving_peers
    assert len(dying_peers) >= 1, dying_peers

    # Pick a target that has shard 0, and a transfer source on a different
    # peer that has shard 1. Prefer killing a non-leader so peer 0 stays
    # alive to drive the test.
    target_peer_id = next(p for p in surviving_peers if p != peer_ids[0])
    target_idx = peer_ids.index(target_peer_id)
    source_peer_id = next(
        p for p in dying_peers if p != target_peer_id
    )
    driver_uri = peer_uris[0] if target_idx != 0 else peer_uris[1]

    target_dir = peer_dirs[target_idx]
    raft_state = target_dir / "storage" / "raft_state.json"
    resharding_state = (
        target_dir / "storage" / "collections" / COLLECTION / "resharding_state.json"
    )
    surviving_shard_dir = (
        target_dir / "storage" / "collections" / COLLECTION / str(surviving_shard)
    )
    surviving_replica_state = surviving_shard_dir / "replica_state.json"

    # Insert points so the resharding-stream-records transfer has data to move.
    upsert_random_points(peer_uris[0], num=200, collection_name=COLLECTION)

    # Trigger resharding down. peer_id selects the driver — pick the source
    # peer (which holds the dying shard) so the transfer flows naturally
    # without remote operations.
    resp = requests.post(
        f"{peer_uris[0]}/collections/{COLLECTION}/cluster",
        json={
            "start_resharding": {
                "direction": "down",
                "peer_id": source_peer_id,
                "shard_key": None,
            }
        },
    )
    assert_http_ok(resp)

    for uri in peer_uris:
        wait_for_collection_resharding_operations_count(uri, COLLECTION, 1)

    # Drive the resharding-stream-records transfer from the dying shard to
    # the surviving shard's replica on target. Wait for the transfer to
    # finish (transfer count back to 0) — at which point target's replica
    # has transitioned through to `ReshardingScaleDown` and stays there
    # until `finish_migrating_points` is called (which we deliberately do
    # not call).
    resp = requests.post(
        f"{peer_uris[0]}/collections/{COLLECTION}/cluster",
        json={
            "replicate_shard": {
                "from_peer_id": source_peer_id,
                "to_peer_id": target_peer_id,
                "shard_id": dying_shard,
                "to_shard_id": surviving_shard,
                "method": "resharding_stream_records",
            }
        },
    )
    assert_http_ok(resp)
    wait_for_collection_shard_transfers_count(peer_uris[0], COLLECTION, 0)

    # Confirm target's replica of the surviving shard is in
    # `ReshardingScaleDown`. This is the source-of-truth state we need for
    # the bug: SetShardReplicaState(ReshardingScaleDown -> Dead) is what
    # triggers the local abort_resharding side-effect that the replay path
    # incorrectly skips.
    wait_for(
        lambda: surviving_replica_state.exists()
        and json.loads(surviving_replica_state.read_text())["peers"].get(
            str(target_peer_id)
        )
        == "ReshardingScaleDown"
    )

    # Snapshot the active resharding key so we can forge it back later.
    initial_resharding_state = json.loads(resharding_state.read_text())
    assert initial_resharding_state is not None

    commit_before_set = json.loads(raft_state.read_text())["state"]["hard_state"]["commit"]

    # Kill target peer. The other replica of the surviving shard is still
    # alive, so the last-source-of-truth check at mod.rs:448 passes and
    # `SetShardReplicaState(ReshardingScaleDown -> Dead)` actually mutates
    # state on the running peers.
    p = processes.pop(target_idx)
    restart_port = p.p2p_port
    p.kill()

    # Drive updates to trigger the deactivation. With
    # write_consistency_factor=2, the upsert waits for both replicas of each
    # shard, so when target is dead the request fails synchronously and
    # the failure handler observes the failed remote ack.
    upsert_random_points(
        driver_uri,
        num=200,
        collection_name=COLLECTION,
        fail_on_error=False,
        ordering="weak",
    )

    running_uris = [uri for idx, uri in enumerate(peer_uris) if idx != target_idx]
    for uri in running_uris:
        wait_for_collection_resharding_operations_count(uri, COLLECTION, 0)

    # Natural catchup: target restarts, leader replicates the
    # SetShardReplicaState entry, target applies it normally — the local
    # abort_resharding side-effect fires because `current_state` is
    # captured as `ReshardingScaleDown` before the persist runs.
    peer_uris[target_idx] = start_peer(
        target_dir, f"peer_catchup_{target_idx}.log", bootstrap_uri, port=restart_port
    )
    catchup_uri = peer_uris[target_idx]
    wait_for_peer_online(catchup_uri)
    wait_for_collection_resharding_operations_count(catchup_uri, COLLECTION, 0)

    # Sanity-check: resharding cleared on target after natural catchup.
    assert json.loads(resharding_state.read_text()) is None

    # Forge the partial-apply state. Restore resharding_state.json to the
    # active resharding (as it would look mid-apply between the durable
    # replica_state.write and the local abort_resharding) and reset
    # apply_progress_queue to force the SetShardReplicaState entry to
    # replay. Replica state stays Dead — that's the trap that makes
    # `current_state` read back as Dead on replay.
    p = processes.pop(target_idx)
    restart_port = p.p2p_port
    p.kill()

    # Pause to let the killed process fully release WAL file locks before
    # the next start. SIGKILL + wait() returns when the process exits, but
    # background runtimes (e.g. transfer tasks the peer was driving) and
    # the OS lock state may take a few seconds to clean up; without this
    # pause the next peer hits `Can't open consensus WAL: WouldBlock`.
    time.sleep(5)

    # Forge resharding_state back to active.
    resharding_state.write_text(json.dumps(initial_resharding_state))

    # Forge target's replica state for the surviving shard to Dead. After
    # the natural catchup, the cluster's automatic recovery flow may have
    # transitioned target's replica from Dead through Recovery/Partial back
    # toward Active. For the replay to exercise the bug, we need
    # `current_state == Dead` so the idempotent_replay path is taken; if
    # we left current_state as whatever the recovery flow produced
    # (typically Active), the from_state validation would mismatch
    # (`from_state=Some(ReshardingScaleDown)` vs `current_state=Some(Active)`),
    # the entry would error with BadInput, and apply_entries would swallow
    # the error without ever reaching the abort_resharding side-effect.
    repl_state = json.loads(surviving_replica_state.read_text())
    repl_state["peers"][str(target_peer_id)] = "Dead"
    surviving_replica_state.write_text(json.dumps(repl_state))

    state = json.loads(raft_state.read_text())
    state["apply_progress_queue"] = [
        commit_before_set + 1,
        state["state"]["hard_state"]["commit"],
    ]
    raft_state.write_text(json.dumps(state))

    peer_uris[target_idx] = start_peer(
        target_dir, f"peer_replay_{target_idx}.log", bootstrap_uri, port=restart_port
    )
    replay_uri = peer_uris[target_idx]
    wait_for_peer_online(replay_uri)

    # Post-fix: the replaying peer reconciles its forged resharding_state
    # to None, matching the rest of the cluster.
    #
    # Pre-fix bug: `is_resharding = current_state.is_resharding()` evaluates
    # false because current_state was reloaded from replica_state.json as
    # Dead, so the local abort_resharding is skipped on replay.
    # resharding_state.json stays Some on this peer while running peers
    # have it cleared, and `resharding_operations` in the cluster info
    # diverges.
    wait_for_collection_resharding_operations_count(replay_uri, COLLECTION, 0)

    for uri in peer_uris:
        info = get_collection_cluster_info(uri, COLLECTION)
        assert not info.get("resharding_operations"), (
            f"Resharding state divergence on peer {uri}: "
            f"resharding_operations={info.get('resharding_operations')!r}"
        )
