"""SetShardReplicaState(ReshardingScaleDown -> Dead) replay must clear resharding_state.

Down-direction counterpart to test_resharding_set_replica_dead_replay.py.

The local `abort_resharding` side-effect inside `Collection::set_shard_replica_state`
is gated on `current_state.is_resharding()` — but `current_state` is captured BEFORE
`ensure_replica_with_state` persists the new state. On replay (after a crash that
persisted the new replica state but didn't run the side-effect), `current_state`
reads back as Dead from disk, so the gate evaluates false and the local
abort_resharding is silently skipped. resharding_state.json then remains stale on
the replaying peer while peers that completed a normal apply have it cleared.

`ReplicaState::is_resharding()` returns true for both `Resharding` (up) and
`ReshardingScaleDown` (down), so the same gate misfires in either direction.
"""

import json
import pathlib

import requests

from .assertions import assert_http_ok
from .fixtures import create_collection, upsert_random_points
from .utils import (
    get_collection_cluster_info,
    processes,
    start_cluster,
    start_peer,
    wait_collection_exists_and_active_on_all_peers,
    wait_for,
    wait_for_collection_resharding_operations_count,
    wait_for_peer_online,
)

COLLECTION = "test_resharding_set_replica_dead_replay_down"


def test_set_replica_dead_replay_clears_resharding_state_down(tmp_path: pathlib.Path):
    peer_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3)

    # 2 shards with replication_factor=2 — the last shard (the one Down
    # resharding will remove) has 2 replicas spread across two peers. We need
    # at least one OTHER active replica to remain alive when we kill the
    # target, otherwise the last-source-of-truth check in
    # `Collection::set_shard_replica_state` would block the deactivation
    # (`ReshardingScaleDown` counts as active for that check).
    create_collection(
        peer_uris[0],
        collection=COLLECTION,
        shard_number=2,
        replication_factor=2,
    )
    wait_collection_exists_and_active_on_all_peers(COLLECTION, peer_uris)

    # Down resharding always removes the shard with the highest id.
    target_shard_id = 1

    # Pick a target peer that actually holds a replica of the shard being
    # removed. Iterate from the back so we prefer killing a non-leader peer.
    target_peer_id = None
    target_idx = None
    for idx in range(len(peer_uris) - 1, -1, -1):
        info = get_collection_cluster_info(peer_uris[idx], COLLECTION)
        if target_shard_id in {s["shard_id"] for s in info["local_shards"]}:
            target_peer_id = info["peer_id"]
            target_idx = idx
            break
    assert target_peer_id is not None and target_idx is not None, (
        f"no peer holds shard {target_shard_id} replica — distribution unexpected"
    )

    # We send updates from a peer that isn't the one we'll kill.
    driver_idx = next(idx for idx in range(len(peer_uris)) if idx != target_idx)

    target_dir = peer_dirs[target_idx]
    raft_state = target_dir / "storage" / "raft_state.json"
    resharding_state = (
        target_dir / "storage" / "collections" / COLLECTION / "resharding_state.json"
    )
    target_shard_dir = (
        target_dir / "storage" / "collections" / COLLECTION / str(target_shard_id)
    )
    target_replica_state = target_shard_dir / "replica_state.json"

    # Trigger resharding down: removes the last shard. peer_id selects which
    # peer drives — pick the one we'll kill so the resharding key matches the
    # replica we'll deactivate.
    resp = requests.post(
        f"{peer_uris[0]}/collections/{COLLECTION}/cluster",
        json={
            "start_resharding": {
                "direction": "down",
                "peer_id": target_peer_id,
                "shard_key": None,
            }
        },
    )
    assert_http_ok(resp)

    # Wait for resharding to be observable on every peer and target's replica
    # to be in `ReshardingScaleDown` (the source-of-truth state for Down).
    for uri in peer_uris:
        wait_for_collection_resharding_operations_count(uri, COLLECTION, 1)
    wait_for(lambda: target_replica_state.exists())
    wait_for(
        lambda: json.loads(target_replica_state.read_text())["peers"].get(
            str(target_peer_id)
        )
        == "ReshardingScaleDown"
    )

    # Snapshot the on-disk state of the resharding key — we'll forge it back in
    # later to mimic a crash between `ensure_replica_with_state` (which durably
    # persisted the new replica state) and the local `abort_resharding`
    # side-effect.
    initial_resharding_state = json.loads(resharding_state.read_text())
    assert initial_resharding_state is not None, "resharding_state.json must hold Some(...)"

    commit_before_set = json.loads(raft_state.read_text())["state"]["hard_state"]["commit"]

    # Kill target peer. Background fan-out updates to shard 1 will fail on its
    # `ReshardingScaleDown` replica and the failure handler proposes
    # `SetShardReplicaState(ReshardingScaleDown -> Dead)`. The other replica of
    # shard 1 is still alive, so the last-source-of-truth check passes and the
    # entry actually mutates state on the running peers.
    p = processes.pop(target_idx)
    restart_port = p.p2p_port
    p.kill()

    upsert_random_points(
        peer_uris[driver_idx],
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
    # abort_resharding side-effect fires because `current_state` is captured as
    # `ReshardingScaleDown` before the persist runs.
    peer_uris[target_idx] = start_peer(
        target_dir, f"peer_catchup_{target_idx}.log", bootstrap_uri, port=restart_port
    )
    catchup_uri = peer_uris[target_idx]
    wait_for_peer_online(catchup_uri)
    wait_for_collection_resharding_operations_count(catchup_uri, COLLECTION, 0)

    # Sanity-check the persisted state on target after the natural catchup.
    # `Collection::finish_resharding` for Down drops the removed shard, so the
    # `target_shard_dir` may already be gone here — only the resharding_state
    # file is guaranteed to exist.
    assert json.loads(resharding_state.read_text()) is None

    # Forge the partial-apply state. Restore resharding_state.json to the
    # active resharding (as it would look mid-apply between the durable
    # replica_state.write and the local abort_resharding) and reset
    # apply_progress_queue to force the SetShardReplicaState entry to replay.
    p = processes.pop(target_idx)
    restart_port = p.p2p_port
    p.kill()

    resharding_state.write_text(json.dumps(initial_resharding_state))

    state = json.loads(raft_state.read_text())
    state["apply_progress_queue"] = [
        commit_before_set + 1,
        state["state"]["hard_state"]["commit"],
    ]
    raft_state.write_text(json.dumps(state))

    # Restart and watch the replay.
    peer_uris[target_idx] = start_peer(
        target_dir, f"peer_replay_{target_idx}.log", bootstrap_uri, port=restart_port
    )
    replay_uri = peer_uris[target_idx]
    wait_for_peer_online(replay_uri)

    # Post-fix expectation: the replaying peer reconciles its forged
    # resharding_state to None, matching the rest of the cluster.
    #
    # Pre-fix bug: `is_resharding = current_state.is_resharding()` evaluates
    # false because current_state was reloaded from replica_state.json as Dead,
    # so the local `abort_resharding` is skipped on replay.
    # resharding_state.json stays Some on this peer while the running peers
    # have it cleared, and `resharding_operations` in the cluster info
    # diverges.
    wait_for_collection_resharding_operations_count(replay_uri, COLLECTION, 0)

    # All peers must agree — no divergence in resharding_operations.
    for uri in peer_uris:
        info = get_collection_cluster_info(uri, COLLECTION)
        assert not info.get("resharding_operations"), (
            f"Resharding state divergence on peer {uri}: "
            f"resharding_operations={info.get('resharding_operations')!r}"
        )
