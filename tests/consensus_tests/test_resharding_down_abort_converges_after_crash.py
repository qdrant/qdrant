"""abort_resharding clears (and persists) resharding_state before aborting the related
transfer. A peer killed in that window (widened by a debug sleep) — state already
cleared, transfer still present — replays the abort and converges, because the
persisted cleared state makes the replay idempotent. Asserts every peer converges on
no resharding_operations.
"""

import pathlib
import threading

from .fixtures import create_collection, upsert_random_points
from .test_resharding import all_replicas, start_resharding
from .utils import *

COLLECTION = "test_resharding_down_abort_converges_after_crash"


def _resharding_transfer_source(info) -> int | None:
    for t in info.get("shard_transfers", []):
        if "resharding" in (t.get("method") or ""):
            return t["from"]
    return None


def test_resharding_down_abort_converges_when_killed_mid_abort(tmp_path: pathlib.Path):
    env = {"QDRANT_STAGING_RESHARDING_ABORT_DELAY_SEC": "12"}
    peer_uris, peer_dirs, _ = start_cluster(tmp_path, 3, extra_env=env)
    skip_if_no_feature(peer_uris[0], "staging")
    wait_for(all_peers_are_voters, peer_uris)
    peer_ids = [get_cluster_info(uri)["peer_id"] for uri in peer_uris]
    peer_procs = list(processes)  # aligned to peer index; `processes` is mutated below

    create_collection(peer_uris[0], collection=COLLECTION, shard_number=3, replication_factor=2)
    wait_collection_exists_and_active_on_all_peers(COLLECTION, peer_uris)
    # Batch the load: a single large `wait=true` upsert keeps a replica busy past
    # the hardcoded 2s inter-node health-check under CI load, which fails the
    # write with a transient "Healthcheck timeout 2000ms exceeded" before
    # resharding even starts. 1000-point ops stay well under that threshold.
    upsert_random_points(peer_uris[0], num=20_000, collection_name=COLLECTION, batch_size=1_000)

    resp = start_resharding(peer_uris[0], COLLECTION, direction="down", shard_key=None)
    assert_http_ok(resp)
    for uri in peer_uris:
        wait_for_collection_resharding_operations_count(uri, COLLECTION, 1)

    # Driver is disabled in tests; start the migrate transfer ourselves (dropped
    # shard 2 -> surviving shard 0, between two distinct peers).
    info = get_collection_cluster_info(peer_uris[0], COLLECTION)
    target_shard_id, surviving_shard_id = 2, 0
    target_peers = [r["peer_id"] for r in all_replicas(info) if r["shard_id"] == target_shard_id]
    surviving_peers = [r["peer_id"] for r in all_replicas(info) if r["shard_id"] == surviving_shard_id]
    from_peer_id, to_peer_id = next(
        (fp, tp) for fp in target_peers for tp in surviving_peers if fp != tp
    )

    resp = requests.post(f"{peer_uris[0]}/collections/{COLLECTION}/cluster", json={
        "replicate_shard": {
            "from_peer_id": from_peer_id, "to_peer_id": to_peer_id,
            "shard_id": target_shard_id, "to_shard_id": surviving_shard_id,
            "method": "resharding_stream_records",
        }
    })
    assert_http_ok(resp)

    victim_idx = peer_ids.index(to_peer_id)
    alive_uri = peer_uris[next(i for i in range(len(peer_uris)) if i != victim_idx)]
    victim_uri = peer_uris[victim_idx]

    # Wait until BOTH the abort target and the victim have applied the transfer
    # start. alive_uri must have it: the abort_transfer request below checks
    # `check_transfer_exists` against alive_uri's *local* state and returns 404 if
    # the transfer-start hasn't replicated there yet — and the fire-and-forget
    # thread swallows that 404, so the transfer would just complete naturally and
    # the abort window would never open. The victim must have it so that a later
    # "gone" means the abort window, not a peer that simply hasn't applied the
    # transfer start yet.
    def _both_have_transfer() -> bool:
        return all(
            _resharding_transfer_source(get_collection_cluster_info(uri, COLLECTION)) == from_peer_id
            for uri in (alive_uri, victim_uri)
        )

    wait_for(_both_have_transfer, wait_for_interval=0.05)

    # Fire-and-forget: the debug sleep makes the synchronous abort wait time out, but
    # the op still commits and applies on every peer.
    def _abort_transfer():
        try:
            requests.post(f"{alive_uri}/collections/{COLLECTION}/cluster", json={
                "abort_transfer": {
                    "shard_id": target_shard_id, "to_shard_id": surviving_shard_id,
                    "from_peer_id": from_peer_id, "to_peer_id": to_peer_id,
                }
            }, timeout=60)
        except requests.RequestException:
            pass

    threading.Thread(target=_abort_transfer, daemon=True).start()

    # Catch the victim paused mid-abort (resharding_state already cleared, transfer
    # still present) and kill it before it aborts the transfer. Quorum (2/3) is preserved.
    def _victim_in_window() -> bool:
        try:
            vinfo = get_collection_cluster_info(victim_uri, COLLECTION)
        except requests.RequestException:
            return False
        return not vinfo.get("resharding_operations") and _resharding_transfer_source(vinfo) is not None

    wait_for(_victim_in_window, wait_for_timeout=30)
    victim_proc = peer_procs[victim_idx]
    victim_port = victim_proc.p2p_port
    processes.remove(victim_proc)
    victim_proc.kill()

    # Restart (liveness only — the stuck shard may never pass /readyz). The victim
    # replays the abort against an already-cleared, persisted resharding_state and
    # converges.
    peer_uris[victim_idx] = start_peer(
        peer_dirs[victim_idx], f"peer_restart_{victim_idx}.log", alive_uri, port=victim_port,
    )
    wait_for_peer_online(peer_uris[victim_idx], path="/healthz", wait_for_timeout=60)

    try:
        for uri in peer_uris:
            wait_for_collection_resharding_operations_count(uri, COLLECTION, 0, wait_for_timeout=30)
    except Exception:
        stuck = {u: n for u in peer_uris if (n := len(get_collection_cluster_info(u, COLLECTION).get("resharding_operations") or []))}
        raise AssertionError(f"peers still resharding after 30s: {stuck}")
