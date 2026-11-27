# Copy to qdrant's tests/consensus_tests/, then from the repo root:
#   cargo build --bin qdrant --features staging
#   uv --project tests run pytest tests/consensus_tests/test_consensus_reshard_down_migrate_into_copying_replica.py
# Fails while the bug exists, passes once it's fixed.
"""A down-resharding transfer into a replica that is still the target of a shard copy overwrites its Partial state
with ReshardingScaleDown, so when the copy's source fails, the shard is left without any Active replica.
"""

import pathlib

from .fixtures import create_collection, upsert_random_points
from .test_resharding import all_replicas, start_resharding
from .utils import *

COLLECTION = "test_collection"
ENV = {"QDRANT__CLUSTER__RESHARDING_ENABLED": "true"}
SERVING_STATES = ("Active", "ReshardingScaleDown", "Resharding", "ActiveRead")


def shard_states(uri: str, shard_id: int) -> dict:
    info = get_collection_cluster_info(uri, COLLECTION)
    return {r["peer_id"]: r["state"] for r in all_replicas(info) if r["shard_id"] == shard_id}


def transfers(uri: str) -> list:
    return get_collection_cluster_info(uri, COLLECTION)["shard_transfers"]


def is_copy(t: dict, a: int, c: int) -> bool:
    return t["shard_id"] == 0 and t.get("to_shard_id") is None and t["from"] == a and t["to"] == c


def is_migrate(t: dict) -> bool:
    return t["shard_id"] == 1 and t.get("to_shard_id") == 0


def restart(idx: int, peer_dirs, peer_uris, procs, bootstrap_idx: int, log: str, extra_env: dict) -> PeerProcess:
    proc = procs[idx]
    processes.remove(proc)
    proc.kill()
    bootstrap = get_peer_consensus_uri(procs[bootstrap_idx].p2p_port)
    peer_uris[idx] = start_peer(peer_dirs[idx], log, bootstrap, port=proc.p2p_port, extra_env=extra_env)
    return processes[-1]


def test_reshard_down_migrate_into_copying_replica(tmp_path: pathlib.Path):
    peer_uris, peer_dirs, _ = start_cluster(tmp_path, 3, extra_env=ENV)
    skip_if_no_feature(peer_uris[0], "staging")
    wait_for(all_peers_are_voters, peer_uris)
    peer_ids = [get_cluster_info(uri)["peer_id"] for uri in peer_uris]
    procs = list(processes)

    create_collection(peer_uris[0], collection=COLLECTION, shard_number=2, replication_factor=1, sparse_vectors=False)
    wait_collection_exists_and_active_on_all_peers(COLLECTION, peer_uris)
    upsert_random_points(peer_uris[0], 400, collection_name=COLLECTION, batch_size=100, with_sparse_vector=False)

    a = next(iter(shard_states(peer_uris[0], 0)))
    b = next(iter(shard_states(peer_uris[0], 1)))
    c = next(p for p in peer_ids if p not in (a, b))
    ia, ib, ic = peer_ids.index(a), peer_ids.index(b), peer_ids.index(c)
    print(f"shard 0 on A={a}, shard 1 on B={b}, empty C={c}")

    # Only A, the copy's source, is slowed down, so the copy outlives the migrate from B
    procs[ia] = restart(ia, peer_dirs, peer_uris, procs, ib, "peer_a_delayed.log",
                        {**ENV, "QDRANT_STAGING_SHARD_TRANSFER_DELAY_SEC": "20"})
    wait_for_peer_online(peer_uris[ia], wait_for_timeout=60)
    for uri in peer_uris:
        wait_for_all_replicas_active(uri, COLLECTION)

    replicate_shard(peer_uris[ia], COLLECTION, 0, a, c, method="stream_records")
    wait_for(lambda: shard_states(peer_uris[ic], 0).get(c) == "Partial"
             and any(is_copy(t, a, c) for t in transfers(peer_uris[ic])))

    resp = start_resharding(peer_uris[ib], COLLECTION, direction="down", peer_id=b)
    assert_http_ok(resp)
    for uri in peer_uris:
        wait_for_collection_resharding_operations_count(uri, COLLECTION, 1)

    migrate = requests.post(f"{peer_uris[ib]}/collections/{COLLECTION}/cluster", params={"timeout": 20}, json={
        "replicate_shard": {
            "shard_id": 1, "to_shard_id": 0, "from_peer_id": b, "to_peer_id": c,
            "method": "resharding_stream_records",
        }
    }, timeout=30)
    print(f"migrate of shard 1 into shard 0 on C: {migrate.status_code} {migrate.text}")
    wait_for_same_applied_commit(peer_uris)
    wait_for(lambda: not any(is_migrate(t) for t in transfers(peer_uris[ic])), wait_for_timeout=60)
    state_c_after_migrate = shard_states(peer_uris[ic], 0).get(c)

    if not any(is_copy(t, a, c) for t in transfers(peer_uris[ic])):
        raise RuntimeError("copy of shard 0 into C finished before A was killed, the scenario did not run")

    # Kill the copy's source and write, so the other peers try to deactivate it
    killed = procs[ia]
    processes.remove(killed)
    killed.kill()

    def write_and_check_a_deactivated() -> bool:
        upsert_random_points(peer_uris[ib], 50, collection_name=COLLECTION, offset=10_000,
                             fail_on_error=False, with_sparse_vector=False, timeout=10)
        return shard_states(peer_uris[ib], 0).get(a) != "Active"

    try:
        wait_for(write_and_check_a_deactivated, wait_for_timeout=20, wait_for_interval=1)
    except Exception:
        pass
    states_after_kill = {peer: shard_states(peer_uris[peer_ids.index(peer)], 0) for peer in (b, c)}

    bootstrap = get_peer_consensus_uri(procs[ib].p2p_port)
    peer_uris[ia] = start_peer(peer_dirs[ia], "peer_a_restarted.log", bootstrap, port=killed.p2p_port, extra_env=ENV)
    wait_for_peer_online(peer_uris[ia], path="/healthz", wait_for_timeout=60)

    seen = {}

    def shard_0_served_everywhere() -> bool:
        for uri in peer_uris:
            try:
                seen[uri] = shard_states(uri, 0)
            except requests.RequestException as e:
                seen[uri] = repr(e)
                return False
        return all(isinstance(s, dict) and any(st in SERVING_STATES for st in s.values()) for s in seen.values())

    try:
        wait_for(shard_0_served_everywhere, wait_for_timeout=60, wait_for_interval=1)
    except Exception:
        assert False, (
            f"shard 0 has no Active replica 60 s after its copy source A={a} restarted: replicas per peer {seen}. "
            f"The migrate of shard 1 into C={c} was answered {migrate.status_code} while C was still the copy's target, "
            f"C's replica of shard 0 was {state_c_after_migrate} after it, "
            f"and after killing A, B and C saw shard 0 as {states_after_kill}"
        )
