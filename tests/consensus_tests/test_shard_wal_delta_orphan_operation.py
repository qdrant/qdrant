"""
Test WAL delta transfers to a replica that has an operation no other replica has.

The update path writes an operation to the leader's local WAL before it sends the operation to
the other replicas. If the leader dies in between, the operation exists only on the leader's
replica, and that replica's clock is one tick ahead of all other replicas.

- If the transfer source has not seen a newer tick of that clock, it asks the receiver which
  points the orphan operations changed, and resolves the WAL delta from before them. After
  transferring the delta, it overwrites those points on the receiver with its own versions of them.
- After a restart the leader's clocks start from scratch. Its next update through the same clock
  reuses the orphan operation's tick with a new token. The ticks are equal again, but the tokens
  differ, so the source rejects the WAL delta and the transfer falls back to a snapshot transfer.

The tests use staging features and are skipped without them:
- the `delay` debug operation, which the leader writes to its local WAL, while remote shards skip
  it and still report success with the clock tag
- `QDRANT_STAGING_ABORT_AFTER_LOCAL_UPDATE_POINT_ID`, which aborts the leader right after its local
  WAL write, when the update touches the given point
"""

import pathlib

from .fixtures import create_collection, upsert_random_points
from .utils import *

COLLECTION_NAME = "test_collection"
SHARD_ID = 0

# Logged by the transfer source when the receiver is ahead on its own clocks
AHEAD_ON_OWN_CLOCKS_LOG = "Remote shard is ahead on its own clocks"

# Logged by the transfer source when it overwrites points changed by orphan operations
OVERWRITING_POINTS_LOG = "points changed by orphan operations on peer"

# Logged by the transfer source when a WAL delta transfer fails and falls back to another method
FALLBACK_LOG = "Failed to do shard diff transfer"

# Logged by the transfer source when `resolve_wal_delta` returns `WalDeltaError::Diverged`
DIVERGED_ERROR = "recovery point has an operation this WAL does not have"

# Point upserted through the leader that aborts before sending it to other replicas
ORPHAN_POINT_ID = 1_000_000

# Prevent automatic recovery, so we start the transfer ourselves with a specific method
TRANSFER_LIMITS_ENV = {
    "QDRANT__STORAGE__PERFORMANCE__INCOMING_SHARD_TRANSFERS_LIMIT": "0",
    "QDRANT__STORAGE__PERFORMANCE__OUTGOING_SHARD_TRANSFERS_LIMIT": "0",
}


def submit_delay(peer_uri: str):
    r = requests.post(
        f"{peer_uri}/collections/{COLLECTION_NAME}/debug?wait=true",
        json={"delay": {"duration_sec": 0.0}},
    )
    assert_http_ok(r)


def get_recovery_point(peer_uri: str) -> dict:
    r = requests.get(f"{peer_uri}/collections/{COLLECTION_NAME}/shards/{SHARD_ID}/recovery_point")
    assert_http_ok(r)
    return {
        (tag["peer_id"], tag["clock_id"]): (tag["clock_tick"], tag["token"])
        for tag in r.json()["result"]
    }


def assert_receiver_one_tick_ahead(receiver_uri: str, source_uris: list, receiver_peer_id: int):
    """Assert the receiver's recovery point is ahead of every source by exactly one tick of one
    clock, and that this clock belongs to the receiver's own peer."""
    receiver = get_recovery_point(receiver_uri)

    for source_uri in source_uris:
        source = get_recovery_point(source_uri)
        message = f"receiver: {receiver}, source {source_uri}: {source}"

        ahead = {
            key: (tick, source.get(key, (None, None))[0])
            for key, (tick, _token) in receiver.items()
            if key not in source or tick > source[key][0]
        }

        assert len(ahead) == 1, message
        ((peer_id, _clock_id), (receiver_tick, source_tick)), = ahead.items()
        assert peer_id == receiver_peer_id, message
        assert source_tick is not None, message
        assert receiver_tick == source_tick + 1, message


def assert_receiver_same_tick_other_token(receiver_uri: str, source_uris: list, receiver_peer_id: int):
    """Assert no source is behind the receiver on any clock, and that every source has the same
    tick as the receiver on one clock of the receiver's own peer, but with a different token."""
    receiver = get_recovery_point(receiver_uri)

    for source_uri in source_uris:
        source = get_recovery_point(source_uri)
        message = f"receiver: {receiver}, source {source_uri}: {source}"

        for key, (tick, _token) in receiver.items():
            assert key in source and tick <= source[key][0], message

        other_token = [
            key
            for key, (tick, token) in receiver.items()
            if source[key][0] == tick and source[key][1] != token
        ]

        assert len(other_token) == 1, message
        ((peer_id, _clock_id),) = other_token
        assert peer_id == receiver_peer_id, message


def check_replica_state(peer_uri: str, peer_id: int, expected_state: str) -> bool:
    info = get_collection_cluster_info(peer_uri, COLLECTION_NAME)
    local = [(info["peer_id"], shard["state"]) for shard in info["local_shards"]]
    remote = [(shard["peer_id"], shard["state"]) for shard in info["remote_shards"]]
    return (peer_id, expected_state) in local + remote


def has_point(peer_uri: str, point_id: int) -> bool:
    r = requests.post(f"{peer_uri}/collections/{COLLECTION_NAME}/points", json={"ids": [point_id]})
    assert_http_ok(r)
    return len(r.json()["result"]) > 0


def scroll_all_points(peer_api_uris: list) -> list:
    data = []
    for uri in peer_api_uris:
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/scroll",
            json={
                "limit": 999999999,
                "with_vectors": True,
                "with_payload": True,
            },
        )
        assert_http_ok(r)
        data.append(r.json()["result"]["points"])
    return data


def test_wal_delta_transfer_past_orphan_operation_without_points(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3, extra_env=TRANSFER_LIMITS_ENV)

    # The `delay` debug operation is only available with the staging feature
    skip_if_no_feature(peer_api_uris[0], "staging")

    # A is the transfer source, R receives the operation that no other replica has
    a_uri, c_uri, r_uri = peer_api_uris
    a_peer_id = get_cluster_info(a_uri)["peer_id"]
    r_peer_id = get_cluster_info(r_uri)["peer_id"]

    create_collection(a_uri, shard_number=1, replication_factor=3)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )

    # Upsert through R, so every replica knows R's clock
    upsert_random_points(r_uri, 100)

    # Only R writes the delay operation to its WAL, which puts R's clock one tick ahead
    submit_delay(r_uri)
    assert_receiver_one_tick_ahead(r_uri, [a_uri, c_uri], r_peer_id)

    # Kill R
    r_process = processes.pop()
    r_port = r_process.p2p_port
    r_process.kill()

    # Upsert through A. A fails to reach R and marks it as Dead
    upsert_random_points(a_uri, 10, offset=1000)
    wait_for(check_replica_state, a_uri, r_peer_id, "Dead")

    # Restart R, it must see its own replica as Dead
    r_uri = start_peer(peer_dirs[-1], "peer_0_2_restarted.log", bootstrap_uri, port=r_port, extra_env=TRANSFER_LIMITS_ENV)
    peer_api_uris[-1] = r_uri
    wait_for_peer_online(r_uri, "/")
    wait_for(check_replica_state, r_uri, r_peer_id, "Dead")

    # R still has the extra tick after the restart, now from the clocks snapshot taken when it
    # became Dead
    assert_receiver_one_tick_ahead(r_uri, [a_uri, c_uri], r_peer_id)

    # Recover R from A with a WAL delta transfer
    replicate_shard(a_uri, COLLECTION_NAME, SHARD_ID, a_peer_id, r_peer_id, method="wal_delta")
    wait_for_collection_shard_transfers_count(a_uri, COLLECTION_NAME, 0)
    wait_for_all_replicas_active(a_uri, COLLECTION_NAME)

    # A found R ahead on its own clock, and resolved the WAL delta from before the delay operation.
    # The delay operation changed no points, so there was nothing to overwrite, and no fallback
    a_log = pathlib.Path(init_pytest_log_folder()) / "peer_0_0.log"
    a_log_text = a_log.read_text()
    assert AHEAD_ON_OWN_CLOCKS_LOG in a_log_text
    assert FALLBACK_LOG not in a_log_text

    check_data_consistency(scroll_all_points(peer_api_uris))


def start_cluster_with_orphan_operation(tmp_path: pathlib.Path, orphan_operation) -> dict:
    """Start peers A, C and R, and leave an orphan operation on R.

    Creates a collection with one shard replicated on all peers, and upserts points through R,
    so every replica knows R's clock. Then R runs `orphan_operation(r_uri)`, which must touch
    `ORPHAN_POINT_ID`. The failpoint makes R write it to its own WAL and abort before sending it to
    A and C. A then marks R as Dead, and R restarts without the failpoint.

    Returns the peer API URIs and IDs, and what's needed to restart peers.
    """
    env = {
        **TRANSFER_LIMITS_ENV,
        "QDRANT_STAGING_ABORT_AFTER_LOCAL_UPDATE_POINT_ID": str(ORPHAN_POINT_ID),
    }

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, 3, extra_env=env)

    # The abort failpoint is only available with the staging feature
    skip_if_no_feature(peer_api_uris[0], "staging")

    a_uri, c_uri, r_uri = peer_api_uris
    a_peer_id = get_cluster_info(a_uri)["peer_id"]
    c_peer_id = get_cluster_info(c_uri)["peer_id"]
    r_peer_id = get_cluster_info(r_uri)["peer_id"]

    create_collection(a_uri, shard_number=1, replication_factor=3)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )

    # Upsert through R, so every replica knows R's clock
    upsert_random_points(r_uri, 100)

    # R writes the orphan operation to its own WAL, then aborts before sending it to A and C
    try:
        orphan_operation(r_uri)
    except requests.exceptions.ConnectionError:
        pass
    else:
        pytest.fail("R must abort while running the orphan operation")

    r_process = processes.pop()
    r_port = r_process.p2p_port
    r_process.kill()

    # Upsert through A. A fails to reach R and marks it as Dead
    upsert_random_points(a_uri, 10, offset=1000)
    wait_for(check_replica_state, a_uri, r_peer_id, "Dead")

    # Restart R without the failpoint, it must see its own replica as Dead
    r_uri = start_peer(peer_dirs[-1], "peer_0_2_restarted.log", bootstrap_uri, port=r_port, extra_env=TRANSFER_LIMITS_ENV)
    peer_api_uris[-1] = r_uri
    wait_for_peer_online(r_uri, "/")
    wait_for(check_replica_state, r_uri, r_peer_id, "Dead")

    # WAL replay restored the orphan operation's tick on R
    assert_receiver_one_tick_ahead(r_uri, [a_uri, c_uri], r_peer_id)

    return {
        "peer_api_uris": peer_api_uris,
        "peer_dirs": peer_dirs,
        "bootstrap_uri": bootstrap_uri,
        "a_peer_id": a_peer_id,
        "c_peer_id": c_peer_id,
        "r_peer_id": r_peer_id,
    }


def upsert_orphan_point(r_uri: str):
    upsert_random_points(r_uri, 1, offset=ORPHAN_POINT_ID)


def test_wal_delta_transfer_overwrites_orphan_upsert(tmp_path: pathlib.Path):
    assert_project_root()

    cluster = start_cluster_with_orphan_operation(tmp_path, upsert_orphan_point)
    peer_api_uris = cluster["peer_api_uris"]
    a_uri, _c_uri, _r_uri = peer_api_uris
    a_peer_id, r_peer_id = cluster["a_peer_id"], cluster["r_peer_id"]

    # Recover R from A with a WAL delta transfer
    replicate_shard(a_uri, COLLECTION_NAME, SHARD_ID, a_peer_id, r_peer_id, method="wal_delta")
    wait_for_collection_shard_transfers_count(a_uri, COLLECTION_NAME, 0)
    wait_for_all_replicas_active(a_uri, COLLECTION_NAME)

    # A found R ahead on its own clock, resolved the WAL delta from before the orphan upsert, and
    # overwrote the orphan point on R without falling back to another transfer method
    a_log = pathlib.Path(init_pytest_log_folder()) / "peer_0_0.log"
    a_log_text = a_log.read_text()
    assert AHEAD_ON_OWN_CLOCKS_LOG in a_log_text
    assert OVERWRITING_POINTS_LOG in a_log_text
    assert FALLBACK_LOG not in a_log_text

    # A doesn't have the orphan point, so the transfer deleted it on R. Each peer reads from its own
    # replica here, because all of them are Active
    assert not any(has_point(uri, ORPHAN_POINT_ID) for uri in peer_api_uris)
    check_data_consistency(scroll_all_points(peer_api_uris))


def test_wal_delta_transfer_rejects_reused_tick_on_receiver(tmp_path: pathlib.Path):
    assert_project_root()

    cluster = start_cluster_with_orphan_operation(tmp_path, upsert_orphan_point)
    peer_api_uris = cluster["peer_api_uris"]
    a_uri, c_uri, r_uri = peer_api_uris
    a_peer_id, r_peer_id = cluster["a_peer_id"], cluster["r_peer_id"]

    # Upsert through R again. R's clocks started from scratch after the restart, so A and C send
    # back their current tick and R reuses the orphan point's tick with a new token. R's own replica
    # is Dead, so only A and C get this update
    upsert_random_points(r_uri, 1, offset=ORPHAN_POINT_ID + 1)
    assert_receiver_same_tick_other_token(r_uri, [a_uri, c_uri], r_peer_id)

    # Recover R from A with a WAL delta transfer
    replicate_shard(a_uri, COLLECTION_NAME, SHARD_ID, a_peer_id, r_peer_id, method="wal_delta")
    wait_for_collection_shard_transfers_count(a_uri, COLLECTION_NAME, 0)
    wait_for_all_replicas_active(a_uri, COLLECTION_NAME)

    # A found the same tick with another token in R's recovery point, and fell back to another
    # transfer method
    a_log = pathlib.Path(init_pytest_log_folder()) / "peer_0_0.log"
    assert DIVERGED_ERROR in a_log.read_text()

    # The fallback transfer replaced R's shard, so the orphan point is gone. Each peer reads from
    # its own replica here, because all of them are Active
    assert not any(has_point(uri, ORPHAN_POINT_ID) for uri in peer_api_uris)
    check_data_consistency(scroll_all_points(peer_api_uris))
