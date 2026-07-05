import multiprocessing
import pathlib
from time import sleep

from .fixtures import upsert_random_points, create_collection
from .utils import *

N_PEERS = 3
N_SHARDS = 3
N_REPLICA = 1
COLLECTION_NAME = "test_collection"

# Point ids that are never inserted as real points. A set-payload request for
# one of these fails locally (the point does not exist), but the operation is
# still written to the WAL *before* the point-existence check rejects it (see
# `LocalShard::submit_update`, which calls `wal.lock_and_write` before applying).
# During a snapshot transfer the queue proxy replays buffered WAL operations to
# the receiver, which applies them with `force=true` (bypassing the missing-point
# tolerance in `handle_failed_replicas`), so the operation hard-fails there with
# `NotFound: No point with id ... found` and aborts the transfer.
MISSING_ID_START = 10_000_000

# Number of distinct missing ids targeted per set-payload request. Each request
# carries a contiguous range of ids, which hash across every shard, so the
# failing operations are buffered into the queue proxy of *all* shards being
# transferred - not just the one a single id happens to land on.
MISSING_IDS_PER_REQUEST = 200

# How many concurrent missing-point senders to run per peer. Sustained, heavy,
# unthrottled load guarantees that a missing-point operation is present in the
# queue proxy during *every* transfer attempt (including the automatic retries
# that consensus starts for the resulting Dead replica), so the failure is
# reliable rather than a lost race.
MISSING_SENDERS_PER_PEER = 2

# Initial dataset size. Larger means the snapshot creation + transfer window is
# longer, giving the background load more time to fill the queue proxy.
INITIAL_POINTS = 20_000

# How long to wait for the transferred replica to reach the Active state.
TRANSFER_TIMEOUT_SEC = 90

# How long to wait for point counts to converge across peers after the load is
# stopped. Killing the load processes does not cancel their requests that are
# already executing server-side, so the last accepted update may still be
# propagating to replicas when we start counting.
CONVERGENCE_TIMEOUT_SEC = 30


def set_payload_missing_points_in_loop(peer_url, collection_name, base_id):
    """Continuously issue set-payload requests for points that do not exist.

    Each request targets a range of ids so it spans all shards. These requests
    are expected to be rejected with a "not found" error on the source - that is
    the whole point of the test - so we deliberately do not assert success here.
    Sent with `wait=false` to maximize throughput.
    """
    point_id = base_id
    while True:
        point_ids = list(range(point_id, point_id + MISSING_IDS_PER_REQUEST))
        try:
            requests.post(
                f"{peer_url}/collections/{collection_name}/points/payload?wait=false",
                json={
                    "points": point_ids,
                    "payload": {"city": "nowhere"},
                },
                timeout=10,
            )
        except requests.exceptions.RequestException:
            pass
        point_id += MISSING_IDS_PER_REQUEST


def run_set_payload_missing_points_in_background(peer_url, collection_name, base_id):
    p = multiprocessing.Process(
        target=set_payload_missing_points_in_loop,
        args=(peer_url, collection_name, base_id),
    )
    p.start()
    return p


def update_points_in_loop(peer_url, collection_name, offset=0):
    limit = 3
    while True:
        upsert_random_points(peer_url, limit, collection_name, offset=offset)
        offset += limit
        sleep(0.1)


def run_update_points_in_background(peer_url, collection_name, init_offset=0):
    p = multiprocessing.Process(
        target=update_points_in_loop,
        args=(peer_url, collection_name, init_offset),
    )
    p.start()
    return p


def receiver_shard_state(peer_api_uri, shard_id):
    """Return the state of `shard_id` in the receiver's local shards, or None if
    the shard is not present (e.g. dropped after an aborted transfer)."""
    info = get_collection_cluster_info(peer_api_uri, COLLECTION_NAME)
    for shard in info['local_shards']:
        if shard['shard_id'] == shard_id:
            return shard['state']
    return None


# Snapshot transfer must finish even when set-payload requests for non-existing
# points are running concurrently with real insertions.
#
# Such operations are persisted to the WAL before the point-existence check
# fails them. While a snapshot transfer is in progress the queue proxy replays
# buffered WAL operations to the receiver, which applies them with `force=true`.
# A set-payload for a point the receiver does not have hard-fails with:
#
#   status: NotFound, message: "Not found: No point with id ... found"
#
# Bounded retries (queue-level + driver-level) can lose the race under sustained
# load: the receiver replica is then marked Dead and the transfer is aborted.
# Consensus auto-restarts the transfer, but while the load keeps running every
# attempt fails the same way, so the replica never reaches Active.
#
# IMPORTANT: the missing-point load must stay running while we check the result.
# If it is stopped first, the next automatic recovery transfer has no failing
# operations to replay and succeeds, masking the bug.
#
# This test must FAIL on buggy code (replica never reaches Active / goes Dead)
# and PASS once the receiver tolerates missing-point operations during recovery.
def test_shard_snapshot_transfer_with_missing_point_updates(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )

    # Insert some initial real points. Batch the insert: a single 20k-point
    # request saturates all cores long enough to starve the consensus thread
    # (cascading leader elections) and to blow past the 2000ms per-shard update
    # healthcheck deadline, which returns a flaky 408 here before the actual
    # test even starts.
    upsert_random_points(peer_api_uris[0], INITIAL_POINTS, batch_size=1000)

    # Concurrent background load:
    #  - real insertions of new points
    #  - heavy set-payload requests targeting points that do not exist anywhere
    procs = []
    procs.append(run_update_points_in_background(
        peer_api_uris[0], COLLECTION_NAME, init_offset=INITIAL_POINTS,
    ))
    for peer_idx, uri in enumerate(peer_api_uris):
        for sender_idx in range(MISSING_SENDERS_PER_PEER):
            # Disjoint id ranges per sender so they keep producing fresh misses
            base_id = MISSING_ID_START + (peer_idx * 100 + sender_idx) * 50_000_000
            procs.append(run_set_payload_missing_points_in_background(
                uri, COLLECTION_NAME, base_id,
            ))

    try:
        transfer_info = get_collection_cluster_info(peer_api_uris[0], COLLECTION_NAME)
        receiver_info = get_collection_cluster_info(peer_api_uris[2], COLLECTION_NAME)

        from_peer_id = transfer_info['peer_id']
        to_peer_id = receiver_info['peer_id']
        shard_id = transfer_info['local_shards'][0]['shard_id']

        # Let the background load buffer some operations first
        # This can be false-positive, but not false-negative.
        sleep(1)

        # Replicate shard `shard_id` from peer 0 to peer 2 via snapshot
        replicate_shard(
            peer_api_uris[0], COLLECTION_NAME, shard_id, from_peer_id, to_peer_id,
            method="snapshot",
        )

        # Watch the transferred replica *while the load is still running*. On a
        # correct implementation it reaches Active; on buggy code it is marked
        # Dead (transfer aborted) and never recovers under sustained load.
        reached_active = False
        saw_dead = False
        deadline = time.time() + TRANSFER_TIMEOUT_SEC
        while time.time() < deadline:
            state = receiver_shard_state(peer_api_uris[2], shard_id)
            if state == 'Dead':
                saw_dead = True
            if state == 'Active':
                reached_active = True
                break
            sleep(0.5)

        assert reached_active, (
            f"Snapshot transfer of shard {shard_id} did not complete: the receiver "
            f"replica never reached Active within {TRANSFER_TIMEOUT_SEC}s "
            f"(saw Dead state: {saw_dead}). The transfer was aborted by a "
            f"'No point with id ... found' error during queue proxy replay."
        )
    finally:
        for p in procs:
            p.kill()
        for p in procs:
            p.join()

    # Once the load is stopped, the cluster must converge: all replicas Active
    # and point counts consistent across nodes.
    for uri in peer_api_uris:
        wait_for_all_replicas_active(uri, COLLECTION_NAME)

    receiver_info = get_collection_cluster_info(peer_api_uris[2], COLLECTION_NAME)
    assert len(receiver_info['local_shards']) == 2

    def exact_counts():
        counts = []
        for uri in peer_api_uris:
            r = requests.post(
                f"{uri}/collections/{COLLECTION_NAME}/points/count", json={"exact": True}
            )
            assert_http_ok(r)
            counts.append(r.json()["result"]['count'])
        return counts

    # An upsert accepted just before the load processes were killed may still be
    # in flight: applied on the receiving peer's local replica but not yet
    # forwarded to the other replica of the shard. Counting during that window
    # observes different totals per peer, so poll until the counts converge.
    deadline = time.time() + CONVERGENCE_TIMEOUT_SEC
    counts = exact_counts()
    while not counts[0] == counts[1] == counts[2] and time.time() < deadline:
        sleep(0.5)
        counts = exact_counts()
    assert counts[0] == counts[1] == counts[2], f"Inconsistent point counts: {counts}"
