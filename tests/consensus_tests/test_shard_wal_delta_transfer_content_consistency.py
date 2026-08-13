"""A WAL delta transfer must not leave a replica Active without the updates it missed.

Re-replicates shard 0 between its two holders by wal_delta while a third peer, which
coordinates all the writes, has its consensus stalled. Fails if the two holders still
disagree once the stall is over and everything has settled.

Needs `--features staging` for `test_slow_down` and `recovery_point`.
"""

import multiprocessing
import pathlib
import random
from contextlib import contextmanager
from time import sleep
from typing import NamedTuple

from .fixtures import create_collection, upsert_random_points
from .utils import *

COLLECTION_NAME = "test_collection"
N_PEERS, N_SHARDS, N_REPLICA, SHARD_ID = 4, 1, 2, 0
ID_SPACE, BATCH_SIZE, PRELOAD = 500_000, 100, 5_000
FREEZE_SEC, POST_ACTIVE_LOAD_SEC = 90.0, 15
TRANSFER_TIMEOUT, CONVERGENCE_TIMEOUT = 120, 60


class Peer(NamedTuple):
    uri: str
    id: int


def roles(peer_api_uris):
    """Two holders of shard 0, plus a writer that holds nothing."""
    leader = get_leader(peer_api_uris[0])
    holders, others = [], []
    for uri in peer_api_uris:
        info = get_collection_cluster_info(uri, COLLECTION_NAME)
        peer = Peer(uri, info["peer_id"])
        holds = any(s["shard_id"] == SHARD_ID for s in info["local_shards"])
        (holders if holds else others).append(peer)

    # Stalling the leader stalls consensus itself, and the transfer then cannot
    # finish until the stall ends - which is not the window under test.
    writers = [p for p in others if p.id != leader]
    assert len(holders) == N_REPLICA and writers, f"holders={holders} writers={writers}"
    return holders[0], holders[1], writers[0]


def write_loop(uri):
    revision = 0
    while True:
        revision += 1
        try:
            upsert_random_points(
                uri,
                BATCH_SIZE,
                COLLECTION_NAME,
                offset=random.randrange(0, ID_SPACE - BATCH_SIZE),
                batch_size=BATCH_SIZE,
                fail_on_error=False,
                extra_payload={"revision": revision},
            )
        except requests.exceptions.RequestException:
            pass


@contextmanager
def background_writes(uri):
    proc = multiprocessing.Process(target=write_loop, args=(uri,))
    proc.start()
    try:
        yield
    finally:
        proc.kill()
        proc.join()


def stall_consensus(uri, peer_id, seconds):
    """Returns the wall-clock time the stall ends. `duration` is in seconds."""
    r = requests.post(
        f"{uri}/collections/{COLLECTION_NAME}/cluster",
        json={"test_slow_down": {"peer_id": peer_id, "duration": seconds}},
    )
    assert_http_ok(r)
    return time.time() + seconds


def shard_state(uri):
    info = get_collection_cluster_info(uri, COLLECTION_NAME)
    return next(
        (s["state"] for s in info["local_shards"] if s["shard_id"] == SHARD_ID), None
    )


def wait_for_transfer(uri, timeout):
    """Return the states seen, once shard 0 is Active again after leaving Active."""
    deadline, seen = time.time() + timeout, []
    while time.time() < deadline:
        state = shard_state(uri)
        if not seen or seen[-1] != state:
            seen.append(state)
        if state == "Active" and any(s != "Active" for s in seen):
            return seen
        sleep(0.2)
    raise AssertionError(f"transfer did not finish in {timeout}s, states seen: {seen}")


def points(uri):
    out, offset = {}, None
    while True:
        body = {"limit": 1000, "with_vectors": False, "with_payload": True}
        if offset is not None:
            body["offset"] = offset
        r = requests.post(
            f"{uri}/collections/{COLLECTION_NAME}/points/scroll", json=body
        )
        assert_http_ok(r)
        result = r.json()["result"]
        for point in result["points"]:
            out[point["id"]] = (point.get("payload") or {}).get("revision")
        offset = result.get("next_page_offset")
        if offset is None:
            return out


def recovery_point(uri):
    r = requests.get(
        f"{uri}/collections/{COLLECTION_NAME}/shards/{SHARD_ID}/recovery_point"
    )
    return r.json()["result"] if r.status_code == 200 else None


def assert_holders_agree(source, receiver, timeout):
    deadline = time.time() + timeout
    while True:
        src, rcv = points(source.uri), points(receiver.uri)
        missing = src.keys() - rcv.keys()
        stale = {i for i in src.keys() & rcv.keys() if src[i] != rcv[i]}
        if not missing and not stale:
            return
        if time.time() > deadline:
            break
        sleep(1)

    raise AssertionError(
        f"receiver is missing {len(missing)} points and has {len(stale)} stale ones "
        f"({len(src)} points on the source, {len(rcv)} on the receiver). "
        f"First missing ids: {sorted(missing)[:5]}. A receiver recovery point BEHIND "
        f"the source means the updates were never sent.\n"
        f"  source   {recovery_point(source.uri)}\n"
        f"  receiver {recovery_point(receiver.uri)}"
    )


def test_shard_wal_delta_transfer_content_consistency(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, _, _ = start_cluster(tmp_path, N_PEERS)
    create_collection(
        peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA
    )
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )
    upsert_random_points(peer_api_uris[0], PRELOAD, batch_size=200)

    source, receiver, writer = roles(peer_api_uris)

    with background_writes(writer.uri):
        sleep(1)
        replicate_shard(
            source.uri,
            COLLECTION_NAME,
            SHARD_ID,
            source.id,
            receiver.id,
            method="wal_delta",
        )
        # Proposed after the transfer's Start, and raft applies in order, so the
        # writer's view of the receiver freezes at Recovery.
        stall_ends = stall_consensus(source.uri, writer.id, FREEZE_SEC)

        seen = wait_for_transfer(receiver.uri, TRANSFER_TIMEOUT)
        left = stall_ends - time.time()
        assert left > POST_ACTIVE_LOAD_SEC, (
            f"only {left:.1f}s of stall left after the transfer, need "
            f"{POST_ACTIVE_LOAD_SEC}s. States seen: {seen}. Raise FREEZE_SEC"
        )
        sleep(POST_ACTIVE_LOAD_SEC)

    sleep(max(0.0, stall_ends - time.time()) + 2)
    for uri in peer_api_uris:
        wait_for_all_replicas_active(uri, COLLECTION_NAME)

    assert_holders_agree(source, receiver, CONVERGENCE_TIMEOUT)
