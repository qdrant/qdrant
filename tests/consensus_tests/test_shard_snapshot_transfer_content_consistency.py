"""A replica promoted to Active must not be skipped by concurrent updates.

A peer that stalls applying raft entries keeps an old view of the replica set: the
receiver still reads `Recovery`, which `is_updatable()` rejects, so
`ShardReplicaSet::update()` drops it from the fan-out before sending anything - no
request, no failure, nothing logged. The other replicas satisfy
`write_consistency_factor`, so the write is acknowledged; the skipped replica stays
`Active`, and only `Dead` replicas are re-synced.

Steps:
1. 3 peers, 1 shard, replication factor 2, preloaded.
2. Roles from actual placement: the two holders are source and writer, the peer
   without a replica is the receiver - if it already holds one, the transfer would
   be a re-sync.
3. Write continuously through the writer, at random ids over a space far larger
   than the load covers, so a lost update is not overwritten before measuring.
4. Replicate source -> receiver by snapshot.
5. Stall the writer's consensus; the request queues behind the transfer's own
   entries, freezing its view at `Recovery`.
6. Wait for the receiver to go `Active`, asserting the freeze outlasts it.
7. Keep writing; these updates are acknowledged, so must reach the receiver.
8. Stop the load, wait out the freeze, let in-flight updates converge.
9. Assert the replicas agree (see `find_divergence`).

A recovery point behind the others means the updates were never sent, rather than
sent and skipped as already-applied.

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

N_PEERS = 3
N_SHARDS = 1
N_REPLICA = 2
SHARD_ID = 0
COLLECTION_NAME = "test_collection"

ID_SPACE = 500_000
BATCH_SIZE = 100
PRELOAD_COUNT = 5_000

FREEZE_WRITER_SEC = 60.0
POST_ACTIVE_LOAD_SEC = 15

TRANSFER_TIMEOUT_SEC = 90
CONVERGENCE_TIMEOUT_SEC = 60
SCROLL_PAGE = 1_000

# --- Helpers -----------------------------------------------------------


class Peer(NamedTuple):
    uri: str
    id: int


class Topology(NamedTuple):
    source: Peer
    writer: Peer
    receiver: Peer


def start_preloaded_cluster(tmp_path):
    peer_api_uris, _, _ = start_cluster(tmp_path, N_PEERS)
    create_collection(
        peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA
    )
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )
    upsert_random_points(peer_api_uris[0], PRELOAD_COUNT, batch_size=200)
    return peer_api_uris


def discover_topology(peer_api_uris):
    holders, non_holders = [], []
    for uri in peer_api_uris:
        info = get_collection_cluster_info(uri, COLLECTION_NAME)
        peer = Peer(uri, info["peer_id"])
        holds = any(s["shard_id"] == SHARD_ID for s in info["local_shards"])
        (holders if holds else non_holders).append(peer)

    assert len(holders) == N_REPLICA and len(non_holders) == 1, (
        f"expected {N_REPLICA} holders and 1 non-holder of shard {SHARD_ID}, "
        f"got {len(holders)} and {len(non_holders)}"
    )
    return Topology(source=holders[0], writer=holders[1], receiver=non_holders[0])


def write_points_in_loop(peer_url):
    revision = 0
    while True:
        revision += 1
        try:
            upsert_random_points(
                peer_url,
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
def background_writes(peer_url):
    proc = multiprocessing.Process(target=write_points_in_loop, args=(peer_url,))
    proc.start()
    try:
        yield
    finally:
        proc.kill()
        proc.join()


class Freeze:
    def __init__(self, seconds):
        self.seconds = seconds
        self.started = time.time()

    def remaining(self):
        return self.seconds - (time.time() - self.started)

    def wait_out(self):
        left = self.remaining()
        if left > 0:
            sleep(left + 2)


def freeze_consensus(peer_api_uri, peer_id, seconds):
    r = requests.post(
        f"{peer_api_uri}/collections/{COLLECTION_NAME}/cluster",
        json={"test_slow_down": {"peer_id": peer_id, "duration": seconds}},
    )
    assert_http_ok(r)
    return Freeze(seconds)


def wait_until(predicate, timeout, message):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        sleep(0.5)
    raise AssertionError(message)


def local_shard_state(peer_api_uri):
    info = get_collection_cluster_info(peer_api_uri, COLLECTION_NAME)
    for shard in info["local_shards"]:
        if shard["shard_id"] == SHARD_ID:
            return shard["state"]
    return None


def recovery_point(peer_api_uri):
    r = requests.get(
        f"{peer_api_uri}/collections/{COLLECTION_NAME}/shards/{SHARD_ID}/recovery_point"
    )
    return r.json()["result"] if r.status_code == 200 else None


def scroll_points(peer_api_uri, consistency=None):
    query = f"?consistency={consistency}" if consistency else ""
    points, offset = {}, None
    while True:
        body = {"limit": SCROLL_PAGE, "with_vectors": False, "with_payload": True}
        if offset is not None:
            body["offset"] = offset
        r = requests.post(
            f"{peer_api_uri}/collections/{COLLECTION_NAME}/points/scroll{query}",
            json=body,
        )
        assert_http_ok(r)
        result = r.json()["result"]
        for point in result["points"]:
            points[point["id"]] = (point.get("payload") or {}).get("revision")
        offset = result.get("next_page_offset")
        if offset is None:
            return points


def find_divergence(peer_api_uris):
    """Points the replicas disagree on, by two independent measures.

    Comparing each peer's own copy assumes a holder reads locally; a read at
    consistency `all` drops any point the replicas disagree on, whatever the
    routing. Both must come back empty.
    """
    per_peer = {uri: scroll_points(uri) for uri in peer_api_uris}
    copies = list(per_peer.values())
    all_ids = set().union(*(set(copy) for copy in copies))

    differing = [
        i for i in all_ids if any(c.get(i) != copies[0].get(i) for c in copies[1:])
    ]
    agreed = set(scroll_points(peer_api_uris[0], consistency="all"))
    dropped = sorted(all_ids - agreed)
    return per_peer, differing, dropped


def assert_replicas_agree(peer_api_uris, receiver_uri):
    deadline = time.time() + CONVERGENCE_TIMEOUT_SEC
    per_peer, differing, dropped = find_divergence(peer_api_uris)
    while (differing or dropped) and time.time() < deadline:
        sleep(1)
        per_peer, differing, dropped = find_divergence(peer_api_uris)

    if not differing and not dropped:
        return

    counts = {uri: len(points) for uri, points in per_peer.items()}
    sample = {
        i: {uri: points.get(i) for uri, points in per_peer.items()}
        for i in sorted(differing)[:3]
    }
    points_behind = {uri: recovery_point(uri) for uri in peer_api_uris}

    raise AssertionError(
        f"{len(differing)} points differ between replicas and {len(dropped)} are "
        f"dropped by a read at consistency `all`. Counts: {counts}. Sample "
        f"revisions per peer: {sample}. First dropped ids: {dropped[:10]}. "
        f"Receiver was {receiver_uri}. Recovery points: {points_behind}"
    )


# --- Test --------------------------------------------------------------------


def test_shard_snapshot_transfer_content_consistency(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris = start_preloaded_cluster(tmp_path)
    source, writer, receiver = discover_topology(peer_api_uris)

    with background_writes(writer.uri):
        sleep(1)
        assert local_shard_state(receiver.uri) is None, (
            f"{receiver.uri} already holds shard {SHARD_ID}; the transfer would be "
            f"a re-sync rather than a new replica"
        )

        replicate_shard(
            source.uri,
            COLLECTION_NAME,
            SHARD_ID,
            source.id,
            receiver.id,
            method="snapshot",
        )
        freeze = freeze_consensus(source.uri, writer.id, FREEZE_WRITER_SEC)

        wait_until(
            lambda: local_shard_state(receiver.uri) == "Active",
            TRANSFER_TIMEOUT_SEC,
            f"snapshot transfer of shard {SHARD_ID} did not complete: the receiver "
            f"never reached Active within {TRANSFER_TIMEOUT_SEC}s",
        )

        assert freeze.remaining() > POST_ACTIVE_LOAD_SEC, (
            f"receiver reached Active with only {freeze.remaining():.1f}s of freeze "
            f"left, less than the {POST_ACTIVE_LOAD_SEC}s of load that follows. "
            f"Raise FREEZE_WRITER_SEC above the transfer duration"
        )

        sleep(POST_ACTIVE_LOAD_SEC)

    freeze.wait_out()
    for uri in peer_api_uris:
        wait_for_all_replicas_active(uri, COLLECTION_NAME)

    assert_replicas_agree(peer_api_uris, receiver.uri)
