"""
Filter/condition-resolving update operations on a replicated collection.

The coordinator forwards the *original* filter operation to every replica and
each replica resolves it to concrete point ids locally, before its WAL append
(see #9575 / #9678). Replicas holding the same data must resolve the same
filter to the same point set, and after a full-cluster restart each replica
must replay its id-based WAL records to exactly the same state.

Reads prefer the local replica, so with replication_factor == peer count a
scroll against a specific peer inspects that peer's own replica.
"""
import pathlib

from .assertions import assert_http_ok
from .utils import *

N_PEERS = 2
N_SHARDS = 2
N_REPLICAS = 2

COLLECTION = "test_collection_filter_ops"


def vector(seed: int) -> list:
    return [seed * 0.5, 0.25, 0.125, 1.0]


def upsert(peer_uri: str, body: dict):
    r = requests.put(
        f"{peer_uri}/collections/{COLLECTION}/points?wait=true", json=body)
    assert_http_ok(r)


def scroll_local_replica(peer_uri: str) -> dict:
    """Scroll every point with payload + vectors; served by the peer's own
    (active, local-preferred) replicas."""
    r = requests.post(
        f"{peer_uri}/collections/{COLLECTION}/points/scroll", json={
            "limit": 100,
            "with_payload": True,
            "with_vector": True,
        })
    assert_http_ok(r)
    points = r.json()["result"]["points"]
    return {point["id"]: point for point in points}


def try_scroll_matches(peer_uri: str, expected: dict) -> bool:
    try:
        return scroll_local_replica(peer_uri) == expected
    except Exception:
        return False


def test_filter_ops_resolved_per_replica(tmp_path: pathlib.Path):
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)
    peer_procs = processes[-N_PEERS:]

    r = requests.put(
        f"{peer_api_uris[0]}/collections/{COLLECTION}", json={
            "vectors": {"size": 4, "distance": "Dot"},
            "shard_number": N_SHARDS,
            "replication_factor": N_REPLICAS,
        })
    assert_http_ok(r)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION, peer_api_uris=peer_api_uris)

    # Points 1..=10: odd ids in group "drop", even ids in group "keep".
    upsert(peer_api_uris[0], {
        "points": [
            {
                "id": point_id,
                "vector": vector(point_id),
                "payload": {
                    "group": "drop" if point_id % 2 == 1 else "keep",
                    "n": point_id,
                },
            }
            for point_id in range(1, 11)
        ],
    })

    # Conditional insert-only upsert: existing point 2 must NOT be modified,
    # new point 11 must be inserted. Every replica resolves the existence
    # condition against its own state.
    upsert(peer_api_uris[0], {
        "points": [
            {"id": 2, "vector": vector(99), "payload": {"hacked": True}},
            {"id": 11, "vector": vector(11),
             "payload": {"group": "keep", "n": 11}},
        ],
        "update_mode": "insert_only",
    })

    # Conditional upsert with update filter: point 4 matches (n < 5) and is
    # replaced, point 6 does not match and must stay untouched.
    upsert(peer_api_uris[0], {
        "points": [
            {"id": 4, "vector": vector(4),
             "payload": {"group": "keep", "n": 4, "touched": True}},
            {"id": 6, "vector": vector(99),
             "payload": {"group": "keep", "n": 6, "touched": True}},
        ],
        "update_filter": {"must": [{"key": "n", "range": {"lt": 5}}]},
    })

    # Delete-by-filter: removes the odd ids (1, 3, 5, 7, 9). Resolved to
    # concrete ids per shard per replica; shards without matches resolve to
    # an empty operation.
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION}/points/delete?wait=true",
        json={"filter": {"must": [{"key": "group", "match": {"value": "drop"}}]}})
    assert_http_ok(r)

    # Set-payload-by-filter: tags points with n >= 10 (ids 10 and 11).
    r = requests.post(
        f"{peer_api_uris[0]}/collections/{COLLECTION}/points/payload?wait=true",
        json={
            "payload": {"flag": "late"},
            "filter": {"must": [{"key": "n", "range": {"gte": 10}}]},
        })
    assert_http_ok(r)

    expected_payloads = {
        2: {"group": "keep", "n": 2},
        4: {"group": "keep", "n": 4, "touched": True},
        6: {"group": "keep", "n": 6},
        8: {"group": "keep", "n": 8},
        10: {"group": "keep", "n": 10, "flag": "late"},
        11: {"group": "keep", "n": 11, "flag": "late"},
    }

    # Both replicas must hold the same resolved state.
    snapshots = [scroll_local_replica(uri) for uri in peer_api_uris]
    for snapshot in snapshots:
        assert {point_id: point["payload"] for point_id, point in
                snapshot.items()} == expected_payloads
    assert snapshots[0] == snapshots[1], \
        "replicas diverged after filter operations"
    reference = snapshots[0]

    # Full-cluster graceful restart: each replica replays its own WAL, which
    # must contain only id-based records, to exactly the same state.
    for proc in reversed(peer_procs):
        proc.interrupt()

    restarted_uris = []
    (first_uri, new_bootstrap_uri) = start_first_peer(
        peer_dirs[0], "peer_0_0_restarted.log", port=peer_procs[0].p2p_port)
    restarted_uris.append(first_uri)
    restarted_uris.append(start_peer(
        peer_dirs[1], "peer_0_1_restarted.log", new_bootstrap_uri,
        port=peer_procs[1].p2p_port))

    wait_for(all_nodes_respond, restarted_uris)
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION, peer_api_uris=restarted_uris)

    for uri in restarted_uris:
        wait_for(try_scroll_matches, uri, reference)
        replayed = scroll_local_replica(uri)
        assert replayed == reference, \
            f"replica on {uri} diverged after WAL replay"
