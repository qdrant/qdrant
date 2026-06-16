import pathlib

import requests

from .assertions import assert_http_ok
from .fixtures import create_collection, upsert_points
from .utils import *

N_PEERS = 3
COLLECTION_NAME = "test_collection"
# Keep in sync with `api::HTTP_HEADER_ROUTING_TOKEN`.
ROUTING_HEADER = "X-Qdrant-Route-Affinity"
POINT_ID = 1
VECTOR = [0.1, 0.2, 0.3, 0.4]


def upsert_marker(peer_uri, marker):
    """Upsert the single test point with the given `marker` payload on all replicas."""
    r = upsert_points(
        peer_uri,
        [{"id": POINT_ID, "vector": VECTOR, "payload": {"marker": marker}}],
        wait="true",
        ordering="strong",
    )
    assert_http_ok(r)


def retrieve_marker(peer_uri, headers={}):
    """Retrieve the test point and return its `marker`, routing with the given headers."""
    r = requests.post(
        f"{peer_uri}/collections/{COLLECTION_NAME}/points",
        json={"ids": [POINT_ID], "with_payload": True},
        headers=headers,
        timeout=10,
    )
    assert_http_ok(r)
    points = r.json()["result"]
    return points[0]["payload"]["marker"] if points else None


def create_snapshot(peer_uri):
    r = requests.post(
        f"{peer_uri}/collections/{COLLECTION_NAME}/snapshots?wait=true",
        timeout=30,
    )
    assert_http_ok(r)
    return r.json()["result"]["name"]


def recover_snapshot_no_sync(peer_uri, snapshot_url):
    # `no_sync` keeps the recovered data authoritative on this peer *without* removing
    # or re-syncing the other replica, leaving the two replicas in a divergent (but
    # both Active) state — exactly the situation the routing token addresses.
    r = requests.put(
        f"{peer_uri}/collections/{COLLECTION_NAME}/snapshots/recover",
        json={"location": snapshot_url, "priority": "no_sync"},
        timeout=60,
    )
    assert_http_ok(r)


def split_by_local_replica(peer_api_uris):
    """Split peers into those that hold a local replica of shard 0 and those that don't."""
    hosts, non_hosts = [], []
    for uri in peer_api_uris:
        r = requests.get(f"{uri}/collections/{COLLECTION_NAME}/cluster", timeout=10)
        assert_http_ok(r)
        local_shards = r.json()["result"]["local_shards"]
        if any(shard["shard_id"] == 0 for shard in local_shards):
            hosts.append(uri)
        else:
            non_hosts.append(uri)
    return hosts, non_hosts


def test_routing_token_sticky_reads(tmp_path: pathlib.Path):
    """
    A routing token pins a read to a deterministic replica.

    We build two replicas of one shard with *divergent* data: snapshot state "A",
    move both replicas to state "B", then recover the "A" snapshot onto a single
    replica with `no_sync` (so the two replicas stay Active but divergent). Reads go
    through a third peer that holds no local replica, so it must route to one of the
    two remote replicas. We then validate:

      1. without a routing token, repeated reads vary between the replicas;
      2. with a routing token, repeated reads are stable;
      3. different routing tokens can land on different replicas.
    """
    assert_project_root()

    peer_api_uris, _peer_dirs, _bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    # One shard with two replicas (placed on two of the three peers).
    # - `write_consistency_factor=2` so every write reaches *both* replicas before we diverge them.
    # - `read_fan_out_factor=0` so each read hits exactly one replica (no fan-out),
    #   which is what makes the routing decision observable.
    create_collection(
        peer_api_uris[0],
        shard_number=1,
        replication_factor=2,
        write_consistency_factor=2,
        sparse_vectors=False,
        read_fan_out_factor=0,
    )
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )

    # Both replicas start in state "A".
    upsert_marker(peer_api_uris[0], "A")

    hosts, non_hosts = split_by_local_replica(peer_api_uris)
    assert len(hosts) == 2, f"expected 2 replica hosts, got {hosts}"
    assert len(non_hosts) == 1, f"expected 1 peer without a local replica, got {non_hosts}"
    peer_a, peer_b = hosts
    # The coordinator holds no local replica, so reads issued here cannot be answered
    # locally and must be routed to one of the two remote replicas.
    coordinator = non_hosts[0]

    # Snapshot state "A" on peer_a, then move *both* replicas to state "B".
    snapshot_name = create_snapshot(peer_a)
    upsert_marker(peer_api_uris[0], "B")

    # Recover the "A" snapshot onto peer_a only, without resync:
    #   peer_a -> "A", peer_b -> "B"  (both Active, divergent)
    snapshot_url = f"{peer_a}/collections/{COLLECTION_NAME}/snapshots/{snapshot_name}"
    recover_snapshot_no_sync(peer_a, snapshot_url)
    wait_for_all_replicas_active(peer_a, COLLECTION_NAME)
    wait_for_all_replicas_active(coordinator, COLLECTION_NAME)

    # Precondition: the replicas are genuinely divergent (local reads differ).
    assert retrieve_marker(peer_a) == "A", "snapshot recovery did not diverge peer_a"
    assert retrieve_marker(peer_b) == "B", "peer_b unexpectedly changed state"

    n_reads = 40

    # 1. No routing token: reads are randomly routed, so both replicas are observed.
    without_token = {retrieve_marker(coordinator) for _ in range(n_reads)}
    assert without_token == {"A", "B"}, (
        f"expected varying results without a routing token, observed {without_token}"
    )

    # 2. With a routing token: reads are pinned to one replica, so the result is stable.
    sticky_headers = {ROUTING_HEADER: "user-sticky"}
    with_token = {
        retrieve_marker(coordinator, headers=sticky_headers) for _ in range(n_reads)
    }
    assert len(with_token) == 1, (
        f"expected a stable result with a routing token, observed {with_token}"
    )

    # 3. Different routing tokens can land on different replicas. Each token maps
    #    deterministically to a replica via `hash(token, peer_id)`; since peer ids are
    #    random per cluster, use enough tokens to make an all-same-replica split
    #    vanishingly unlikely (~2 * 0.5**32).
    per_token = {
        retrieve_marker(coordinator, headers={ROUTING_HEADER: f"user-{i}"})
        for i in range(32)
    }
    assert per_token == {"A", "B"}, (
        f"expected different routing tokens to reach different replicas, observed {per_token}"
    )
