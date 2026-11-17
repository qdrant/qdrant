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


def markers_by_peer(peer_api_uris, headers={}):
    """Retrieve the marker through *every* peer, returning a {uri: marker} map."""
    return {uri: retrieve_marker(uri, headers=headers) for uri in peer_api_uris}


def retrieve_marker_on_all_peers(peer_api_uris, headers={}):
    """Retrieve the marker through *every* peer and assert they all agree.

    Routing is deterministic: a token is ordered by `hash(token, peer_id)`, which is
    identical on every node, so every peer must resolve the token to the same replica
    no matter which peer received the request. Returns that single agreed-upon marker.
    """
    by_peer = markers_by_peer(peer_api_uris, headers=headers)
    distinct = set(by_peer.values())
    assert len(distinct) == 1, f"peers disagreed on the routed replica: {by_peer}"
    return distinct.pop()


def wait_for_stable_routing(peer_api_uris, tokens):
    """Wait until every peer agrees on the routed replica for each of `tokens`.

    Right after a `no_sync` snapshot recovery, the recovered replica serves *local*
    reads immediately, but there is a brief window where a *remote* read to it can
    transiently fail. When that happens the read path falls back to the next replica
    in hash order on just the requesting peer, so a token momentarily resolves to
    different replicas across peers (e.g. `{A, B, B}`). This is expected fallback
    behaviour, not a routing bug, so poll until that window has passed before we
    assert determinism. Tokens are checked without the agreement assertion so the
    poll can retry instead of failing.
    """
    def all_tokens_agree():
        for token in tokens:
            by_peer = markers_by_peer(peer_api_uris, headers={ROUTING_HEADER: token})
            if len(set(by_peer.values())) != 1:
                return False
        return True

    wait_for(all_tokens_agree)


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


def replicas_diverged(peer_a, peer_b):
    """True once peer_a serves the recovered "A" and peer_b still serves "B".

    Read locally on each replica host (no token, so the local replica is preferred),
    confirming the two replicas hold the divergent state the test relies on.
    """
    return retrieve_marker(peer_a) == "A" and retrieve_marker(peer_b) == "B"


def test_routing_token_sticky_reads(tmp_path: pathlib.Path):
    """
    A routing token pins a read to a deterministic replica, and the same token lands
    on the same replica regardless of which peer received the request.

    We build two replicas of one shard with *divergent* data: snapshot state "A",
    move both replicas to state "B", then recover the "A" snapshot onto a single
    replica with `no_sync` (so the two replicas stay Active but divergent). We then
    validate, issuing reads through *every* peer (not just the coordinator):

      1. without a routing token, a peer holding no local replica routes reads
         randomly, so repeated reads vary between the replicas (the baseline);
      2. with a routing token, every peer resolves it to the same replica, so reads
         are stable and identical across all peers;
      3. different routing tokens can land on different replicas, yet each individual
         token resolves to the same replica on every peer.
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
    # Reads are routed through every peer below, so every peer must see both replicas
    # as Active before we start (otherwise a not-yet-readable replica would be skipped
    # and break the deterministic routing we are asserting).
    for uri in peer_api_uris:
        wait_for_all_replicas_active(uri, COLLECTION_NAME)

    # Precondition: the replicas are genuinely divergent (local reads differ). Poll
    # rather than asserting immediately — `wait_for_all_replicas_active` only confirms
    # the replica *metadata* is Active, but there is a brief window afterwards where
    # peer_a still serves its pre-recovery "B" before the recovered "A" becomes
    # readable on the read path.
    wait_for(replicas_diverged, peer_a, peer_b)

    # `replicas_diverged` only confirms each replica is readable *locally*. A remote
    # read to the freshly recovered peer_a can still transiently fail for a short
    # while, and the read path then falls back to the other replica in hash order on
    # just the requesting peer — making a single token look like it resolves to
    # different replicas across peers. Wait until token-routed reads are stable
    # across all peers for every token we are about to assert on.
    sticky_token = "user-sticky"
    per_token_tokens = [f"user-{i}" for i in range(32)]
    wait_for_stable_routing(peer_api_uris, [sticky_token, *per_token_tokens])

    n_reads = 40

    # 1. No routing token: the coordinator routes each read to a random replica, so
    #    both are observed. (The non-determinism is only visible on the coordinator;
    #    the two replica-holding peers always answer such reads from their local copy.)
    without_token = {retrieve_marker(coordinator) for _ in range(n_reads)}
    assert without_token == {"A", "B"}, (
        f"expected varying results without a routing token, observed {without_token}"
    )

    # 2. With a routing token: the read is pinned to one replica, and *every* peer
    #    resolves the token to the same replica (`retrieve_marker_on_all_peers` asserts
    #    the peers agree). Repeated rounds must therefore yield a single stable marker.
    sticky_headers = {ROUTING_HEADER: sticky_token}
    with_token = {
        retrieve_marker_on_all_peers(peer_api_uris, headers=sticky_headers)
        for _ in range(n_reads)
    }
    assert len(with_token) == 1, (
        f"expected a stable result across all peers with a routing token, observed {with_token}"
    )

    # 3. Different routing tokens can land on different replicas, yet each individual
    #    token resolves to the same replica on every peer (asserted per token inside
    #    `retrieve_marker_on_all_peers`). Each token maps deterministically to a replica
    #    via `hash(token, peer_id)`; since peer ids are random per cluster, use enough
    #    tokens to make an all-same-replica split vanishingly unlikely (~2 * 0.5**32).
    per_token = {
        retrieve_marker_on_all_peers(peer_api_uris, headers={ROUTING_HEADER: token})
        for token in per_token_tokens
    }
    assert per_token == {"A", "B"}, (
        f"expected different routing tokens to reach different replicas, observed {per_token}"
    )
