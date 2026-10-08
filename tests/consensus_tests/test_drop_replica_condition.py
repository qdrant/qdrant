import pathlib

import requests

from .assertions import assert_http_ok
from .fixtures import create_collection
from .utils import *

COLLECTION = "test_collection"


def shard_0_replicas(peer_uri):
    info = get_collection_cluster_info(peer_uri, COLLECTION)
    local = [info["peer_id"] for s in info["local_shards"] if s["shard_id"] == 0]
    remote = [s["peer_id"] for s in info["remote_shards"] if s["shard_id"] == 0]
    return local + remote


def drop_replica(peer_uri, peer_id, min_other_active_replicas=None):
    body = {"shard_id": 0, "peer_id": peer_id}
    if min_other_active_replicas is not None:
        body["min_other_active_replicas"] = min_other_active_replicas
    return requests.post(
        f"{peer_uri}/collections/{COLLECTION}/cluster?wait=true", json={"drop_replica": body}
    )


def test_drop_replica_min_other_active_replicas(tmp_path: pathlib.Path):
    peer_uris, _, _ = start_cluster(tmp_path, 3)

    create_collection(peer_uris[0], COLLECTION, shard_number=1, replication_factor=3)
    wait_collection_exists_and_active_on_all_peers(COLLECTION, peer_uris)

    replicas = shard_0_replicas(peer_uris[0])
    assert len(replicas) == 3
    target = replicas[0]

    # Condition not met: only 2 other replicas exist. Refused as a user error, state unchanged.
    resp = drop_replica(peer_uris[0], target, min_other_active_replicas=3)
    assert resp.status_code == 400, resp.text
    assert "at least 3 are required" in resp.text
    assert len(shard_0_replicas(peer_uris[0])) == 3

    # The refusal does not block consensus: the next operation applies.
    resp = drop_replica(peer_uris[0], target, min_other_active_replicas=2)
    assert_http_ok(resp)
    assert target not in shard_0_replicas(peer_uris[0])

    # Every peer sees the same state.
    expected_replicas = sorted(r for r in replicas if r != target)
    wait_for(
        lambda: all(
            sorted(shard_0_replicas(uri)) == expected_replicas for uri in peer_uris
        )
    )

    # Without a condition, behavior is unchanged.
    resp = drop_replica(peer_uris[0], replicas[1])
    assert_http_ok(resp)
    assert len(shard_0_replicas(peer_uris[0])) == 1
