import pathlib

from .fixtures import create_collection, upsert_random_points
from .test_resharding import all_replicas, start_resharding
from .utils import *

COLLECTION = "test_collection"
# Keep the copy running for the whole test, so its target stays `Partial`
ENV = {
    "QDRANT__CLUSTER__RESHARDING_ENABLED": "true",
    "QDRANT_STAGING_SHARD_TRANSFER_DELAY_SEC": "600",
}


def shard_states(uri: str, shard_id: int) -> dict:
    info = get_collection_cluster_info(uri, COLLECTION)
    return {r["peer_id"]: r["state"] for r in all_replicas(info) if r["shard_id"] == shard_id}


def test_reshard_down_migrate_into_copying_replica(tmp_path: pathlib.Path):
    peer_uris, _, _ = start_cluster(tmp_path, 3, extra_env=ENV)
    skip_if_no_feature(peer_uris[0], "staging")
    wait_for(all_peers_are_voters, peer_uris)
    peer_ids = [get_cluster_info(uri)["peer_id"] for uri in peer_uris]

    create_collection(peer_uris[0], collection=COLLECTION, shard_number=2, replication_factor=1, sparse_vectors=False)
    wait_collection_exists_and_active_on_all_peers(COLLECTION, peer_uris)
    upsert_random_points(peer_uris[0], 10, collection_name=COLLECTION, with_sparse_vector=False)

    a = next(iter(shard_states(peer_uris[0], 0)))
    b = next(iter(shard_states(peer_uris[0], 1)))
    c = next(p for p in peer_ids if p not in (a, b))
    uri_b = peer_uris[peer_ids.index(b)]

    # Copy shard 0 into C; C stays `Partial` while the copy is delayed
    replicate_shard(uri_b, COLLECTION, 0, a, c, method="stream_records")
    wait_for(lambda: shard_states(uri_b, 0).get(c) == "Partial")

    assert_http_ok(start_resharding(uri_b, COLLECTION, direction="down", peer_id=b))
    wait_for_collection_resharding_operations_count(uri_b, COLLECTION, 1)

    migrate = requests.post(f"{uri_b}/collections/{COLLECTION}/cluster", json={
        "replicate_shard": {
            "shard_id": 1, "to_shard_id": 0, "from_peer_id": b, "to_peer_id": c,
            "method": "resharding_stream_records",
        }
    })
    assert migrate.status_code == 400, migrate.text

    wait_for_same_applied_commit(peer_uris)
    for uri in peer_uris:
        assert shard_states(uri, 0) == {a: "Active", c: "Partial"}
