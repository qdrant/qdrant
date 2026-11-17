import pathlib
import random

import requests

from .utils import *

N_PEERS = 3
COLLECTION_NAME = "test_collection"
VECTOR_SIZE = 64
NUM_POINTS = 200


def _random_vector(rng):
    return [rng.uniform(-1.0, 1.0) for _ in range(VECTOR_SIZE)]


def create_turbo4_collection(peer_url):
    r = requests.put(
        f"{peer_url}/collections/{COLLECTION_NAME}?timeout=10",
        json={
            # `turbo4` datatype => the primary vector storage keeps TurboQuant
            # encoded codes in-place. This is the "TQ vector storage" that shard
            # transfer must ship as raw bytes.
            "vectors": {
                "size": VECTOR_SIZE,
                "distance": "Dot",
                "datatype": "turbo4",
            },
            "shard_number": 1,
            "replication_factor": 1,
        },
    )
    assert_http_ok(r)


def upsert_turbo4_points(peer_url, num, seed=42):
    rng = random.Random(seed)
    points = [
        {"id": i, "vector": _random_vector(rng)}
        for i in range(num)
    ]
    r = requests.put(
        f"{peer_url}/collections/{COLLECTION_NAME}/points?wait=true",
        json={"points": points},
    )
    assert_http_ok(r)


def scroll_vectors(peer_url):
    """Return {id: vector} for all points, read from `peer_url`'s local replica."""
    r = requests.post(
        f"{peer_url}/collections/{COLLECTION_NAME}/points/scroll",
        json={
            "limit": NUM_POINTS + 1,
            "with_vectors": True,
            "with_payload": False,
        },
    )
    assert_http_ok(r)
    points = r.json()["result"]["points"]
    return {p["id"]: p["vector"] for p in points}


def exact_count(peer_url):
    r = requests.post(
        f"{peer_url}/collections/{COLLECTION_NAME}/points/count",
        json={"exact": True},
    )
    assert_http_ok(r)
    return r.json()["result"]["count"]


# Transfer a shard whose vectors use the `turbo4` (TurboQuant) storage datatype,
# using the `stream_records` method, and assert the transferred vectors are
# byte-identical on the receiver.
#
# `stream_records` reads points from the source and re-inserts them on the
# receiver. For a `turbo4` vector storage the decoded-float path would decode
# TurboQuant codes to `f32` on the source and re-encode them on the receiver,
# drifting the stored codes and degrading recall (the TQ round-trip bug).
#
# The raw-vector transfer path ships the storage-native TurboQuant codes
# verbatim, so both replicas decode identical codes and scroll returns identical
# vectors. This test asserts exactly that: any drift (e.g. if the raw path
# regressed to the decoded-float path) would make the per-id vectors differ.
def test_shard_stream_transfer_turbo4_raw(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_turbo4_collection(peer_api_uris[0])
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_api_uris,
    )

    upsert_turbo4_points(peer_api_uris[0], NUM_POINTS)

    # Find the peer that currently holds the single shard, and pick another as
    # the transfer target.
    source_index = None
    for index, uri in enumerate(peer_api_uris):
        info = get_collection_cluster_info(uri, COLLECTION_NAME)
        if len(info["local_shards"]) == 1:
            source_index = index
            break
    assert source_index is not None, "No peer holds the shard"
    target_index = (source_index + 1) % N_PEERS

    source_uri = peer_api_uris[source_index]
    target_uri = peer_api_uris[target_index]
    source_info = get_collection_cluster_info(source_uri, COLLECTION_NAME)
    target_info = get_collection_cluster_info(target_uri, COLLECTION_NAME)

    from_peer_id = source_info["peer_id"]
    to_peer_id = target_info["peer_id"]
    shard_id = source_info["local_shards"][0]["shard_id"]

    # Snapshot the source vectors before the transfer.
    source_vectors = scroll_vectors(source_uri)
    assert len(source_vectors) == NUM_POINTS

    # Replicate the shard to the target peer using stream records.
    r = requests.post(
        f"{source_uri}/collections/{COLLECTION_NAME}/cluster",
        json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": from_peer_id,
                "to_peer_id": to_peer_id,
                "method": "stream_records",
            }
        },
    )
    assert_http_ok(r)

    # Wait for the transfer to finish.
    wait_for_collection_shard_transfers_count(source_uri, COLLECTION_NAME, 0)

    target_info = get_collection_cluster_info(target_uri, COLLECTION_NAME)
    assert len(target_info["local_shards"]) == 1, "Shard must be replicated to target peer"

    # Point counts must be consistent on both replicas.
    assert exact_count(source_uri) == NUM_POINTS
    assert exact_count(target_uri) == NUM_POINTS

    # The receiver must have byte-identical turbo4-decoded vectors. If the raw
    # path had regressed to a decoded-float transfer, re-encoding on the
    # receiver would drift the codes and these would differ.
    target_vectors = scroll_vectors(target_uri)
    assert set(target_vectors) == set(source_vectors), "Transferred point ids must match"

    mismatches = [
        point_id
        for point_id, vector in source_vectors.items()
        if target_vectors[point_id] != vector
    ]
    assert not mismatches, (
        f"{len(mismatches)} transferred turbo4 vectors drifted "
        f"(e.g. id {mismatches[:5]}); raw vector transfer must be lossless"
    )
