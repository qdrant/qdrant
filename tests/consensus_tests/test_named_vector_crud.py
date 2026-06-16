import pathlib

import requests

from .fixtures import create_collection, upsert_random_points
from .test_shard_transfer_deferred import VECTOR_DIM
from .utils import *

N_PEERS = 3
COLLECTION_NAME = "test_vector_crud"


def create_vector_name(peer_url, collection_name, vector_name, config, timeout=10, wait=True):
    """Create a named vector via PUT /collections/{name}/vectors/{vector_name}"""
    r = requests.put(
        f"{peer_url}/collections/{collection_name}/vectors/{vector_name}?timeout={timeout}&wait={'true' if wait else 'false'}",
        json=config,
    )
    assert_http_ok(r)
    return r.json()


def delete_vector_name(peer_url, collection_name, vector_name, timeout=10, wait=True):
    """Delete a named vector via DELETE /collections/{name}/vectors/{vector_name}"""
    r = requests.delete(
        f"{peer_url}/collections/{collection_name}/vectors/{vector_name}?timeout={timeout}&wait={'true' if wait else 'false'}",
    )
    assert_http_ok(r)
    return r.json()


def get_collection_vectors_config(peer_url, collection_name):
    """Get the vectors configuration from collection info."""
    info = get_collection_info(peer_url, collection_name)
    return info.get("config", {}).get("params", {}).get("vectors", {})


def get_collection_sparse_vectors_config(peer_url, collection_name):
    """Get the sparse vectors configuration from collection info."""
    info = get_collection_info(peer_url, collection_name)
    return info.get("config", {}).get("params", {}).get("sparse_vectors", {})


def wait_collection_vector_config(peer_url, collection_name, vector_name, expected_size):
    """Wait until a peer's collection config contains the given vector with the expected size."""
    def check():
        vectors = get_collection_vectors_config(peer_url, collection_name)
        return vector_name in vectors and vectors[vector_name].get("size") == expected_size

    wait_for(check)


def get_optimizer_status(peer_url, collection_name):
    """Get optimizer status from collection info."""
    info = get_collection_info(peer_url, collection_name)
    return info.get("status", {})


def test_create_vector_no_optimization(tmp_path: pathlib.Path):
    """
    Test that creating named vectors does not trigger segment optimization.

    1. Create cluster, create collection, upload 1000 points.
    2. Set indexing threshold low to trigger indexing, wait for green.
    3. Record segment count.
    4. Create a new dense named vector.
    5. Assert segment count unchanged (no optimization triggered).
    6. Create a new sparse named vector.
    7. Assert segment count unchanged (no optimization triggered).
    """

    VECTOR_DIM = 64
    VECTOR_DIM2 = 99
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    # Create collection with low indexing threshold to trigger indexing
    r = requests.put(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}?timeout=30",
        json={
            "vectors": {"default": {"size": VECTOR_DIM, "distance": "Cosine"}},
            "shard_number": 1,
            "replication_factor": 1,
            "optimizers_config": {
                "indexing_threshold": 100,
            },
        },
    )
    assert_http_ok(r)

    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )

    # Upload 1000 points
    for i in range(10):
        points = [
            {
                "id": i * 100 + j,
                "vector": {"default": [float(x) / 1000 for x in range(VECTOR_DIM)] },
                "payload": {"idx": i * 100 + j},
            }
            for j in range(100)
        ]
        r = requests.put(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points?wait=true",
            json={"points": points},
        )
        assert_http_ok(r)

    # Wait for indexing to complete (collection goes green)
    wait_collection_green(peer_api_uris[0], COLLECTION_NAME)

    # Record segment state after indexing
    status_before_creation = get_optimizer_status(peer_api_uris[0], COLLECTION_NAME)
    print(f"Segments after indexing: {status_before_creation}")

    # Create a new dense named vector
    create_vector_name(
        peer_api_uris[0],
        COLLECTION_NAME,
        "new_dense",
        {"dense": {"size": VECTOR_DIM2, "distance": "Dot"}},
    )

    # Verify no optimization was triggered - segment count should be unchanged
    status_after_creation = get_optimizer_status(peer_api_uris[0], COLLECTION_NAME)
    print(f"Segments after creating dense vector: {status_after_creation}")
    assert status_after_creation == status_before_creation, (
        f"Segment count changed after creating dense vector: {status_before_creation} -> {status_after_creation}"
    )

    # Verify the new vector exists in collection config on all peers
    for uri in peer_api_uris:
        vectors = get_collection_vectors_config(uri, COLLECTION_NAME)
        assert "new_dense" in vectors, f"new_dense not in vectors config on {uri}"
        assert vectors["new_dense"]["size"] == VECTOR_DIM2

    # Create a new sparse named vector
    create_vector_name(
        peer_api_uris[0],
        COLLECTION_NAME,
        "new_sparse",
        {"sparse": {}},
    )

    # Verify no optimization was triggered - segment count should be unchanged
    status_after_creation = get_optimizer_status(peer_api_uris[0], COLLECTION_NAME)
    print(f"Segments after creating sparse vector: {status_after_creation}")
    assert status_after_creation == status_before_creation, (
        f"Segment count changed after creating sparse vector: {status_before_creation} -> {status_after_creation}"
    )

    # Verify sparse vector exists on all peers
    for uri in peer_api_uris:
        sparse = get_collection_sparse_vectors_config(uri, COLLECTION_NAME)
        assert "new_sparse" in sparse, f"new_sparse not in sparse vectors config on {uri}"


def test_vector_crud_with_consensus_snapshot(tmp_path: pathlib.Path):
    """
    Test that named vector create/delete survives consensus snapshot recovery.

    1. Create cluster with aggressive WAL compaction (forces consensus snapshots).
    2. Create collection, upload 1000 points.
    3. Kill one node.
    4. Delete the original vector, create a new one with different dimensions.
    5. Restart the killed node — it must recover via consensus snapshot.
    6. Verify the restarted node has the new vector config.
    """
    assert_project_root()

    VECTOR_NAME = "v1"
    VECTOR_DIM = 64
    VECTOR_DIM2 = 78

    env = {
        # Force consensus snapshot by aggressively compacting WAL
        "QDRANT__CLUSTER__CONSENSUS__COMPACT_WAL_ENTRIES": "1",
    }

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path, N_PEERS, extra_env=env
    )

    # Create collection with a named dense vector VECTOR_NAME
    r = requests.put(
        f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}?timeout=30",
        json={
            "vectors": {
                VECTOR_NAME: {"size": VECTOR_DIM, "distance": "Cosine"},
            },
            "shard_number": 1,
            "replication_factor": 3,
        },
    )
    assert_http_ok(r)

    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_api_uris
    )

    # Upload 1000 points with v1
    for i in range(10):
        points = [
            {
                "id": i * 100 + j,
                "vector": {VECTOR_NAME: [float(x) / 1000 for x in range(VECTOR_DIM)]},
                "payload": {"idx": i * 100 + j},
            }
            for j in range(100)
        ]
        r = requests.put(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points?wait=true",
            json={"points": points},
        )
        assert_http_ok(r)

    # Verify all 1000 points on all peers
    for uri in peer_api_uris:
        wait_collection_points_count(uri, COLLECTION_NAME, 1000)

    # Kill the last peer
    killed_peer = processes.pop()
    restart_port = killed_peer.p2p_port
    killed_peer.kill()
    print(f"Killed peer at port {killed_peer.http_port}")

    # Perform some consensus operations to trigger WAL compaction + snapshot.
    # Delete vector v1 and create v2 with different dimensions.
    # Use a short timeout because a peer is down — the server will still await
    # consensus sync across all peers and hit the client timeout on the dead one.
    delete_vector_name(peer_api_uris[0], COLLECTION_NAME, VECTOR_NAME, wait=False, timeout=2)

    # Verify v1 is gone on surviving peers
    for uri in peer_api_uris[:-1]:
        vectors = get_collection_vectors_config(uri, COLLECTION_NAME)
        assert VECTOR_NAME not in vectors, f"{VECTOR_NAME} should be deleted on {uri}"

    # Create VECTOR_NAME with different dimensions
    create_vector_name(
        peer_api_uris[0],
        COLLECTION_NAME,
        VECTOR_NAME,
        {"dense": {"size": VECTOR_DIM2, "distance": "Dot"}},
        timeout=2,
    )

    # Verify v2 exists on surviving peers
    for uri in peer_api_uris[:-1]:
        vectors = get_collection_vectors_config(uri, COLLECTION_NAME)
        assert VECTOR_NAME in vectors, f"{VECTOR_NAME} not in config on {uri}"
        assert vectors[VECTOR_NAME]["size"] == VECTOR_DIM2

    # Do a few more consensus operations to ensure WAL compaction triggers snapshot
    for _ in range(5):
        create_vector_name(
            peer_api_uris[0], COLLECTION_NAME, "tmp_vec",
            {"dense": {"size": 2, "distance": "Cosine"}},
            timeout=2,
        )
        delete_vector_name(peer_api_uris[0], COLLECTION_NAME, "tmp_vec", timeout=2)


    # Upload 200 points with v1
    for i in range(2):
        points = [
            {
                "id": i * 100 + j,
                "vector": {
                    VECTOR_NAME: [float(x) / 1000 for x in range(VECTOR_DIM2)]

                },
                "payload": {"idx": i * 100 + j},
            }
            for j in range(100)
        ]
        r = requests.put(
            f"{peer_api_uris[0]}/collections/{COLLECTION_NAME}/points?wait=true",
            json={"points": points},
        )
        assert_http_ok(r)

    # Restart the killed peer — it should recover via consensus snapshot
    new_url = start_peer(
        peer_dirs[-1], "peer_restarted.log", bootstrap_uri, port=restart_port, extra_env=env
    )
    peer_api_uris[-1] = new_url

    wait_all_peers_up([new_url])
    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=[new_url]
    )

    # Wait for the restarted node to sync the correct vector config via consensus
    wait_collection_vector_config(new_url, COLLECTION_NAME, VECTOR_NAME, VECTOR_DIM2)

    # Verify point count is still correct
    wait_collection_points_count(new_url, COLLECTION_NAME, 1000)

    # Verify the restarted peer actually stores vectors with the new schema.
    # Points 0–199 were upserted with VECTOR_DIM2 after the delete+create;
    # their vectors must be retrievable with the correct dimensionality.
    r = requests.post(
        f"{new_url}/collections/{COLLECTION_NAME}/points/scroll",
        json={
            "limit": 10,
            "with_vector": [VECTOR_NAME],
            "filter": {"must": [{"key": "idx", "range": {"lte": 9}}]},
        },
    )
    assert_http_ok(r)
    scroll_result = r.json()["result"]["points"]
    assert len(scroll_result) > 0, "Expected at least one point from scroll"

    for point in scroll_result:
        vec = point.get("vector", {}).get(VECTOR_NAME)
        assert vec is not None, (
            f"Point {point['id']} on restarted peer has no vector '{VECTOR_NAME}'"
        )
        assert len(vec) == VECTOR_DIM2, (
            f"Point {point['id']}: expected dim {VECTOR_DIM2}, got {len(vec)}"
        )

    # Also verify that a search with the new dimensionality works on the restarted peer
    r = requests.post(
        f"{new_url}/collections/{COLLECTION_NAME}/points/search",
        json={
            "vector": {
                "name": VECTOR_NAME,
                "vector": [0.1] * VECTOR_DIM2,
            },
            "limit": 5,
        },
    )
    assert_http_ok(r)
    search_result = r.json()["result"]
    assert len(search_result) > 0, (
        "Search with new vector schema returned no results on restarted peer"
    )
