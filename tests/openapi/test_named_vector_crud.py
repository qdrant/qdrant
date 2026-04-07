import pytest
import time
import requests

from .helpers.helpers import request_with_validation
from .helpers.settings import QDRANT_HOST


VECTOR_SIZE1 = 4
VECTOR_SIZE2 = 8


@pytest.fixture(autouse=True, scope="module")
def setup(collection_name):
    # Drop if exists
    requests.delete(f"{QDRANT_HOST}/collections/{collection_name}")

    # Create collection with two named vectors
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "vec_a": {"size": VECTOR_SIZE1, "distance": "Cosine"},
                "vec_b": {"size": VECTOR_SIZE2, "distance": "Cosine"},
            },
            "optimizers_config": {
                "indexing_threshold": 1,
            },
        },
    )
    assert response.ok
    yield
    requests.delete(f"{QDRANT_HOST}/collections/{collection_name}")


def test_delete_recreate_vector_scroll(collection_name):
    """
    Deleting a named vector and recreating it must not break scroll.

    Reproduces a bug where EmptyDenseVectorStorage on immutable segments
    has total_vector_count=0 while points exist, causing a panic
    in vectors_by_offsets during scroll/retrieve.
    """
    # Upsert points with both vectors
    points = [
        {
            "id": i,
            "vector": {
                "vec_a": [float(x) / 100 for x in range(VECTOR_SIZE1)],
                "vec_b": [float(x) / 100 for x in range(VECTOR_SIZE2)],
            },
            "payload": {"idx": i},
        }
        for i in range(200)
    ]
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"points": points},
    )
    assert response.ok

    # Delete vec_a (use plain requests — OpenAPI response schema is mismatched)
    response = requests.delete(
        f"{QDRANT_HOST}/collections/{collection_name}/vectors/vec_a",
        params={'wait': 'true'},
    )
    assert response.ok

    # Recreate vec_a with different dimensions
    response = requests.put(
        f"{QDRANT_HOST}/collections/{collection_name}/vectors/vec_a",
        params={'wait': 'true'},
        json={"dense": {"size": 6, "distance": "Dot"}},
    )
    assert response.ok

    # Scroll with vectors — must not panic
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={"limit": 10, "with_vector": True},
    )
    assert response.ok
    result = response.json()['result']
    assert len(result['points']) == 10

    for point in result['points']:
        assert 'vec_b' in point['vector'], f"vec_b missing from point {point['id']}"
        assert len(point['vector']['vec_b']) == VECTOR_SIZE2


def test_delete_recreate_indexed_vector_scroll():
    """
    Delete and recreate a named vector on indexed segments (HNSW built),
    then scroll, upsert new data, and scroll again.
    """
    _run_delete_recreate_scroll(wait_for_indexing=True)


def test_delete_recreate_unindexed_vector_scroll():
    """
    Delete and recreate a named vector without waiting for indexing,
    then scroll, upsert new data, and scroll again.
    """
    _run_delete_recreate_scroll(wait_for_indexing=False)


def _run_delete_recreate_scroll(wait_for_indexing: bool):
    coll = "test_named_vec_idx" if wait_for_indexing else "test_named_vec_noidx"
    dim_a = 128
    dim_b = 64
    dim_a_new = 96
    num_points = 200

    # Setup
    requests.delete(f"{QDRANT_HOST}/collections/{coll}")
    response = requests.put(
        f"{QDRANT_HOST}/collections/{coll}",
        json={
            "vectors": {
                "vec_a": {"size": dim_a, "distance": "Cosine"},
                "vec_b": {"size": dim_b, "distance": "Cosine"},
            },
            "optimizers_config": {
                "indexing_threshold": 10 if wait_for_indexing else 10_000,
            },
        },
    )
    assert response.ok

    # Upsert points
    points = [
        {
            "id": i,
            "vector": {
                "vec_a": [float(i * x % 97) / 100 for x in range(dim_a)],
                "vec_b": [float(i * x % 53) / 100 for x in range(dim_b)],
            },
        }
        for i in range(num_points)
    ]
    response = requests.put(
        f"{QDRANT_HOST}/collections/{coll}/points?wait=true",
        json={"points": points},
    )
    assert response.ok

    if wait_for_indexing:
        wait_collection_green(coll)

    # Delete vec_a
    response = requests.delete(
        f"{QDRANT_HOST}/collections/{coll}/vectors/vec_a?wait=true",
    )
    assert response.ok

    # Recreate vec_a with different dimensions
    response = requests.put(
        f"{QDRANT_HOST}/collections/{coll}/vectors/vec_a?wait=true",
        json={"dense": {"size": dim_a_new, "distance": "Dot"}},
    )
    assert response.ok

    # First scroll — must not panic
    response = requests.post(
        f"{QDRANT_HOST}/collections/{coll}/points/scroll",
        json={"limit": 10, "with_vector": True},
    )
    assert response.ok
    result = response.json()['result']
    assert len(result['points']) == 10

    for point in result['points']:
        assert 'vec_b' in point['vector']
        assert len(point['vector']['vec_b']) == dim_b

    # Upsert some points with data for the new vec_a
    updated_points = [
        {
            "id": i,
            "vector": {
                "vec_a": [float(i * x % 41) / 100 for x in range(dim_a_new)],
                "vec_b": [float(i * x % 53) / 100 for x in range(dim_b)],
            },
        }
        for i in range(50)
    ]
    response = requests.put(
        f"{QDRANT_HOST}/collections/{coll}/points?wait=true",
        json={"points": updated_points},
    )
    assert response.ok

    # Second scroll — verify updated points have vec_a data
    response = requests.post(
        f"{QDRANT_HOST}/collections/{coll}/points/scroll",
        json={"limit": num_points, "with_vector": True},
    )
    assert response.ok
    result = response.json()['result']
    assert len(result['points']) == num_points

    updated_ids = set(range(50))
    for point in result['points']:
        assert 'vec_b' in point['vector']
        assert len(point['vector']['vec_b']) == dim_b

        if point['id'] in updated_ids:
            assert 'vec_a' in point['vector'], f"vec_a missing from updated point {point['id']}"
            assert len(point['vector']['vec_a']) == dim_a_new

    # Cleanup
    requests.delete(f"{QDRANT_HOST}/collections/{coll}")


def wait_collection_green(collection_name, timeout=30):
    """Poll collection status until optimizer is idle."""
    start = time.time()
    while time.time() - start < timeout:
        r = requests.get(f"{QDRANT_HOST}/collections/{collection_name}")
        assert r.ok
        if r.json()['result']['status'] == 'green':
            return
        time.sleep(0.5)
    raise TimeoutError(f"Collection {collection_name} did not turn green within {timeout}s")
