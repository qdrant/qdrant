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
    request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )

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
    request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )


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

    # Delete vec_a
    response = request_with_validation(
        api='/collections/{collection_name}/vectors/{vector_name}',
        method="DELETE",
        path_params={'collection_name': collection_name, 'vector_name': 'vec_a'},
        query_params={'wait': 'true'},
    )
    assert response.ok

    # Recreate vec_a with different dimensions
    response = request_with_validation(
        api='/collections/{collection_name}/vectors/{vector_name}',
        method="PUT",
        path_params={'collection_name': collection_name, 'vector_name': 'vec_a'},
        query_params={'wait': 'true'},
        body={"dense": {"size": 6, "distance": "Dot"}},
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


# Input-validation tests below intentionally use plain `requests` because
# `request_with_validation` would reject the body client-side (size violates
# the spec's minimum/maximum), preventing the server-side rejection from
# being exercised.

def test_create_vector_rejects_zero_size(collection_name):
    """size: 0 must be rejected at the API boundary, not reach the storage layer."""
    response = requests.put(
        f"{QDRANT_HOST}/collections/{collection_name}/vectors/vec_zero",
        params={'wait': 'true'},
        json={"dense": {"size": 0, "distance": "Cosine"}},
    )
    assert response.status_code == 422, response.text
    assert "size" in response.text


def test_create_vector_rejects_oversize(collection_name):
    """size above 65536 must be rejected at the API boundary."""
    response = requests.put(
        f"{QDRANT_HOST}/collections/{collection_name}/vectors/vec_huge",
        params={'wait': 'true'},
        json={"dense": {"size": 65537, "distance": "Cosine"}},
    )
    assert response.status_code == 422, response.text
    assert "size" in response.text


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
    request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': coll},
    )
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': coll},
        body={
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
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': coll},
        query_params={'wait': 'true'},
        body={"points": points},
    )
    assert response.ok

    if wait_for_indexing:
        wait_collection_green(coll)

    # Delete vec_a
    response = request_with_validation(
        api='/collections/{collection_name}/vectors/{vector_name}',
        method="DELETE",
        path_params={'collection_name': coll, 'vector_name': 'vec_a'},
        query_params={'wait': 'true'},
    )
    assert response.ok

    # Recreate vec_a with different dimensions
    response = request_with_validation(
        api='/collections/{collection_name}/vectors/{vector_name}',
        method="PUT",
        path_params={'collection_name': coll, 'vector_name': 'vec_a'},
        query_params={'wait': 'true'},
        body={"dense": {"size": dim_a_new, "distance": "Dot"}},
    )
    assert response.ok

    # First scroll — must not panic
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': coll},
        body={"limit": 10, "with_vector": True},
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
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': coll},
        query_params={'wait': 'true'},
        body={"points": updated_points},
    )
    assert response.ok

    # Second scroll — verify updated points have vec_a data
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': coll},
        body={"limit": num_points, "with_vector": True},
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
    request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': coll},
    )


def wait_collection_green(collection_name, timeout=30):
    """Poll collection status until optimizer is idle."""
    start = time.time()
    while time.time() - start < timeout:
        r = request_with_validation(
            api='/collections/{collection_name}',
            method="GET",
            path_params={'collection_name': collection_name},
        )
        assert r.ok
        if r.json()['result']['status'] == 'green':
            return
        time.sleep(0.5)
    raise TimeoutError(f"Collection {collection_name} did not turn green within {timeout}s")
