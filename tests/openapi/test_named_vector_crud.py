import pytest
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
