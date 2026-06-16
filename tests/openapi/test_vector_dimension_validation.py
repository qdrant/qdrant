"""
Test that vector dimension validation works for both sync (wait=true) and async (wait=false) upserts.

Regression test for https://github.com/qdrant/qdrant/issues/9039
"""

import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(collection_name):
    drop_collection(collection_name=collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 4,
                "distance": "Cosine",
            },
        }
    )
    assert response.ok

    yield
    drop_collection(collection_name=collection_name)


def test_sync_upsert_wrong_dimension_rejected(collection_name):
    """With wait=true, wrong-dimension vectors should be rejected with 4xx."""
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {"id": 1, "vector": [0.1, 0.2, 0.3]}  # 3-dim into 4-dim
            ]
        }
    )
    assert not response.ok
    assert response.status_code == 400
    assert "dimension" in response.json()["status"]["error"].lower()


def test_async_upsert_wrong_dimension_rejected(collection_name):
    """With wait=false (default), wrong-dimension vectors should also be rejected with 4xx."""
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'false'},
        body={
            "points": [
                {"id": 2, "vector": [0.1, 0.2, 0.3]}  # 3-dim into 4-dim
            ]
        }
    )
    assert not response.ok
    assert response.status_code == 400
    assert "dimension" in response.json()["status"]["error"].lower()


def test_async_upsert_correct_dimension_accepted(collection_name):
    """With wait=false, correct-dimension vectors should still succeed."""
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'false'},
        body={
            "points": [
                {"id": 3, "vector": [0.1, 0.2, 0.3, 0.4]}  # 4-dim into 4-dim
            ]
        }
    )
    assert response.ok


def test_async_batch_upsert_wrong_dimension_rejected(collection_name):
    """Batch upsert with wait=false should also reject wrong-dimension vectors."""
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'false'},
        body={
            "batch": {
                "ids": [10, 11],
                "vectors": [
                    [0.1, 0.2, 0.3, 0.4],  # correct
                    [0.1, 0.2, 0.3],        # wrong dimension
                ]
            }
        }
    )
    assert not response.ok
    assert response.status_code == 400
    assert "dimension" in response.json()["status"]["error"].lower()
