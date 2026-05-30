import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_sparse_vector_validation'


@pytest.fixture(autouse=True)
def setup():
    sparse_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def sparse_collection_setup(collection_name='test_collection'):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "sparse_vectors": {
                "text": { }
            },
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok


def test_sparse_vector_validations():
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {
                        "text": {"indices": [100, 500], "values": [0.9, 0.8, 0.5]}
                    }
                },
            ]
        }
    )
    assert not response.ok
    assert 'Validation error' in response.json()["status"]["error"]
    assert 'points[0].vector.?.values: Validation error: must be the same length as indices [{}]' in response.json()["status"]["error"]

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {
                        "text": {"indices": [100, 500, 500], "values": [0.9, 0.8, 0.5]}
                    }
                },
            ]
        }
    )
    assert not response.ok
    assert 'Validation error' in response.json()["status"]["error"]
    assert 'points[0].vector.?.indices: Validation error: must be unique [{}]' in response.json()["status"]["error"]
