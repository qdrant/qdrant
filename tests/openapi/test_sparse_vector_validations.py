import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(collection_name):
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
                        "text": {
                            "indices": [0, 2], "values": [0.1, 0.2],
                        }
                    }
                },
                {
                    "id": 2,
                    "vector": {
                        "text": {
                            "indices": [1, 3], "values": [0.3, 0.4],
                        }
                    }
                },
                {
                    "id": 3,
                    "vector": {
                        "text": {
                            "indices": [0, 1], "values": [0.5, 0.6],
                        }
                    }
                },
            ]
        }
    )
    assert response.ok


def test_sparse_vector_validations(collection_name):
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


def test_sorted_sparse_vector(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}/points/query',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "query": {
                "nearest": {"indices": [2, 0, 1], "values": [0.1, 0.2, 0.3]},
                "mmr": {
                    "diversity": 0.5,
                    "candidates_limit": 10,
                },
            },
            "using": "text"
        }
    )
    assert response.ok
