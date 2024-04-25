import pytest
import requests
import os

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


QDRANT_HOST = os.environ.get("QDRANT_HOST", "localhost:6333")

collection_name = 'test_multi_vector_persistence'


@pytest.fixture(autouse=True)
def setup():
    multivector_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)

def multivector_collection_setup(collection_name='test_collection'):
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
            "vectors": {
                "my-multivec": {
                    "size": 4,
                    "distance": "Dot",
                    "multi_vec_config": {
                        "max_sim": {}
                    }
                }
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


def test_multi_vector_persisted():
    # batch upsert
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
                        "my-multivec": [[0.05, 0.61, 0.76, 0.74]]
                    }
                },
                {
                    "id": 2,
                    "vector": {
                        "my-multivec": [[0.19, 0.81, 0.75, 0.11]]
                    }
                },
                {
                    "id": 3,
                    "vector": {
                        "my-multivec": [[0.36, 0.55, 0.47, 0.94]]
                    }
                },
            ]
        }
    )
    assert response.ok

    # scroll
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={"limit": 10, "with_vector": True}
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 3
    results = response.json()['result']['points']

    first_point = results[0]
    assert first_point['id'] == 1
    assert first_point['vector']['my-multivec'] == [[0.05, 0.61, 0.76, 0.74]]

    # retrieve by id
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 2},
    )
    assert response.ok
    point = response.json()['result']

    assert point['id'] == 2
    assert point['vector']['my-multivec'] == [[0.19, 0.81, 0.75, 0.11]]

# TODO add test for failing search with multi vector

def test_multi_vector_validation():
    # use dense vector
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
                        "my-multivec": [0.19, 0.81, 0.75, 0.11]
                    }
                }
            ]
        }
    )
    assert not response.ok
    assert 'Wrong input: Conversion between multi and regular vectors failed' in response.json()["status"]["error"]

    # empty multi vector
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
                        "my-multivec": []
                    }
                }
            ]
        }
    )
    assert not response.ok
    assert 'Wrong input: Vector inserting error: expected dim: 4, got 0' in response.json()["status"]["error"]

    # empty inner vector
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
                        "my-multivec": [[]]
                    }
                }
            ]
        }
    )
    assert not response.ok
    assert 'Validation error in JSON body: [points[0].vector.?.data: all vectors must be non-empty]' in response.json()["status"]["error"]

    # one inner vector
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
                        "my-multivec": [
                            [0.05, 0.61, 0.76, 0.74],
                            []
                        ]
                    }
                }
            ]
        }
    )
    assert not response.ok
    assert 'Validation error in JSON body: [points[0].vector.?.data: all vectors must be non-empty]' in response.json()["status"]["error"]


