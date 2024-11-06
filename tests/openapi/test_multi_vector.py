import pytest
import os

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, collection_name):
    multivector_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def multivector_collection_setup(
        collection_name='test_collection',
        on_disk_vectors=False):
    drop_collection(collection_name=collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "my-multivec": {
                    "size": 4,
                    "distance": "Dot",
                    "on_disk": on_disk_vectors,
                    "multivector_config": {
                        "comparator": "max_sim"
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


def test_multi_vector_float_persisted(collection_name):
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
                        "my-multivec": [
                            [0.05, 0.61, 0.76, 0.74],
                            [0.05, 0.61, 0.76, 0.74],
                            [0.05, 0.61, 0.76, 0.74]
                        ]
                    }
                },
                {
                    "id": 2,
                    "vector": {
                        "my-multivec": [
                            [0.19, 0.81, 0.75, 0.11],
                            [0.19, 0.81, 0.75, 0.11],
                            [0.19, 0.81, 0.75, 0.11]
                        ]
                    }
                },
                {
                    "id": 3,
                    "vector": {
                        "my-multivec": [
                            [0.36, 0.55, 0.47, 0.94],
                            [0.36, 0.55, 0.47, 0.94],
                            [0.36, 0.55, 0.47, 0.94]
                        ]
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
    assert first_point['vector']['my-multivec'] == [[0.05, 0.61, 0.76, 0.74], [0.05, 0.61, 0.76, 0.74], [0.05, 0.61, 0.76, 0.74]]

    # retrieve by id
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 2},
    )
    assert response.ok
    point = response.json()['result']

    assert point['id'] == 2
    assert point['vector']['my-multivec'] == [[0.19, 0.81, 0.75, 0.11], [0.19, 0.81, 0.75, 0.11], [0.19, 0.81, 0.75, 0.11]]

    # delete by id
    response = request_with_validation(
        api='/collections/{collection_name}/points/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "points": [2]
        }
    )
    assert response.ok

    # retrieve by id after deletion
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 2},
    )
    assert not response.ok


def test_multi_vector_validation(collection_name):
    # fails because it uses and empty multi vector
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
    assert 'Wrong input: Vector dimension error: expected dim: 4, got 0' in response.json()["status"]["error"]

    # fails because it uses an empty inner vector
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
    assert 'Validation error in JSON body: [points[0].vector.?.data: all vectors must be non-empty]' in \
           response.json()["status"]["error"]

    # fails because it uses one inner vector
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
    assert 'Validation error in JSON body: [points[0].vector.?.data: all vectors must be non-empty]' in \
           response.json()["status"]["error"]

    # fails because it uses inner vectors with different dimensions
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
                            [0.05, 0.61, 0.76]
                        ]
                    }
                }
            ]
        }
    )
    assert not response.ok
    assert 'Validation error in JSON body: [points[0].vector.?.data: all vectors must have the same dimension, found vector with dimension 3' in \
           response.json()["status"]["error"]


# allow multivec upsert on legacy API by emulating a multivec input with a single dense vector
def test_upsert_legacy_api(collection_name):
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
                            [0.05, 0.61, 0.76, 0.74],
                            [0.05, 0.61, 0.76, 0.74]
                        ]
                    }
                },
                {
                    "id": 2,
                    "vector": {
                        "my-multivec": [0.19, 0.81, 0.75, 0.11]
                    }
                },
            ]
        }
    )
    assert response.ok

    # retrieve by id 1
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 1},
    )
    assert response.ok
    point = response.json()['result']

    assert point['id'] == 1
    assert point['vector']['my-multivec'] == [[0.05, 0.61, 0.76, 0.74], [0.05, 0.61, 0.76, 0.74], [0.05, 0.61, 0.76, 0.74]]

    # retrieve by id 2
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 2},
    )
    assert response.ok
    point = response.json()['result']

    assert point['id'] == 2
    assert point['vector']['my-multivec'] == [[0.19, 0.81, 0.75, 0.11]]


# allow multivec search on legacy API by emulating a multivec input with a single dense vector
def test_search_legacy_api(collection_name):
    # validate input size
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": {
                "name": "my-multivec",
                "vector": [0.05, 0.61, 0.76]
            },
            "limit": 3
        }
    )
    assert not response.ok
    assert 'Wrong input: Vector dimension error: expected dim: 4, got 3' in \
           response.json()["status"]["error"]

    # search on empty collection
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": {
                "name": "my-multivec",
                "vector": [0.05, 0.61, 0.76, 0.74]
            },
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 0

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
                            [0.05, 0.61, 0.76, 0.74],
                            [0.05, 0.61, 0.76, 0.74]
                        ]
                    }
                },
                {
                    "id": 2,
                    "vector": {
                        "my-multivec": [
                            [0.19, 0.81, 0.75, 0.11],
                            [0.19, 0.81, 0.75, 0.11],
                            [0.19, 0.81, 0.75, 0.11]
                        ]
                    }
                },
                {
                    "id": 3,
                    "vector": {
                        "my-multivec": [
                            [0.36, 0.55, 0.47, 0.94],
                            [0.36, 0.55, 0.47, 0.94],
                            [0.36, 0.55, 0.47, 0.94]
                        ]
                    }
                },
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": {
                "name": "my-multivec",
                "vector": [0.05, 0.61, 0.76, 0.74]
            },
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 3
