import pytest
import os

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, collection_name):
    multivector_uint8_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def multivector_uint8_collection_setup(
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
                    "datatype": "uint8",
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


def test_multi_vector_uint8_persisted(collection_name):
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
                            [1.05, 1.61, 1.76, 1.74],
                            [2.05, 2.61, 2.76, 2.74],
                            [3.05, 3.61, 3.76, 3.74]
                        ]
                    }
                },
                {
                    "id": 2,
                    "vector": {
                        "my-multivec": [
                            [3.19, 3.81, 3.75, 3.11],
                            [2.19, 2.81, 2.75, 2.11],
                            [1.19, 1.81, 1.75, 1.11]
                        ]
                    }
                },
                {
                    "id": 3,
                    "vector": {
                        "my-multivec": [
                            [2.36, 2.55, 2.47, 2.94],
                            [1.36, 1.55, 1.47, 1.94],
                            [3.36, 3.55, 3.47, 3.94]
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
    assert first_point['vector']['my-multivec'] == [[1, 1, 1, 1], [2, 2, 2, 2], [3, 3, 3, 3]]

    # retrieve by id
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 2},
    )
    assert response.ok
    point = response.json()['result']

    assert point['id'] == 2
    assert point['vector']['my-multivec'] == [[3, 3, 3, 3], [2, 2, 2, 2], [1, 1, 1, 1]]

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


def test_multi_vector_unint8_truncation(collection_name):
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
                        "my-multivec": [[256.19, 1.81, 2.75, 3.11]]
                    }
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 1},
    )
    assert response.ok
    point = response.json()['result']

    assert point['id'] == 1
    assert point['vector']['my-multivec'] == [[255, 1, 2, 3]]
