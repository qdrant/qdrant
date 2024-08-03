import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_sparse_dense_collection_setup'


@pytest.fixture(autouse=True)
def setup():
    sparse_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def sparse_collection_setup(
        collection_name='test_collection',
):
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
                "text": {}
            },
        }
    )
    assert response.ok


def test_sparse_dense_updates():

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
                            "indices": [100, 500, 10], "values": [0.9, 0.8, 0.5]
                        }
                    }
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "limit": 10,
            "vector": {
                "name": "text",
                "vector": {
                    "indices": [100],
                    "values": [0.9]
                }
            }
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 1

    # Overwrite existing vector with new indices
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
                            "indices": [600, 700, 10], "values": [0.9, 0.8, 0.5]
                        }
                    }
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "limit": 10,
            "vector": {
                "name": "text",
                "vector": {
                    "indices": [100],
                    "values": [0.9]
                }
            }
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 0

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "limit": 10,
            "vector": {
                "name": "text",
                "vector": {
                    "indices": [700],
                    "values": [0.9]
                }
            }
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 1

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "limit": 10,
            "vector": {
                "name": "text",
                "vector": {
                    "indices": [10],
                    "values": [0.9]
                }
            }
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 1

