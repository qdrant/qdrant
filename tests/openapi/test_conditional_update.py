from operator import itemgetter

import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, on_disk_payload, collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors,
                           on_disk_payload=on_disk_payload)
    yield
    drop_collection(collection_name=collection_name)


def test_conditional_update(collection_name):

    # Upsert and delete points
    response = request_with_validation(
        api="/collections/{collection_name}/points/payload",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "points": [6],
            "payload": {
                "version": 5
            }
        },
        query_params={"wait": "true"},
    )
    assert response.ok

    # Upsert and delete points
    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={
            "points": [
                {
                    "id": 6,
                    "vector": [1.1, 1.2, 1.3, 1.4],
                    "payload": {
                        "version": 4,
                        "test": "test_value"
                    }
                }
            ],
            "update_filter": {
                "must": {
                    "key": "version",
                    "range": {
                        "lt": 4,
                    }
                }
            }
        },
        query_params={"wait": "true"},
    )
    assert response.ok

    # Check that the point was not updated
    response = request_with_validation(
        api="/collections/{collection_name}/points/{id}",
        method="GET",
        path_params={
            "collection_name": collection_name,
            "id": 6
        },
        query_params={},
    )
    assert response.ok
    assert response.json()["result"]["payload"]["version"] == 5


    # Upsert and delete points
    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={
            "points": [
                {
                    "id": 6,
                    "vector": [1.1, 1.2, 1.3, 1.4],
                    "payload": {
                        "version": 6,
                        "test": "test_value"
                    }
                }
            ],
            "update_filter": {
                "must": {
                    "key": "version",
                    "range": {
                        "lt": 6,
                    }
                }
            }
        },
        query_params={"wait": "true"},
    )
    assert response.ok

    # Check that the point was not updated
    response = request_with_validation(
        api="/collections/{collection_name}/points/{id}",
        method="GET",
        path_params={
            "collection_name": collection_name,
            "id": 6
        },
        query_params={},
    )
    assert response.ok
    assert response.json()["result"]["payload"]["version"] == 6

    # Upsert and delete points
    response = request_with_validation(
        api="/collections/{collection_name}/points/vectors",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={
            "points": [
                {
                    "id": 6,
                    "vector": [2.1, 2.2, 2.3, 2.4],
                }
            ],
            "update_filter": {
                "must": {
                    "key": "version",
                    "range": {
                        "lt": 4,
                    }
                }
            }
        },
        query_params={"wait": "true"},
    )
    assert response.ok

    # Check that the point was not updated
    response = request_with_validation(
        api="/collections/{collection_name}/points/{id}",
        method="GET",
        path_params={
            "collection_name": collection_name,
            "id": 6
        },
        query_params={},
    )
    assert response.ok
    assert response.json()["result"]["vector"][0] < 2.0