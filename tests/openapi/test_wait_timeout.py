import pytest

from openapi.helpers.collection_setup import drop_collection
from openapi.helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(collection_name):
    drop_collection(collection_name)

    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={"sparse_vectors": {"text": {}}},
    )

    assert response.ok

    yield
    drop_collection(collection_name=collection_name)


def test_wait_timeout_ack(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/debug",
        method="POST",
        path_params={"collection_name": collection_name},
        body={"delay": {"duration_sec": 15.0}},
    )
    assert response.ok and response.json()["result"]["status"] == "acknowledged"

    response = request_with_validation(
        api="/collections/{collection_name}/wait",
        method="POST",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true", "timeout": 1},
        body={
            "points": {
                "id": 1,
                "vector": {
                    "text": "Lorem ipsum, dolor sit amet.",
                    "model": "qdrant/bm25",
                },
            }
        },
    )
    assert response.ok and response.json()["result"]["status"] == "wait_timeout"


def test_wait_timeout_completed(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/debug",
        method="POST",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={"delay": {"duration_sec": 15.0}},
    )
    assert response.ok and response.json()["result"]["status"] == "completed"

    response = request_with_validation(
        api="/collections/{collection_name}/wait",
        method="POST",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true", "timeout": 5},
        body={
            "points": {
                "id": 1,
                "vector": {
                    "text": "Lorem ipsum, dolor sit amet.",
                    "model": "qdrant/bm25",
                },
            }
        },
    )
    assert response.ok and response.json()["result"]["status"] == "wait_timeout"


def test_wait_timeout_twice(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/debug",
        method="POST",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true", "timeout": 5},
        body={"delay": {"duration_sec": 15.0}},
    )
    assert response.ok and response.json()["result"]["status"] == "wait_timeout"

    response = request_with_validation(
        api="/collections/{collection_name}/wait",
        method="POST",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true", "timeout": 1},
        body={
            "points": {
                "id": 1,
                "vector": {
                    "text": "Lorem ipsum, dolor sit amet.",
                    "model": "qdrant/bm25",
                },
            }
        },
    )
    assert response.ok and response.json()["result"]["status"] == "wait_timeout"
