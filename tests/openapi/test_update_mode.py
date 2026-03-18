import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, on_disk_payload, collection_name):
    basic_collection_setup(
        collection_name=collection_name,
        on_disk_vectors=on_disk_vectors,
        on_disk_payload=on_disk_payload,
    )
    yield
    drop_collection(collection_name=collection_name)


def test_update_only_mode(collection_name):
    """update_only: modifies existing points, does not create new ones."""
    # Upsert batch with existing point (1) and new point (999)
    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={
            "points": [
                {"id": 1, "vector": [9.0, 9.0, 9.0, 9.0], "payload": {"updated": True}},
                {"id": 999, "vector": [1.0, 1.0, 1.0, 1.0], "payload": {"new": True}},
            ],
            "update_mode": "update_only",
        },
        query_params={"wait": "true"},
    )
    assert response.ok

    # Existing point 1 should be modified
    response = request_with_validation(
        api="/collections/{collection_name}/points/{id}",
        method="GET",
        path_params={"collection_name": collection_name, "id": 1},
    )
    assert response.ok
    assert response.json()["result"]["payload"]["updated"] is True
    assert response.json()["result"]["vector"][0] == 9.0

    # New point 999 should NOT exist
    response = request_with_validation(
        api="/collections/{collection_name}/points/{id}",
        method="GET",
        path_params={"collection_name": collection_name, "id": 999},
    )
    assert response.status_code == 404


def test_insert_only_mode(collection_name):
    """insert_only: inserts new points, does not modify existing ones."""
    # Upsert batch with existing point (1) and new point (999)
    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={
            "points": [
                {"id": 1, "vector": [9.0, 9.0, 9.0, 9.0], "payload": {"updated": True}},
                {"id": 999, "vector": [1.0, 1.0, 1.0, 1.0], "payload": {"new": True}},
            ],
            "update_mode": "insert_only",
        },
        query_params={"wait": "true"},
    )
    assert response.ok

    # Existing point 1 should NOT be modified (original vector was [0.05, 0.61, 0.76, 0.74])
    response = request_with_validation(
        api="/collections/{collection_name}/points/{id}",
        method="GET",
        path_params={"collection_name": collection_name, "id": 1},
    )
    assert response.ok
    assert "updated" not in response.json()["result"]["payload"]
    assert response.json()["result"]["vector"][0] < 1.0

    # New point 999 should exist
    response = request_with_validation(
        api="/collections/{collection_name}/points/{id}",
        method="GET",
        path_params={"collection_name": collection_name, "id": 999},
    )
    assert response.ok
    assert response.json()["result"]["payload"]["new"] is True
