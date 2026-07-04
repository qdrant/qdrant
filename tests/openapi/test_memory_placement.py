import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

collection_name = "test_memory_placement"


@pytest.fixture(autouse=True)
def setup():
    drop_collection(collection_name)
    yield
    drop_collection(collection_name)


def test_create_collection_with_memory_placement():
    # Every component configured through the new `memory` parameter
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={
            "vectors": {
                "size": 4,
                "distance": "Dot",
                "memory": "cached",
                "hnsw_config": {"memory": "cold"},
                "quantization_config": {
                    "scalar": {"type": "int8", "memory": "pinned"},
                },
            },
            "sparse_vectors": {
                "sparse": {"index": {"memory": "cold"}},
            },
            "payload": {"memory": "cached"},
        },
    )
    assert response.ok, response.text

    # The new parameters must be echoed back in the collection info
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="GET",
        path_params={"collection_name": collection_name},
    )
    assert response.ok, response.text
    params = response.json()["result"]["config"]["params"]
    assert params["vectors"]["memory"] == "cached"
    assert params["vectors"]["hnsw_config"]["memory"] == "cold"
    assert params["vectors"]["quantization_config"]["scalar"]["memory"] == "pinned"
    assert params["sparse_vectors"]["sparse"]["index"]["memory"] == "cold"
    assert params["payload"]["memory"] == "cached"

    # Payload field index with the new parameter
    response = request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={
            "field_name": "keyword_field",
            "field_schema": {"type": "keyword", "memory": "cold"},
        },
    )
    assert response.ok, response.text

    response = request_with_validation(
        api="/collections/{collection_name}",
        method="GET",
        path_params={"collection_name": collection_name},
    )
    assert response.ok, response.text
    schema = response.json()["result"]["payload_schema"]
    assert schema["keyword_field"]["params"]["memory"] == "cold"


def test_create_collection_without_memory_placement_has_no_new_fields():
    # A collection created without the new parameters must not expose them in
    # the collection info, so clients of older SDKs see an unchanged response
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={
            "vectors": {"size": 4, "distance": "Dot", "on_disk": True},
        },
    )
    assert response.ok, response.text

    response = request_with_validation(
        api="/collections/{collection_name}",
        method="GET",
        path_params={"collection_name": collection_name},
    )
    assert response.ok, response.text
    params = response.json()["result"]["config"]["params"]
    assert "memory" not in params["vectors"]
    assert "payload" not in params


def test_pinned_dense_vector_storage_is_rejected():
    # Dense vector storage has no heap variant: `pinned` must be rejected
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={
            "vectors": {"size": 4, "distance": "Dot", "memory": "pinned"},
        },
    )
    assert response.status_code == 422, response.text
    assert "pinned" in response.json()["status"]["error"]


def test_pinned_payload_storage_is_rejected():
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={
            "vectors": {"size": 4, "distance": "Dot"},
            "payload": {"memory": "pinned"},
        },
    )
    assert response.status_code == 422, response.text
    assert "pinned" in response.json()["status"]["error"]


def test_update_collection_memory_placement():
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={
            "vectors": {"size": 4, "distance": "Dot"},
        },
    )
    assert response.ok, response.text

    # Vector storage placement and payload storage placement are mutable
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PATCH",
        path_params={"collection_name": collection_name},
        body={
            "vectors": {"": {"memory": "cold"}},
            "params": {"payload": {"memory": "cached"}},
        },
    )
    assert response.ok, response.text

    response = request_with_validation(
        api="/collections/{collection_name}",
        method="GET",
        path_params={"collection_name": collection_name},
    )
    assert response.ok, response.text
    params = response.json()["result"]["config"]["params"]
    assert params["vectors"]["memory"] == "cold"
    assert params["payload"]["memory"] == "cached"

    # `pinned` is rejected on update as well
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PATCH",
        path_params={"collection_name": collection_name},
        body={
            "vectors": {"": {"memory": "pinned"}},
        },
    )
    assert response.status_code == 422, response.text
    assert "pinned" in response.json()["status"]["error"]
