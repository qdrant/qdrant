import os
import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors, wal_capacity=1,
                           sharding_method="custom")
    yield
    drop_collection(collection_name=collection_name)


def create_shard_key(key, collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}/shards',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "shard_key": key,
        },
    )
    assert response.ok


def get_shard_keys(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}/shards',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    return response


def test_set_payload_with_multiple_shard_keys_and_point_ids(collection_name):
    create_shard_key("1", collection_name)
    create_shard_key("2", collection_name)

    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={
            "shard_key": "1",
            "points": [
                {"id": 9, "vector": [0.1, 0.2, 0.3, 0.4]},
            ],
        },
    )
    assert response.ok

    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={
            "shard_key": "2",
            "points": [
                {"id": 101, "vector": [0.4, 0.3, 0.2, 0.1]},
            ],
        },
    )
    assert response.ok

    response = request_with_validation(
        api="/collections/{collection_name}/points/payload",
        method="POST",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={
            "payload": {"color": "black"},
            "points": [9, 101],
            "shard_key": ["1", "2"],
        },
    )
    assert response.ok, response.json()

    response = request_with_validation(
        api="/collections/{collection_name}/points/{id}",
        method="GET",
        path_params={"collection_name": collection_name, "id": 9},
    )
    assert response.ok
    assert response.json()["result"]["payload"]["color"] == "black"

    response = request_with_validation(
        api="/collections/{collection_name}/points/{id}",
        method="GET",
        path_params={"collection_name": collection_name, "id": 101},
    )
    assert response.ok
    assert response.json()["result"]["payload"]["color"] == "black"


@pytest.mark.skipif(
    not os.getenv("QDRANT__CLUSTER__ENABLED"),
    reason="only works in distributed mode"
)
def test_shard_keys_list(collection_name):
    # no shard keys on collection
    response = get_shard_keys(collection_name)
    assert len(response.json()["result"]["shard_keys"]) == 0

    create_shard_key("test_key", collection_name)
    create_shard_key(100, collection_name)

    response = get_shard_keys(collection_name)
    shard_keys = response.json()["result"].get("shard_keys", [])
    assert len(shard_keys) == 2
    assert any("test_key" in item.values() for item in shard_keys)
    assert any(100 in item.values() for item in shard_keys)
