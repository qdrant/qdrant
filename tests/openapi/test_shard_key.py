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
