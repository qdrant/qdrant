from datetime import datetime, timedelta, timezone

import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(
        collection_name=collection_name, on_disk_vectors=on_disk_vectors
    )
    yield
    drop_collection(collection_name=collection_name)


def get_created_at(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="GET",
        path_params={"collection_name": collection_name},
    )
    assert response.ok
    return response.json()["result"]["created_at"]


def test_collection_created_at(collection_name):
    created_at = get_created_at(collection_name)
    age = datetime.now(timezone.utc) - datetime.fromisoformat(created_at)
    assert timedelta(0) <= age < timedelta(minutes=1)

    # Config updates rewrite the config file, creation time must survive them
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PATCH",
        path_params={"collection_name": collection_name},
        body={"metadata": {"key": "value"}},
    )
    assert response.ok

    assert get_created_at(collection_name) == created_at
