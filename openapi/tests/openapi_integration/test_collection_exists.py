import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

default_name = ""
collection_name = "test_collection_exist"


@pytest.fixture(autouse=True)
def setup(on_disk_vectors):
    basic_collection_setup(
        collection_name=collection_name, on_disk_vectors=on_disk_vectors
    )
    yield
    drop_collection(collection_name=collection_name)


def test_collection_exists():
    response = request_with_validation(
        api="/collections/{name}/exists",
        method="GET",
        path_params={"name": collection_name},
    )
    assert response.ok
    result = response.json()["exists"]
    assert result == True
