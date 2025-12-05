import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_indexing_progress(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/indexing_progress",
        method="GET",
        path_params={"collection_name": collection_name},
    )
    assert response.ok
    result = response.json()["result"]
    assert result["ongoing"] == []
    assert result["completed"] == []
