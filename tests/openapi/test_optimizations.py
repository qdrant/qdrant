import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(collection_name):
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_optimizations(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/optimizations",
        method="GET",
        path_params={"collection_name": collection_name},
    )
    assert response.ok
    result = response.json()["result"]
    assert result["ongoing"] == []
    assert "completed" not in result


    response = request_with_validation(
        api="/collections/{collection_name}/optimizations",
        method="GET",
        path_params={"collection_name": collection_name},
        query_params={"completed": 'true'},
    )
    assert response.ok
    result = response.json()["result"]
    assert result["ongoing"] == []
    assert result["completed"] == []
