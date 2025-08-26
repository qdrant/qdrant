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


def test_collection_exists(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PATCH",
        path_params={"collection_name": collection_name},
        body={"metadata": {"new_meta": "value"}}
    )
    assert response.ok

    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PATCH",
        path_params={"collection_name": collection_name},
        body={"metadata": {"new_meta_2": "value2"}}
    )
    assert response.ok

    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PATCH",
        path_params={"collection_name": collection_name},
        body={"metadata": {"new_meta": "value3"}}
    )
    assert response.ok

    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PATCH",
        path_params={"collection_name": collection_name},
        body={"metadata": {"new_meta_2": None}}
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    result = response.json()["result"]

    assert result["config"]["metadata"] == {"new_meta": "value3"}
