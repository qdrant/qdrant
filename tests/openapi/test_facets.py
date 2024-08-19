import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = "test_facets"


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)

    request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={
            "field_name": "city",
            "field_schema": "keyword",
        }
    ).raise_for_status()

    yield
    drop_collection(collection_name=collection_name)


def test_basic_facet():
    response = request_with_validation(
        api="/collections/{collection_name}/facet",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "key": "city",
            # limit is optional
        }
    )

    assert response.ok, response.json()

    city_facet = response.json()["result"]
    assert city_facet == {
        "hits": [
            # Sorted by count, then by value
            {"value": "Berlin", "count": 3 },
            {"value": "London", "count": 2 },
            {"value": "Moscow", "count": 2 },
        ]
    }
