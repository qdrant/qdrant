import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

@pytest.fixture(autouse=True, scope="module")
def setup(collection_name):
    create_collection(collection_name)
    yield
    drop_collection(collection_name=collection_name)


def create_collection(collection_name):
    drop_collection(collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 3,
                "distance": "Dot",
            },
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": "a",
            "field_schema": "keyword",
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": [0.05, 0.61, 0.76],
                },
                {
                    "id": 2,
                    "vector": [0.19, 0.81, 0.75],
                },
                {
                    "id": 3,
                    "vector": [0.36, 0.55, 0.47],
                },
            ]
        }
    )
    assert response.ok


def test_filter_is_empty(collection_name):
    """
    Bug: <https://github.com/qdrant/qdrant/pull/6882>
    """

    response = request_with_validation(
        api='/collections/{collection_name}/points/query',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "is_empty": {
                            "key": "a"
                        }
                    }
                ]
            },
            "query": [0.1, 0.1, 0.1],
            "limit": 2,
        }
    )
    assert response.ok

    json = response.json()
    assert len(json['result']['points']) == 2, json
