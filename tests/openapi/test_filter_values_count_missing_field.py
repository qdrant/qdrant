"""Regression test for https://github.com/qdrant/qdrant/issues/9586."""

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
                "size": 4,
                "distance": "Cosine",
            },
        },
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
                    "vector": [0.1, 0.2, 0.3, 0.4],
                    "payload": {"tags": ["a", "b", "c"]},
                },
                {
                    "id": 2,
                    "vector": [0.2, 0.3, 0.4, 0.5],
                    "payload": {"tags": ["x"]},
                },
                {
                    "id": 3,
                    "vector": [0.3, 0.4, 0.5, 0.6],
                    "payload": {"title": "no tags field"},
                },
            ],
        },
    )
    assert response.ok


def _scroll_values_count(collection_name, values_count):
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "key": "tags",
                        "values_count": values_count,
                    }
                ]
            },
            "with_payload": True,
            "with_vector": False,
        },
    )
    assert response.ok
    return sorted(p['id'] for p in response.json()['result']['points'])


def test_filter_values_count_missing_field(collection_name):
    """
    A missing payload field should be treated as having a value count of 0 for
    `values_count` filters.
    """
    assert _scroll_values_count(collection_name, {"lt": 1}) == [3]
    assert _scroll_values_count(collection_name, {"gte": 0}) == [1, 2, 3]
    assert _scroll_values_count(collection_name, {"lte": 0}) == [3]
