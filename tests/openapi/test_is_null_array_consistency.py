"""Regression for https://github.com/qdrant/qdrant/issues/10096

IsNullCondition (`{"is_null":{"key":...}}`) must match arrays containing null
the same way as FieldCondition (`{"key":...,"is_null":true}`), whether or not
the field is indexed.
"""

import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

POINTS = [
    {"id": 1, "payload": {"s": [None, "a"]}, "vector": [0.2, 0.3]},
    {"id": 2, "payload": {"s": None}, "vector": [0.2, 0.4]},
    {"id": 3, "payload": {"s": ["a"]}, "vector": [0.1, 0.4]},
    {"id": 4, "payload": {"s": [None]}, "vector": [0.1, 0.5]},
]

# Points whose payload field `s` contains a null (literal or inside an array).
EXPECTED_NULL_IDS = {1, 2, 4}


def _create_collection(name: str, with_keyword_index: bool):
    drop_collection(collection_name=name)
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': name},
        body={
            "vectors": {
                "size": 2,
                "distance": "Dot",
            },
        },
    )
    assert response.ok

    if with_keyword_index:
        response = request_with_validation(
            api='/collections/{collection_name}/index',
            method="PUT",
            path_params={'collection_name': name},
            query_params={'wait': 'true'},
            body={
                "field_name": "s",
                "field_schema": "keyword",
            },
        )
        assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': name},
        query_params={'wait': 'true'},
        body={"points": POINTS},
    )
    assert response.ok


def _scroll_ids(collection_name: str, filt: dict) -> set[int]:
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": filt,
            "limit": 100,
            "with_payload": True,
        },
    )
    assert response.ok
    return {point['id'] for point in response.json()['result']['points']}


@pytest.fixture(scope="module")
def collections():
    indexed = "test_is_null_array_indexed"
    unindexed = "test_is_null_array_unindexed"
    _create_collection(indexed, with_keyword_index=True)
    _create_collection(unindexed, with_keyword_index=False)
    yield indexed, unindexed
    drop_collection(collection_name=indexed)
    drop_collection(collection_name=unindexed)


@pytest.mark.parametrize(
    "filter_body",
    [
        {"must": [{"is_null": {"key": "s"}}]},
        {"must": [{"key": "s", "is_null": True}]},
    ],
    ids=["IsNullCondition", "FieldCondition_is_null"],
)
def test_is_null_array_consistent_indexed_and_unindexed(collections, filter_body):
    indexed, unindexed = collections
    indexed_ids = _scroll_ids(indexed, filter_body)
    unindexed_ids = _scroll_ids(unindexed, filter_body)
    assert indexed_ids == EXPECTED_NULL_IDS
    assert unindexed_ids == EXPECTED_NULL_IDS
    assert indexed_ids == unindexed_ids
