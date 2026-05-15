import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

COLLECTION_NAME = "test_match_except_empty"


@pytest.fixture(autouse=True, scope="module")
def setup():
    create_collection(COLLECTION_NAME)
    yield
    drop_collection(collection_name=COLLECTION_NAME)


def create_collection(collection_name):
    drop_collection(collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 2,
                "distance": "Dot",
            },
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
                {"id": 1, "vector": [1.0, 0.0], "payload": {"n": 1, "tag": "a"}},
                {"id": 2, "vector": [0.0, 1.0], "payload": {"n": 2, "tag": "b"}},
                {"id": 3, "vector": [1.0, 1.0], "payload": {"n": 3, "tag": "c"}},
            ]
        }
    )
    assert response.ok


def _scroll_ids(collection_name, filter_body):
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": filter_body,
            "limit": 10,
            "with_payload": True,
        }
    )
    assert response.ok
    return sorted(p['id'] for p in response.json()['result']['points'])


def test_match_except_empty_integer_no_index():
    """except: [] without index should return all points that have the field."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "n", "match": {"except": []}}]
    })
    assert ids == [1, 2, 3]


def test_match_except_empty_keyword_no_index():
    """except: [] without index should return all points that have the field."""
    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "tag", "match": {"except": []}}]
    })
    assert ids == [1, 2, 3]


def test_match_except_empty_integer_with_index():
    """
    Bug: https://github.com/qdrant/qdrant/issues/9050

    After creating an integer payload index, `match: {"except": []}` must
    still return all points — same as the non-indexed path. An empty except
    list means "exclude nothing", i.e. match everything.
    """
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': COLLECTION_NAME},
        query_params={'wait': 'true'},
        body={
            "field_name": "n",
            "field_schema": "integer",
        }
    )
    assert response.ok

    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "n", "match": {"except": []}}]
    })
    assert ids == [1, 2, 3], (
        f"match except [] with integer index should return all points, got {ids}"
    )


def test_match_except_empty_keyword_with_index():
    """
    Symmetric check: after creating a keyword payload index, `match: {"except": []}`
    must still return all points.
    """
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': COLLECTION_NAME},
        query_params={'wait': 'true'},
        body={
            "field_name": "tag",
            "field_schema": "keyword",
        }
    )
    assert response.ok

    ids = _scroll_ids(COLLECTION_NAME, {
        "must": [{"key": "tag", "match": {"except": []}}]
    })
    assert ids == [1, 2, 3], (
        f"match except [] with keyword index should return all points, got {ids}"
    )
