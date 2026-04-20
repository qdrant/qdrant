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


def _scroll_is_empty(collection_name, key):
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {"must": [{"is_empty": {"key": key}}]},
            "limit": 10,
            "with_payload": False,
            "with_vector": False,
        }
    )
    assert response.ok
    return sorted(p['id'] for p in response.json()['result']['points'])


def test_filter_is_empty_after_clear_and_rebuild():
    """
    Bug: <https://github.com/qdrant/qdrant/issues/8723>

    After `clear_payload` on a point and a subsequent delete+recreate of the payload
    index, `is_empty` must still return points whose payload no longer contains the
    field, regardless of whether the field was previously present or not.
    """
    name = "test_filter_is_empty_after_clear_and_rebuild"

    drop_collection(name)

    # Create collection with a keyword payload index on "status".
    assert request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': name},
        body={"vectors": {"size": 2, "distance": "Dot"}},
    ).ok

    assert request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': name},
        query_params={'wait': 'true'},
        body={
            "field_name": "status",
            "field_schema": {"type": "keyword", "on_disk": True},
        },
    ).ok

    # Point 1 has an explicit null, point 2 has no payload, point 3 has a value.
    # Keep the value on the highest-ID point so the rebuild bug (len not covering
    # cleared points with higher IDs) gets exercised when point 1 is later cleared.
    assert request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {"id": 1, "vector": [1.0, 0.0], "payload": {"status": None}},
                {"id": 2, "vector": [0.0, 1.0], "payload": {}},
                {"id": 3, "vector": [1.0, 1.0], "payload": {"status": "live"}},
            ],
        },
    ).ok

    assert _scroll_is_empty(name, "status") == [1, 2]

    # Clear the payload of point 1: it should now behave like point 2 (missing field).
    assert request_with_validation(
        api='/collections/{collection_name}/points/payload/clear',
        method="POST",
        path_params={'collection_name': name},
        query_params={'wait': 'true'},
        body={"points": [1]},
    ).ok

    assert _scroll_is_empty(name, "status") == [1, 2]

    # Delete + recreate the payload index. Post-rebuild, cleared points must still
    # match `is_empty`, just like points that never had the field.
    assert request_with_validation(
        api='/collections/{collection_name}/index/{field_name}',
        method="DELETE",
        path_params={'collection_name': name, 'field_name': 'status'},
        query_params={'wait': 'true'},
    ).ok

    assert request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': name},
        query_params={'wait': 'true'},
        body={
            "field_name": "status",
            "field_schema": {"type": "keyword", "on_disk": True},
        },
    ).ok

    assert _scroll_is_empty(name, "status") == [1, 2]

    drop_collection(name)
