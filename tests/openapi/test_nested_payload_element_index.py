import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

POINTS = [
    {
        "id": 1,
        "offers": [
            {"shop": "a", "price": 450, "title": "red bag"},
            {"shop": "b", "price": 520, "title": "blue shoes"},
        ],
    },
    {
        "id": 2,
        "offers": [
            {"shop": "a", "price": 600, "title": "red running shoes"},
            {"shop": "a", "price": 300},
        ],
    },
    {"id": 3, "offers": [{"shop": "c", "price": 100}, 5, {"shop": "b"}]},
    {"id": 4, "offers": []},
    {"id": 5},
]


def create_index(collection_name, body):
    return request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body=body,
    )


def scroll_ids(collection_name, body):
    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={"limit": 100, **body},
    )
    assert response.ok, response.text
    return sorted(point["id"] for point in response.json()["result"]["points"])


@pytest.fixture(autouse=True, scope="module")
def setup(collection_name):
    drop_collection(collection_name)
    request_with_validation(
        api="/collections/{collection_name}",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={"vectors": {"size": 2, "distance": "Dot"}},
    ).raise_for_status()

    request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={
            "points": [
                {
                    "id": point["id"],
                    "vector": [0.1, 0.2],
                    "payload": {key: value for key, value in point.items() if key != "id"},
                }
                for point in POINTS
            ]
        },
    ).raise_for_status()

    # Both spellings of the array are accepted
    response = create_index(
        collection_name,
        {"field_name": "offers[].price", "field_schema": {"type": "integer", "nested": "offers"}},
    )
    assert response.ok, response.text
    response = create_index(
        collection_name,
        {
            "field_name": "offers[].shop",
            "field_schema": {"type": "keyword", "on_disk": True, "nested": "offers[]"},
        },
    )
    assert response.ok, response.text

    yield
    drop_collection(collection_name)


def test_collection_info_shows_nested(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="GET",
        path_params={"collection_name": collection_name},
    )
    assert response.ok
    schema = response.json()["result"]["payload_schema"]

    assert schema["offers[].price"]["data_type"] == "integer"
    assert schema["offers[].price"]["params"]["nested"] == "offers[]"
    assert schema["offers[].price"]["points"] == 3
    assert schema["offers[].shop"]["data_type"] == "keyword"
    assert schema["offers[].shop"]["params"]["nested"] == "offers[]"


def test_recreate_with_same_nested_is_noop(collection_name):
    response = create_index(
        collection_name,
        {"field_name": "offers[].price", "field_schema": {"type": "integer", "nested": "offers[]"}},
    )
    assert response.ok, response.text


def test_filters_match_flattened_semantics(collection_name):
    # Same results as a flattened index: any value of the point matches
    assert scroll_ids(
        collection_name,
        {"filter": {"must": [{"key": "offers[].price", "range": {"lt": 500}}]}},
    ) == [1, 2, 3]
    assert scroll_ids(
        collection_name,
        {"filter": {"must": [{"key": "offers[].shop", "match": {"value": "b"}}]}},
    ) == [1, 3]
    assert scroll_ids(
        collection_name,
        {
            "filter": {
                "must": [
                    {"key": "offers[].shop", "match": {"value": "a"}},
                    {"key": "offers[].price", "range": {"gt": 500}},
                ]
            }
        },
    ) == [1, 2]

    # Nested filters keep their per-element semantics
    assert scroll_ids(
        collection_name,
        {
            "filter": {
                "must": [
                    {
                        "nested": {
                            "key": "offers",
                            "filter": {
                                "must": [
                                    {"key": "shop", "match": {"value": "a"}},
                                    {"key": "price", "range": {"gt": 500}},
                                ]
                            },
                        }
                    }
                ]
            }
        },
    ) == [2]


def test_text_matches_within_one_element(collection_name):
    response = create_index(
        collection_name,
        {"field_name": "offers[].title", "field_schema": {"type": "text", "nested": "offers"}},
    )
    assert response.ok, response.text

    # Point 1 has both words, but in different offers
    assert scroll_ids(
        collection_name,
        {"filter": {"must": [{"key": "offers[].title", "match": {"text": "red shoes"}}]}},
    ) == [2]
    assert scroll_ids(
        collection_name,
        {"filter": {"must": [{"key": "offers[].title", "match": {"text_any": "bag shoes"}}]}},
    ) == [1, 2]


def test_facet_counts_points(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/facet",
        method="POST",
        path_params={"collection_name": collection_name},
        body={"key": "offers[].shop", "exact": True},
    )
    assert response.ok, response.text
    hits = {hit["value"]: hit["count"] for hit in response.json()["result"]["hits"]}
    # Point 2 has two offers of shop `a`, it counts once
    assert hits == {"a": 2, "b": 2, "c": 1}


def test_order_by(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={"limit": 100, "order_by": "offers[].price"},
    )
    assert response.ok, response.text
    ordered = [
        (point["order_value"], point["id"]) for point in response.json()["result"]["points"]
    ]
    assert ordered == [(100, 3), (300, 2), (450, 1), (520, 1), (600, 2)]


@pytest.mark.parametrize(
    "body",
    [
        # Field is not inside the elements of the array
        {"field_name": "price", "field_schema": {"type": "integer", "nested": "offers"}},
        {"field_name": "offers[]", "field_schema": {"type": "keyword", "nested": "offers"}},
        # Only one array level
        {
            "field_name": "offers[].items[].size",
            "field_schema": {"type": "integer", "nested": "offers[].items"},
        },
        # Not combined with tenant indexes
        {
            "field_name": "offers[].tenant",
            "field_schema": {"type": "keyword", "is_tenant": True, "nested": "offers"},
        },
    ],
)
def test_invalid_nested(collection_name, body):
    response = create_index(collection_name, body)
    assert response.status_code == 400, response.text


def test_delete_nested_index(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/index/{field_name}",
        method="DELETE",
        path_params={"collection_name": collection_name, "field_name": "offers[].shop"},
        query_params={"wait": "true"},
    )
    assert response.ok, response.text

    response = request_with_validation(
        api="/collections/{collection_name}",
        method="GET",
        path_params={"collection_name": collection_name},
    )
    schema = response.json()["result"]["payload_schema"]
    assert "offers[].shop" not in schema
    assert "offers[].price" in schema

    # Filtering on the remaining per-element index still works
    assert scroll_ids(
        collection_name,
        {"filter": {"must": [{"key": "offers[].price", "match": {"value": 300}}]}},
    ) == [2]
