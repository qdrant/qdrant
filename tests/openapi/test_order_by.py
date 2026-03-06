import datetime
import math
import random

import pytest
from more_itertools import batched

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

total_points = 300


def upsert_points(collection_name, amount=100):
    def maybe_repeated():
        """Descending sequence of possibly repeated floats"""
        repeated_float = float(amount)
        while True:
            repeated_float = repeated_float if random.random() > 0.5 else repeated_float - 1.0
            yield repeated_float

    maybe_repeated_generator = maybe_repeated()

    def date():
        date = datetime.datetime.now()
        while True:
            if random.random() > 0.5:
                date += datetime.timedelta(days=1)
            yield date.isoformat() + "Z"  # RFC-3339 format

    date_generator = date()

    points = [
        {
            "id": i,
            "vector": [0.1 * i] * 4,
            "payload": {
                "city": "London" if i % 3 == 0 else "Moscow",
                "is_middle_split": i > amount * 0.25 and i < amount * 0.75,
                "price": float(amount - i),
                "payload_id": i,
                "multi_id": [i, amount - i + 1],
                "maybe_repeated_float": next(maybe_repeated_generator),
                "date_rfc3339": next(date_generator),
                "date_simple": next(date_generator).split("T")[0],
            },
        }
        for i in range(amount)
    ]

    for batch in batched(points, 50):
        response = request_with_validation(
            api="/collections/{collection_name}/points",
            method="PUT",
            path_params={"collection_name": collection_name},
            query_params={"wait": "true"},
            body={"points": list(batch)},
        )
        assert response.ok


def create_payload_index(collection_name, field_name, field_schema):
    response = request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={"field_name": field_name, "field_schema": field_schema},
    )
    assert response.ok, response.json()


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    upsert_points(collection_name=collection_name, amount=total_points)
    create_payload_index(
        collection_name=collection_name, field_name="city", field_schema="keyword"
    )
    create_payload_index(
        collection_name=collection_name, field_name="is_middle_split", field_schema="bool"
    )
    create_payload_index(collection_name=collection_name, field_name="price", field_schema="float")
    create_payload_index(
        collection_name=collection_name, field_name="maybe_repeated_float", field_schema="float"
    )
    create_payload_index(
        collection_name=collection_name, field_name="payload_id", field_schema="integer"
    )
    create_payload_index(
        collection_name=collection_name, field_name="multi_id", field_schema="integer"
    )
    create_payload_index(
        collection_name=collection_name, field_name="date_rfc3339", field_schema="datetime"
    )
    create_payload_index(
        collection_name=collection_name, field_name="date_simple", field_schema="datetime"
    )
    yield
    drop_collection(collection_name=collection_name)


def test_order_by_int_ascending(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "order_by": {"key": "payload_id", "direction": "asc"},
            "limit": 5,
        },
    )
    assert response.ok, response.json()

    result = response.json()["result"]
    assert len(result["points"]) == 5

    ids = [x["id"] for x in result["points"]]
    assert [0, 1, 2, 3, 4] == ids
    # Offset is not supported for order_by
    assert result["next_page_offset"] == None


def test_order_by_int_descending(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "order_by": {"key": "payload_id", "direction": "desc"},
            "limit": 5,
        },
    )
    assert response.ok, response.json()

    result = response.json()["result"]
    assert len(result["points"]) == 5

    ids = [x["id"] for x in result["points"]]
    
    # We expect the last ids
    expected_ids = [total_points - (i + 1) for i in range(5)]
    assert expected_ids == ids
    # Offset is not supported for order_by
    assert result["next_page_offset"] == None


def paginate_whole_collection(collection_name, key, direction, must=None):
    limit = 23
    pages = 0
    points_count = 0
    points_set = set()
    last_value = None
    last_value_ids = set()
    start_from = None

    # Get filtered total points
    response = request_with_validation(
        api="/collections/{collection_name}/points/count",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "filter": {"must": must},
            "exact": True,
        },
    )
    assert response.ok, response.json()

    expected_points = response.json()["result"]["count"]

    while True:
        response = request_with_validation(
            api="/collections/{collection_name}/points/scroll",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "order_by": {"key": key, "direction": direction, "start_from": start_from},
                "limit": limit,
                "filter": {
                    "must": must,
                    "must_not": [{"has_id": [id_ for id_ in last_value_ids]}],
                },
            },
        )
        assert response.ok, response.json()

        points = response.json()["result"]["points"]

        if len(points) > 0:
            last_value = points[-1]["payload"][key]

        # Exclude the ids we've already seen for the start_from value.
        # This is what we expect the users to do in order to paginate with order_by
        if start_from != last_value:
            last_value_ids.clear()
            start_from = last_value

        same_value_points = [
            point["id"] for point in points if point["payload"][key] == last_value
        ]
        last_value_ids.update(same_value_points)

        points_len = len(response.json()["result"]["points"])

        points_count += points_len
        pages += 1

        # Check no duplicates
        for point in points:
            assert point["id"] not in points_set
            points_set.add(point["id"])

        if points_len < limit:
            break

    try:
        assert math.ceil(expected_points / limit) == pages
        assert expected_points == points_count
    except AssertionError:
        # Check which points we're missing
        response = request_with_validation(
            api="/collections/{collection_name}/points/scroll",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "limit": total_points,
                "filter": {"must": must},
            },
        )
        assert response.ok, response.json()
        filtered_points = set([point["id"] for point in response.json()["result"]["points"]])

        assert filtered_points == points_set, f"Missing points: {filtered_points - points_set}"


@pytest.mark.parametrize(
    "key, direction",
    [
        ("payload_id", "asc"),
        ("payload_id", "desc"),
        ("price", "asc"),
        ("price", "desc"),
        ("maybe_repeated_float", "asc"),
        ("maybe_repeated_float", "desc"),
        ("date_rfc3339", "asc"),
        ("date_rfc3339", "desc"),
        ("date_simple", "asc"),
        ("date_simple", "desc"),
    ],
)
@pytest.mark.timeout(60)  # possibly break of an infinite loop
def test_paginate_whole_collection(collection_name, key, direction):
    paginate_whole_collection(collection_name, key, direction)


@pytest.mark.parametrize(
    "key, direction",
    [
        ("payload_id", "asc"),
        ("payload_id", "desc"),
        ("price", "asc"),
        ("price", "desc"),
        ("maybe_repeated_float", "asc"),
        ("maybe_repeated_float", "desc"),
        ("date_rfc3339", "asc"),
        ("date_rfc3339", "desc"),
        ("date_simple", "asc"),
        ("date_simple", "desc"),
    ],
)
@pytest.mark.timeout(60)  # possibly break of an infinite loop
def test_order_by_pagination_with_filters(collection_name, key, direction):
    musts = [
        [
            {
                "key": "city",
                "match": {
                    "value": "London",
                },
            }
        ],
        [
            {
                "key": "is_middle_split",
                "match": {
                    "value": False,
                },
            }
        ],
    ]

    for must in musts:
        paginate_whole_collection(collection_name, key, direction, must)


def test_multi_values_appear_multiple_times(collection_name):
    limit = total_points * 2

    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "order_by": "multi_id",
            "limit": limit,
        },
    )
    assert response.ok, response.json()

    points = response.json()["result"]["points"]
    assert len(points) == limit

    freqs = {}
    for point in points:
        id_ = point["id"]
        if id_ in freqs:
            freqs[id_] += 1
        else:
            freqs[id_] = 1

    assert all([count == 2 for count in freqs.values()])


def test_cannot_use_offset_with_order_by(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "order_by": "payload_id",
            "offset": 10,
            "limit": 10,
        },
    )
    assert not response.ok
    assert response.status_code == 400
