import pytest
from math import acosh, asin, cos, isclose, radians, sin, sqrt

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(
        collection_name=collection_name, on_disk_vectors=on_disk_vectors
    )

    response = request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": 'true'},
        body={"field_name": "price", "field_schema": "float"},
    )
    assert response.ok

    response = request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": 'true'},
        body={"field_name": "indexed_locations", "field_schema": "geo"},
    )
    assert response.ok
    yield
    drop_collection(collection_name=collection_name)


@pytest.mark.parametrize(
    "formula,expecting",
    [
        (
            {"sum": [{"mult": ["$score", 0.4]}, {"mult": ["price", 0.6]}]},
            lambda score, price: 0.4 * score + 0.6 * price,
        ),
        (
            {
                "sum": [
                    "$score",
                    # fast sigmoid formula
                    {
                        "div": {
                            "left": "price",
                            "right": {"sum": [1.0, {"abs": "price"}]},
                        }
                    },
                ],
            },
            lambda score, price: score + (price / (1.0 + abs(price))),
        ),
        (
            {"acosh": {"sum": [1.0, {"abs": "price"}]}},
            lambda score, price: acosh(1.0 + abs(price)),
        ),
        (
            {"max": ["$score", "price"]},
            lambda score, price: max(score, price),
        ),
        # More than two operands, and nested sub-expressions
        (
            {"max": [{"mult": ["$score", 3.0]}, "price", 0.0]},
            lambda score, price: max(3.0 * score, price, 0.0),
        ),
        (
            {"min": ["$score", "price"]},
            lambda score, price: min(score, price),
        ),
        # More than two operands, and nested sub-expressions
        (
            {"min": [{"mult": ["$score", 3.0]}, "price", 0.0]},
            lambda score, price: min(3.0 * score, price, 0.0),
        ),
        # max and min of the same operands bracket the operands from both sides
        (
            {"sum": [{"max": ["$score", "price"]}, {"min": ["$score", "price"]}]},
            lambda score, price: max(score, price) + min(score, price),
        ),
    ],
)
def test_formula(collection_name, formula, expecting):
    point_id = 8

    # Get original scores
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={"query": point_id},
    )
    points = response.json()["result"]["points"]
    orig_scores = {point.get("id"): point.get("score") for point in points}

    query = {
        "prefetch": {"query": point_id},
        "query": {"formula": formula, "defaults": {"price": 0.0}},
        "with_payload": True,
    }

    # Formula query
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body=query,
    )
    assert response.ok, response.json()

    # Assert that the response is in descending order
    points = response.json()["result"]["points"]
    scores = [point.get("score") for point in points]
    assert all(scores[i] >= scores[i + 1] for i in range(len(scores) - 1)), (
        "Results should be ordered by score descending"
    )

    # Sanity check that the evaluation was correct
    for point in points:
        orig_score = orig_scores[point.get("id")]
        price_value = point.get("payload").get("price")

        if price_value is list:
            price = price_value[0]
        else:
            price = price_value

        if price is None:
            price = 0.0

        # Calculate expected score according to formula
        expected_score = expecting(orig_score, price)

        point_score = point.get("score")

        # Compare with actual score within floating point precision
        assert isclose(point_score, expected_score, rel_tol=1e-5), (
            f"Expected score {expected_score}, got {point_score}. Point: {point}"
        )

    # Assert that the response contains all points
    assert len(points) == len(orig_scores), "Response should contain all points"

def test_formula_with_score_threshold(collection_name):
    # Insert 4 test points with numeric payload "price" and a group tag
    points = [
        {"id": 1001, "vector": [0.1, 0.1, 0.1, 0.1], "payload": {"price": 0.1, "group": "threshold_test"}},
        {"id": 1002, "vector": [0.2, 0.2, 0.2, 0.2], "payload": {"price": 0.6, "group": "threshold_test"}},
        {"id": 1003, "vector": [0.3, 0.3, 0.3, 0.3], "payload": {"price": 0.4, "group": "threshold_test"}},
        {"id": 1004, "vector": [0.4, 0.4, 0.4, 0.4], "payload": {"price": 0.9, "group": "threshold_test"}},
    ]

    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={"points": points},
    )
    assert response.ok, response.json()

    # Use a formula that sets the score equal to the payload "price"
    formula = "price"

    # Set a threshold of 0.5: expect ids with price >= 0.5 (1002 and 1004)
    threshold = 0.5
    expected_ids = {1002, 1004}
    query = {
        "prefetch": {"limit": 4},
        "query": {"formula": formula, "defaults": {"price": 0.0}},
        "filter": {"must": [{"key": "group", "match": {"value": "threshold_test"}}]},
        "with_payload": True,
        "limit": 10,
        "score_threshold": threshold,
    }

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body=query,
    )
    assert response.ok, response.json()

    points_resp = response.json()["result"]["points"]
    returned_ids = {p.get("id") for p in points_resp}

    # Assert returned ids match expected set
    assert returned_ids == expected_ids, f"Expected ids {expected_ids}, got {returned_ids}"

    # Also assert each returned point has score >= threshold
    for p in points_resp:
        assert p.get("score") >= threshold - 1e-8, (
            f"Point {p.get('id')} with score {p.get('score')} is below threshold {threshold}"
        )


def test_max_matches_the_arithmetic_workaround(collection_name):
    """Before `max` existed, users had to spell it as (a + b + |a - b|) / 2.

    Both forms must produce identical scores, otherwise `max` is not a drop-in replacement
    for the formulas already in the wild.
    """
    point_id = 8
    boosted = {"mult": [3.0, "$score"]}

    workaround = {
        "mult": [
            0.5,
            {
                "sum": [
                    boosted,
                    "price",
                    {"abs": {"sum": [boosted, {"neg": "price"}]}},
                ]
            },
        ]
    }
    direct = {"max": [boosted, "price"]}

    def scores_for(formula):
        response = request_with_validation(
            api="/collections/{collection_name}/points/query",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "prefetch": {"query": point_id},
                "query": {"formula": formula, "defaults": {"price": 0.0}},
                "limit": 10,
            },
        )
        assert response.ok, response.json()
        return {
            point["id"]: point["score"] for point in response.json()["result"]["points"]
        }

    workaround_scores = scores_for(workaround)
    direct_scores = scores_for(direct)

    assert workaround_scores.keys() == direct_scores.keys()
    for point_id, expected in workaround_scores.items():
        assert isclose(direct_scores[point_id], expected, rel_tol=1e-5), (
            f"point {point_id}: max gave {direct_scores[point_id]}, workaround gave {expected}"
        )


def test_empty_max_is_rejected(collection_name):
    """`sum: []` is 0 and `mult: []` is 1, but the max of nothing has no sensible value,
    so it must be rejected rather than silently scoring every point as -infinity."""
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": {"query": 8},
            "query": {"formula": {"max": []}},
        },
    )
    assert not response.ok, response.json()
    assert response.status_code == 400, response.json()


def test_empty_min_is_rejected(collection_name):
    """Same as the empty `max` case: no identity element, so it must be rejected rather than
    silently scoring every point as +infinity."""
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": {"query": 8},
            "query": {"formula": {"min": []}},
        },
    )
    assert not response.ok, response.json()
    assert response.status_code == 400, response.json()


def test_min_matches_the_arithmetic_workaround(collection_name):
    """The identity for a minimum is (a + b - |a - b|) / 2, the sign flip of the max one.

    Same reasoning as the `max` case: `min` must be a drop-in replacement for formulas
    already written out by hand.
    """
    point_id = 8
    boosted = {"mult": [3.0, "$score"]}

    workaround = {
        "mult": [
            0.5,
            {
                "sum": [
                    boosted,
                    "price",
                    {"neg": {"abs": {"sum": [boosted, {"neg": "price"}]}}},
                ]
            },
        ]
    }
    direct = {"min": [boosted, "price"]}

    def scores_for(formula):
        response = request_with_validation(
            api="/collections/{collection_name}/points/query",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "prefetch": {"query": point_id},
                "query": {"formula": formula, "defaults": {"price": 0.0}},
                "limit": 10,
            },
        )
        assert response.ok, response.json()
        return {
            point["id"]: point["score"] for point in response.json()["result"]["points"]
        }

    workaround_scores = scores_for(workaround)
    direct_scores = scores_for(direct)

    assert workaround_scores.keys() == direct_scores.keys()
    for point_id, expected in workaround_scores.items():
        assert isclose(direct_scores[point_id], expected, rel_tol=1e-5), (
            f"point {point_id}: min gave {direct_scores[point_id]}, workaround gave {expected}"
        )


COLOGNE = {"lat": 50.94, "lon": 6.96}
BONN = {"lat": 50.73, "lon": 7.10}
BERLIN = {"lat": 52.52, "lon": 13.40}
MUNICH = {"lat": 48.14, "lon": 11.58}


def haversine_meters(a, b):
    # Same mean Earth radius as the `geo` crate
    earth_radius = 6371008.8
    d_lat = radians(b["lat"] - a["lat"])
    d_lon = radians(b["lon"] - a["lon"])
    h = sin(d_lat / 2) ** 2 + cos(radians(a["lat"])) * cos(radians(b["lat"])) * sin(d_lon / 2) ** 2
    return 2 * earth_radius * asin(sqrt(h))


@pytest.mark.parametrize("field", ["locations", "indexed_locations"])
def test_geo_distance_to_multiple_points(collection_name, field):
    """A point with several locations is scored by its nearest one, read from payload or geo index."""
    group = f"geo_multi_{field}"
    locations_by_id = {
        1101: [COLOGNE],
        1102: [BERLIN, BONN],
        1103: [BERLIN, MUNICH],
    }
    points = [
        {
            "id": point_id,
            "vector": [0.1, 0.2, 0.3, 0.4],
            "payload": {field: locations if len(locations) > 1 else locations[0], "group": group},
        }
        for point_id, locations in locations_by_id.items()
    ]
    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={"points": points},
    )
    assert response.ok, response.json()

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "prefetch": {"limit": 10},
            "query": {"formula": {"mult": [-1, {"geo_distance": {"origin": COLOGNE, "to": field}}]}},
            "filter": {"must": [{"key": "group", "match": {"value": group}}]},
            "limit": 10,
        },
    )
    assert response.ok, response.json()

    points = response.json()["result"]["points"]
    assert [point["id"] for point in points] == [1101, 1102, 1103]

    expected_scores = {
        1101: 0.0,
        1102: -haversine_meters(COLOGNE, BONN),
        1103: -haversine_meters(COLOGNE, MUNICH),
    }
    for point in points:
        assert isclose(point["score"], expected_scores[point["id"]], rel_tol=1e-5), point
