import pytest
from math import isclose

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
