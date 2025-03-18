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
