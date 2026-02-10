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
    """Test that score_threshold works correctly with formula queries"""
    point_id = 8
    formula = {"sum": [{"mult": ["$score", 0.4]}, {"mult": ["price", 0.6]}]}

    # Get original scores
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={"query": point_id},
    )
    points = response.json()["result"]["points"]
    orig_scores = {point.get("id"): point.get("score") for point in points}

    # Query with formula but without score_threshold
    query_no_threshold = {
        "prefetch": {"query": point_id},
        "query": {"formula": formula, "defaults": {"price": 0.0}},
        "with_payload": True,
    }

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body=query_no_threshold,
    )
    assert response.ok, response.json()
    
    result = response.json()["result"]
    points_no_threshold = result["points"]
    assert len(points_no_threshold) > 2, "Should have multiple results without threshold"
    
    # Get the second highest score to use as threshold
    # Scores are returned in descending order by default
    scores_no_threshold = [point.get("score") for point in points_no_threshold]
    second_score = scores_no_threshold[1]
    
    # Set threshold slightly above second score - should return only 1 result
    threshold_above_second = second_score + 0.0001
    
    query_with_threshold = {
        "prefetch": {"query": point_id},
        "query": {"formula": formula, "defaults": {"price": 0.0}},
        "with_payload": True,
        "score_threshold": threshold_above_second,
    }
    
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body=query_with_threshold,
    )
    assert response.ok, response.json()
    
    points_with_threshold = response.json()["result"]["points"]
    
    # Should only return the top result
    assert len(points_with_threshold) == 1, (
        f"Expected 1 result with threshold {threshold_above_second}, got {len(points_with_threshold)}"
    )
    
    # Verify all returned scores are above threshold
    for point in points_with_threshold:
        assert point.get("score") >= threshold_above_second, (
            f"Score {point.get('score')} should be >= threshold {threshold_above_second}"
        )
    
    # Set threshold slightly below second score - should return 2 results
    threshold_below_second = second_score - 0.0001
    
    query_with_threshold["score_threshold"] = threshold_below_second
    
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body=query_with_threshold,
    )
    assert response.ok, response.json()
    
    points_with_threshold = response.json()["result"]["points"]
    
    # Should return top 2 results
    assert len(points_with_threshold) == 2, (
        f"Expected 2 results with threshold {threshold_below_second}, got {len(points_with_threshold)}"
    )
    
    # Verify all returned scores are above threshold
    for point in points_with_threshold:
        assert point.get("score") >= threshold_below_second, (
            f"Score {point.get('score')} should be >= threshold {threshold_below_second}"
        )
