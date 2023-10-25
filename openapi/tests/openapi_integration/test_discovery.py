import math
import random

import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = "test_discovery"


def random_vector(dim=4):
    return [random.random() for _ in range(dim)]


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


# Context is when we don't include a target vector
def test_context():
    context_pairs = [
        [random_vector(), random_vector()],
        [random_vector(), random_vector()],
    ]
    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "context_pairs": context_pairs,
            "limit": 3,
        },
    )
    assert response.ok, response.json()

    scored_points = response.json()["result"]

    # Score in context search relates to loss, so max score for context search is 0.0
    for point in scored_points:
        assert point["score"] <= 0.0


# When we only use target, it should be the exact same as search
def test_only_target_is_search():
    target = random_vector()

    # First, search
    response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": target,
            "limit": 8,
        },
    )
    assert response.ok, response.json()

    search_points = response.json()["result"]

    # Then, discover
    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "target": target,
            "limit": 8,
        },
    )
    assert response.ok, response.json()

    discover_points = response.json()["result"]

    # Results should be equal
    assert len(search_points) == len(discover_points)
    assert search_points == discover_points


# Only when we use both target and context, we are doing discovery
def test_discover_same_context():
    target1 = random_vector()
    context_pairs = [
        [random_vector(), random_vector()],
        [random_vector(), random_vector()],
    ]

    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "target": target1,
            "context_pairs": context_pairs,
            "limit": 8,
        },
    )
    assert response.ok, response.json()

    scored_points1 = response.json()["result"]

    target2 = random_vector()
    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "target": target2,
            "context_pairs": context_pairs,
            "limit": 8,
        },
    )
    assert response.ok, response.json()

    scored_points2 = response.json()["result"]

    # We keep same context, so context part of the score (integer part) should be the same,
    # while target part of the score (decimal part) should be different

    for point1, point2 in zip(scored_points1, scored_points2):
        assert math.floor(point1["score"]) == math.floor(point2["score"])

        target_score1 = point1["score"] - math.floor(point1["score"])
        target_score2 = point2["score"] - math.floor(point2["score"])
        assert target_score1 != target_score2


def test_discover_same_target():
    target = random_vector()
    context_pairs1 = [
        [random_vector(), random_vector()],
        [random_vector(), random_vector()],
    ]

    context_pairs2 = [
        [random_vector(), random_vector()],
        [random_vector(), random_vector()],
    ]

    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "target": target,
            "context_pairs": context_pairs1,
            "limit": 8,
        },
    )
    assert response.ok, response.json()

    scored_points1 = response.json()["result"]

    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "target": target,
            "context_pairs": context_pairs2,
            "limit": 8,
        },
    )
    assert response.ok, response.json()

    scored_points2 = response.json()["result"]

    # We keep same target, so context part of the score (integer part) can be different,
    # while target part of the score (decimal part) should be the same
    assert scored_points1 != scored_points2

    # Order by id so we can compare the target scores
    scored_points1.sort(key=lambda x: x["id"])
    scored_points2.sort(key=lambda x: x["id"])
    for point1, point2 in zip(scored_points1, scored_points2):
        print(point1, point2)
        target_score1 = point1["score"] - math.floor(point1["score"])
        target_score2 = point2["score"] - math.floor(point2["score"])
        assert math.isclose(target_score1, target_score2, rel_tol=1e-5)
