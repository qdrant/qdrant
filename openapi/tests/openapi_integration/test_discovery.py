import math
import random

import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = "test_discovery"


def random_vector(dim=4):
    return [random.random() for _ in range(dim)]


def random_example(dim=4, min_id=1, max_id=8):
    if random.random() < 0.5:
        return random_vector(dim)
    else:
        return random.randint(min_id, max_id)


def count_ids_in_examples(context_pairs, target) -> int:
    set_ = set()
    for pair in context_pairs:
        for example in pair:
            if isinstance(example, int):
                set_.add(example)
    if isinstance(target, int):
        set_.add(target)
    return len(set_)


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


# Context is when we don't include a target vector
def test_context():
    context_pairs = [
        [random_example(), random_example()],
        [random_example(), random_example()],
    ]
    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "context_pairs": context_pairs,
            "limit": 8,
        },
    )
    assert response.ok, response.json()

    scored_points = response.json()["result"]

    assert len(scored_points) == 8 - count_ids_in_examples(context_pairs, None)

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

    assert len(discover_points) == 8

    # Results should be equal
    assert len(search_points) == len(discover_points)
    assert search_points == discover_points


# Only when we use both target and context, we are doing discovery
def test_discover_same_context():
    target1 = random_example()
    context_pairs = [
        [random_example(), random_example()],
        [random_example(), random_example()],
    ]

    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "target": target1,
            "context_pairs": context_pairs,
            "limit": 8,
            "params": {
                "exact": True,
            },
        },
    )
    assert response.ok, response.json()

    scored_points1 = response.json()["result"]

    assert len(scored_points1) == 8 - count_ids_in_examples(context_pairs, target1)

    target2 = random_example()

    response = request_with_validation(
        api="/collections/{collection_name}/points/discover",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "target": target2,
            "context_pairs": context_pairs,
            "limit": 8,
            "params": {
                "exact": True,
            },
        },
    )
    assert response.ok, response.json()

    scored_points2 = response.json()["result"]

    assert len(scored_points2) == 8 - count_ids_in_examples(context_pairs, target2)

    # We keep same context, so context part of the score (integer part) should be the same,
    # while target part of the score (decimal part) should be different

    scored_points_2_map = {point["id"]: point for point in scored_points2}

    for point1, point2 in zip(scored_points1, scored_points2):
        if point1["id"] in scored_points_2_map:
            point2 = scored_points_2_map[point1["id"]]
            assert math.floor(point1["score"]) == math.floor(point2["score"])

            target_score1 = point1["score"] - math.floor(point1["score"])
            target_score2 = point2["score"] - math.floor(point2["score"])
            if target1 == target2:
                assert math.isclose(target_score1, target_score2, rel_tol=1e-5)
            else:
                assert not math.isclose(target_score1, target_score2, rel_tol=1e-5)


def test_discover_same_target():
    target = random_example()

    context_pairs1 = [
        [random_example(), random_example()],
        [random_example(), random_example()],
    ]

    context_pairs2 = [
        [random_example(), random_example()],
        [random_example(), random_example()],
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

    assert len(scored_points1) == 8 - count_ids_in_examples(context_pairs1, target)

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

    assert len(scored_points2) == 8 - count_ids_in_examples(context_pairs2, target)

    # We keep same target, so context part of the score (integer part) can be different,
    # while target part of the score (decimal part) should be the same
    assert scored_points1 != scored_points2

    print("sorted ⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️")
    print([{"id": point["id"], "score": point["score"]} for point in scored_points1])
    print("\n")
    print([{"id": point["id"], "score": point["score"]} for point in scored_points2])

    scored_points2_map = {point["id"]: point for point in scored_points2}

    for point1 in scored_points1:
        if point1["id"] in scored_points2_map:
            point2 = scored_points2_map[point1["id"]]
            print(point1)
            print(point2)
            target_score1 = point1["score"] - math.floor(point1["score"])
            target_score2 = point2["score"] - math.floor(point2["score"])
            assert math.isclose(target_score1, target_score2, rel_tol=1e-5)

# TODO(luis): Fix missing `Content-Type` header
def test_discover_batch():
    targets = []
    contexts = []
    single_results = []

    # Singles
    for i in range(10):
        target = random_example()
        targets.append(target)

        context_pairs = [
            [random_example(), random_example()],
            [random_example(), random_example()],
        ]
        contexts.append(context_pairs)

        response = request_with_validation(
            api="/collections/{collection_name}/points/discover",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "target": target,
                "context_pairs": context_pairs,
                "limit": 8,
            },
        )
        assert response.ok, response.json()

        single_results.append(response.json()["result"])

    # Batch
    searches = [
        {
            "target": target,
            "context_pairs": context_pairs,
            "limit": 8,
        }
        for target, context_pairs in zip(targets, contexts)
    ]

    response = request_with_validation(
        api="/collections/{collection_name}/points/discover/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "searches": searches,
        },
    )

    batch_results = response.json()["result"]

    assert len(single_results) == len(batch_results)
    for single_result, batch_result in zip(single_results, batch_results):
        assert single_result == batch_result
