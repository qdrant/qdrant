import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_default_is_avg_vector(collection_name):
    examples = {
        "positive": [1, 2],
        "negative": [3, 4],
    }

    default_response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {"recommend": examples},
            "params": {"exact": True},
            "limit": 10,
        },
    )
    assert default_response.ok

    # we should only get 4 because there are 8 vectors and we used 4 as examples
    assert len(default_response.json()["result"]["points"]) == 4

    avg_response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {"recommend": {**examples, "strategy": "average_vector"}},
            "params": {"exact": True},
            "limit": 10,
        },
    )
    assert avg_response.ok
    assert len(avg_response.json()["result"]["points"]) == 4

    assert default_response.json()["result"] == avg_response.json()["result"]


def test_single_vs_batch(collection_name):
    # Bunch of valid examples
    params_list = [
        {
            "query": {"recommend": {"positive": [1, 2], "negative": [3, 4]}},
            "limit": 1,
        },
        {
            "query": {"recommend": {"positive": [1], "negative": [3, 4]}},
            "limit": 1,
        },
        {
            # no positive because it's optional with this strategy
            "query": {"recommend": {"negative": [4, 5], "strategy": "best_score"}},
            "params": {"exact": True},
            "limit": 1,
        },
        {
            "query": {"recommend": {"positive": [2, 3], "negative": [4, 5], "strategy": "best_score"}},
            "limit": 1,
        },
        {
            "query": {"recommend": {"positive": [2, 3], "negative": [4, 5], "strategy": "best_score"}},
            "params": {"exact": True},
            "limit": 1,
        },
        {
            "query": {"recommend": {"positive": [8], "negative": [], "strategy": "average_vector"}},
            "params": {"exact": True},
            "limit": 1,
        },
    ]

    batch_response = request_with_validation(
        api="/collections/{collection_name}/points/query/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={"searches": params_list},
    )

    assert batch_response.ok
    assert len(batch_response.json()["result"]) == len(params_list)

    # Compare against sequential single searches
    for i, params in enumerate(params_list):
        single_response = request_with_validation(
            api="/collections/{collection_name}/points/query",
            method="POST",
            path_params={"collection_name": collection_name},
            body=params,
        )
        assert single_response.ok
        assert single_response.json()["result"] == batch_response.json()["result"][i]


def test_without_positives(collection_name):
    def req_with_positives(positive, strategy=None):
        recommend = {"positive": positive}
        if strategy is not None:
            recommend["strategy"] = strategy

        return request_with_validation(
            api="/collections/{collection_name}/points/query",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "query": {"recommend": recommend},
                "limit": 2,
            },
        )

    # Assert this is valid
    response = req_with_positives([1, 2])
    assert response.ok

    # But all these are not. 422, not 400: giving no examples at all violates a
    # `RecommendInput` validation rule, so it is rejected before the query runs.
    response = req_with_positives([])
    assert response.status_code == 422

    response = req_with_positives([], "average_vector")
    assert response.status_code == 422

    # Also no negative and no positive is invalid with best_score
    response = req_with_positives([], "best_score")
    assert response.status_code == 422


def test_best_score_works_with_only_negatives(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {"recommend": {"negative": [1, 2], "strategy": "best_score"}},
            "limit": 5,
        },
    )
    assert response.ok
    assert len(response.json()["result"]["points"]) == 5

    # All scores should be negative
    for result in response.json()["result"]["points"]:
        assert result["score"] < 0


def test_only_1_positive_in_best_score_is_equivalent_to_normal_search(collection_name):
    limit = 4

    # recommendation response
    reco_response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {"recommend": {"positive": [1], "strategy": "best_score"}},
            "params": {"exact": True},
            "limit": limit,
        },
    )
    assert reco_response.ok
    assert len(reco_response.json()["result"]["points"]) == limit

    # Get vector from point 1
    vector = get_points(collection_name, [1])[0]["vector"]

    # Use nearest query with that vector
    search_response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": vector,
            "filter": {"must_not": [{"has_id": [1]}]},
            "params": {"exact": True},
            "limit": limit,
        },
    )

    assert search_response.ok
    assert len(search_response.json()["result"]["points"]) == limit

    # Scores can be different, but the ids and order should be the same
    reco_ids = [result["id"] for result in reco_response.json()["result"]["points"]]
    search_ids = [result["id"] for result in search_response.json()["result"]["points"]]

    assert reco_ids == search_ids


def get_points(collection_name, ids: list):
    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "ids": ids,
            "with_vector": True,
        },
    )
    assert response.ok
    return response.json()["result"]


def test_raw_vectors(collection_name):
    points = get_points(collection_name, [1, 2, 3, 4, 5, 6, 7, 8])

    # Assert using ids is the same as using the raw vectors
    response_ids = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "recommend": {
                    "positive": [point["id"] for point in points[:2]],
                    "negative": [point["id"] for point in points[2:4]],
                }
            },
            "limit": 8,
        },
    )
    assert response_ids.ok
    assert len(response_ids.json()["result"]["points"]) == 4

    response_raw = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": {
                "recommend": {
                    "positive": [point["vector"] for point in points[:2]],
                    "negative": [point["vector"] for point in points[2:4]],
                }
            },
            "limit": 8,
            "filter": {
                "must_not": [
                    {
                        # simulate using ids behavior
                        "has_id": [point["id"] for point in points[:4]]
                    }
                ]
            },
        },
    )
    assert response_raw.ok
    assert len(response_raw.json()["result"]["points"]) == 4

    assert response_ids.json()["result"] == response_raw.json()["result"]
