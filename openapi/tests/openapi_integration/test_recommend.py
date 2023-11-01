import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = "test_recommend"


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_default_is_avg_vector():
    params = {
        "positive": [1, 2],
        "negative": [3, 4],
        "exact": True,
        "limit": 10,
    }

    default_response = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            **params,
        },
    )
    assert default_response.ok

    # we should only get 4 because there are 8 vectors and we used 4 as examples
    assert len(default_response.json()["result"]) == 4

    avg_response = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            **params,
            "strategy": "average_vector",
        },
    )
    assert avg_response.ok
    assert len(avg_response.json()["result"]) == 4

    assert default_response.json()["result"] == avg_response.json()["result"]


def test_single_vs_batch():
    # Bunch of valid examples
    params_list = [
        {
            "positive": [1, 2],
            "negative": [3, 4],
            "limit": 1,
        },
        {
            "positive": [1],
            "negative": [3, 4],
            "limit": 1,
        },
        {
            # no negative because it's optional with this strategy
            "negative": [4, 5],
            "exact": True,
            "strategy": "best_score",
            "limit": 1,
        },
        {
            "positive": [2, 3],
            "negative": [4, 5],
            "strategy": "best_score",
            "limit": 1,
        },
        {
            "positive": [2, 3],
            "negative": [4, 5],
            "exact": True,
            "strategy": "best_score",
            "limit": 1,
        },
        {
            "positive": [8],
            "negative": [],
            "exact": True,
            "strategy": "average_vector",
            "limit": 1,
        },
    ]

    batch_response = request_with_validation(
        api="/collections/{collection_name}/points/recommend/batch",
        method="POST",
        path_params={"collection_name": collection_name},
        body={"searches": params_list},
    )

    assert batch_response.ok
    assert len(batch_response.json()["result"]) == len(params_list)

    # Compare against sequential single searches
    for i, params in enumerate(params_list):
        single_response = request_with_validation(
            api="/collections/{collection_name}/points/recommend",
            method="POST",
            path_params={"collection_name": collection_name},
            body=params,
        )
        assert single_response.ok
        assert single_response.json()["result"] == batch_response.json()["result"][i]


def test_without_positives():
    def req_with_positives(positive, strategy=None):
        if strategy is None:
            strat_dict = {}
        else:
            strat_dict = {"strategy": strategy}

        return request_with_validation(
            api="/collections/{collection_name}/points/recommend",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "positive": positive,
                **strat_dict,
                "limit": 2,
            },
        )

    # Assert this is valid
    response = req_with_positives([1, 2])
    assert response.ok

    # But all these are not
    response = req_with_positives([])
    assert response.status_code == 400

    response = req_with_positives([], "average_vector")
    assert response.status_code == 400

    # Also no negative and no positive is invalid with best_score
    response = req_with_positives([], "best_score")
    assert response.status_code == 400


def test_best_score_works_with_only_negatives():
    response = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "negative": [1, 2],
            "strategy": "best_score",
            "limit": 5,
        },
    )
    assert response.ok
    assert len(response.json()["result"]) == 5

    # All scores should be negative
    for result in response.json()["result"]:
        assert result["score"] < 0


def test_only_1_positive_in_best_score_is_equivalent_to_normal_search():
    limit = 4

    # recommendation response
    reco_response = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [1],
            "strategy": "best_score",
            "limit": limit,
            "exact": True,
        },
    )
    assert reco_response.ok
    assert len(reco_response.json()["result"]) == limit

    # Get vector from point 1
    vector = get_points([1])[0]["vector"]

    # Use normal search with that vector
    search_response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": vector,
            "filter": {"must_not": [{"has_id": [1]}]},
            "limit": limit,
            "exact": True,
        },
    )

    assert search_response.ok
    assert len(search_response.json()["result"]) == limit

    # Scores can be different, but the ids and order should be the same
    reco_ids = [result["id"] for result in reco_response.json()["result"]]
    search_ids = [result["id"] for result in search_response.json()["result"]]

    assert reco_ids == search_ids


def get_points(ids: list):
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


def test_raw_vectors():
    points = get_points([1, 2, 3, 4, 5, 6, 7, 8])

    # Assert using ids is the same as using the raw vectors
    response_ids = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [point["id"] for point in points[:2]],
            "negative": [point["id"] for point in points[2:4]],
            "limit": 8,
        },
    )
    assert response_ids.ok
    assert len(response_ids.json()["result"]) == 4

    response_raw = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [point["vector"] for point in points[:2]],
            "negative": [point["vector"] for point in points[2:4]],
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
    assert len(response_raw.json()["result"]) == 4

    assert response_ids.json()["result"] == response_raw.json()["result"]
