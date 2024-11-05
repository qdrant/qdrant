import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, collection_name):
    multivector_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)

def multivector_collection_setup(
        collection_name='test_collection',
        on_disk_vectors=False):
    drop_collection(collection_name=collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 2,
                "distance": "Dot",
                "on_disk": on_disk_vectors,
                "multivector_config": {
                    "comparator": "max_sim"
                }
            },
        }
    )
    assert response.ok

    # batch upsert
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": [
                        [-1, 1],
                        [-2, 2],
                        [-3, 3]
                    ]

                },
                {
                    "id": 2,
                    "vector": [
                        [-1.5, 1.5],
                        [-2.5, 2.5],
                        [-3.5, 3.5]
                    ]
                },
                {
                    "id": 3,
                    "vector": [
                        [-1.7, 1.7],
                        [-2.7, 2.7],
                        [-3.7, 3.7]
                    ]
                },
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

def test_multi_default_is_avg_vector(collection_name):
    params = {
        "positive": [[1, 2]],
        "negative": [[3, 4]],
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

    # we should only get 3 because we don't have more
    assert len(default_response.json()["result"]) == 3

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
    assert len(avg_response.json()["result"]) == 3

    assert default_response.json()["result"] == avg_response.json()["result"]


def test_multi_single_vs_batch(collection_name):
    # Bunch of valid examples
    params_list = [
        {
            "positive": [1],
            "negative": [3],
            "limit": 1,
        },
        {
            # no negative because it's optional with this strategy
            "negative": [1, 2],
            "exact": True,
            "strategy": "best_score",
            "limit": 1,
        },
        {
            "positive": [1],
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


def test_multi_without_positives(collection_name):
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
    response = req_with_positives([[1, 2]])
    assert response.ok

    # But all these are not
    response = req_with_positives([[]])
    assert response.status_code == 400

    response = req_with_positives([[]], "average_vector")
    assert response.status_code == 400

    # Also no negative and no positive is invalid with best_score
    response = req_with_positives([], "best_score")
    assert response.status_code == 400


def test_multi_best_score_works_with_only_negatives(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/recommend",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "negative": [[1, 2]],
            "strategy": "best_score",
            "limit": 5,
        },
    )
    assert response.ok
    assert len(response.json()["result"]) == 3

    # All scores should be negative
    for result in response.json()["result"]:
        assert result["score"] < 0
