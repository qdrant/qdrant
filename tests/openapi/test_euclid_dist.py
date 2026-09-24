import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


def basic_collection_setup(
    collection_name='test_collection',
    on_disk_vectors=False,
):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 2,
                "distance": "Euclid",
                "on_disk": on_disk_vectors,
            }
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": [0.0, 0.0],
                    "payload": {"city": "Berlin"}
                },
                {
                    "id": 2,
                    "vector": [0.0, 1.0],
                    "payload": {"city": ["Berlin", "London"]}
                },
                {
                    "id": 3,
                    "vector": [-1., -1.],
                    "payload": {"city": ["Berlin", "Moscow"]}
                },
            ]
        }
    )
    assert response.ok


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_search_with_threshold(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}/points/query',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "query": [1., 1.],
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 3

    assert response.json()['result']['points'][0]['id'] == 2
    assert response.json()['result']['points'][1]['id'] == 1
    assert response.json()['result']['points'][2]['id'] == 3

    assert response.json()['result']['points'][0]['score'] - 1.0 < 0.0001
    assert response.json()['result']['points'][1]['score'] - 1.414214 < 0.0001
    assert response.json()['result']['points'][2]['score'] - 2.828427 < 0.0001

    response = request_with_validation(
        api='/collections/{collection_name}/points/query',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "query": [1., 1.],
            "limit": 3,
            "score_threshold": 1.5
        }
    )

    assert response.ok
    assert len(response.json()['result']['points']) == 2

    assert response.json()['result']['points'][0]['id'] == 2
    assert response.json()['result']['points'][1]['id'] == 1

    assert response.json()['result']['points'][0]['score'] - 1.0 < 0.0001
    assert response.json()['result']['points'][1]['score'] - 1.414214 < 0.0001


def test_binary_quantized_euclid_rescore_false_order(collection_name):
    drop_collection(collection_name=collection_name)

    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={
            "vectors": {
                "size": 2,
                "distance": "Euclid",
                "hnsw_config": {"m": 4, "ef_construct": 64},
            },
            "quantization_config": {
                "binary": {"always_ram": True},
            },
        },
    )
    assert response.ok

    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={
            "points": [
                {"id": 0, "vector": [-0.049468, -1.582220]},
                {"id": 1, "vector": [-0.205718, -1.302400]},
                {"id": 2, "vector": [-0.222284, 1.090568]},
                {"id": 3, "vector": [-0.557575, 0.653279]},
                {"id": 4, "vector": [0.773439, -0.274751]},
            ]
        },
    )
    assert response.ok

    response = request_with_validation(
        api="/collections/{collection_name}/points/query",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "query": [1.374882, -1.041541],
            "limit": 5,
            "params": {
                "exact": False,
                "quantization": {"rescore": False},
            },
        },
    )
    assert response.ok

    points = response.json()["result"]["points"]
    assert points[0]["id"] == 4
    assert abs(points[0]["score"] - 0.0) < 1e-4
    assert abs(points[1]["score"] - 1.0) < 1e-4
    assert abs(points[2]["score"] - 1.0) < 1e-4
    assert abs(points[3]["score"] - 1.4142) < 1e-4
    assert abs(points[4]["score"] - 1.4142) < 1e-4

    drop_collection(collection_name=collection_name)

