import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_search_matrix_pairs(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/search/matrix/pairs",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "sample": 10,
            "limit": 2,
        },
    )
    assert response.ok
    search_result = response.json()["result"]
    expected_result = {
        "pairs": [
            {"a": 1, "b": 3, "score": 1.4063001},
            {"a": 1, "b": 4, "score": 1.2531},
            {"a": 2, "b": 1, "score": 1.1550001},
            {"a": 2, "b": 8, "score": 1.1359},
            {"a": 3, "b": 1, "score": 1.4063001},
            {"a": 3, "b": 4, "score": 1.2218001},
            {"a": 4, "b": 1, "score": 1.2531},
            {"a": 4, "b": 3, "score": 1.2218001},
            {"a": 5, "b": 3, "score": 0.70239997},
            {"a": 5, "b": 1, "score": 0.6146},
            {"a": 6, "b": 3, "score": 0.6353},
            {"a": 6, "b": 4, "score": 0.5093},
            {"a": 7, "b": 3, "score": 1.0990001},
            {"a": 7, "b": 1, "score": 1.0349001},
            {"a": 8, "b": 2, "score": 1.1359},
            {"a": 8, "b": 3, "score": 1.0553},
        ]
    }
    assert search_result == expected_result


def test_search_matrix_offsets(collection_name):
    response = request_with_validation(
        api="/collections/{collection_name}/points/search/matrix/offsets",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "sample": 10,
            "limit": 2,
        },
    )
    assert response.ok
    search_result = response.json()["result"]
    expected_result = {
        'offsets_row': [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7],
        'offsets_col': [2, 3, 0, 7, 0, 3, 0, 2, 2, 0, 2, 3, 2, 0, 1, 2],
        'scores': [1.4063001, 1.2531, 1.1550001, 1.1359, 1.4063001, 1.2218001, 1.2531, 1.2218001, 0.70239997, 0.6146, 0.6353, 0.5093, 1.0990001, 1.0349001, 1.1359, 1.0553],
        'ids': [1, 2, 3, 4, 5, 6, 7, 8]
    }
    assert search_result == expected_result
