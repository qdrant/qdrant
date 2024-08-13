import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = "test_distance_matrix"


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_search_matrix_pairs():
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
        'rows': [
            [[2, 1.4063001], [3, 1.2531]],
            [[0, 1.1550001], [7, 1.1359]],
            [[0, 1.4063001], [3, 1.2218001]],
            [[0, 1.2531], [2, 1.2218001]],
            [[2, 0.70239997], [0, 0.6146]],
            [[2, 0.6353], [3, 0.5093]],
            [[2, 1.0990001], [0, 1.0349001]],
            [[1, 1.1359], [2, 1.0553]]],
        'ids': [1, 2, 3, 4, 5, 6, 7, 8]
    }
    assert search_result == expected_result


def test_search_matrix_offsets():
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


def test_search_matrix_rows():
    response = request_with_validation(
        api="/collections/{collection_name}/points/search/matrix/rows",
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
        'rows': [
            {'offsets_id': [2, 3], 'scores': [1.4063001, 1.2531]},
            {'offsets_id': [0, 7], 'scores': [1.1550001, 1.1359]},
            {'offsets_id': [0, 3], 'scores': [1.4063001, 1.2218001]},
            {'offsets_id': [0, 2], 'scores': [1.2531, 1.2218001]},
            {'offsets_id': [2, 0], 'scores': [0.70239997, 0.6146]},
            {'offsets_id': [2, 3], 'scores': [0.6353, 0.5093]},
            {'offsets_id': [2, 0], 'scores': [1.0990001, 1.0349001]},
            {'offsets_id': [1, 2], 'scores': [1.1359, 1.0553]}],
        'ids': [1, 2, 3, 4, 5, 6, 7, 8]
    }
    assert search_result == expected_result
