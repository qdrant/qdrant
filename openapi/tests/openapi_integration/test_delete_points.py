import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection_delete'


@pytest.fixture(autouse=True)
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_delete_points():
    # delete point by filter (has_id)
    response = request_with_validation(
        api='/collections/{collection_name}/points/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "filter": {
                "must": [
                    {"has_id": [5]}
                ]
            }
        }
    )
    assert response.ok

    # quantity check if the above point id was deleted
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert response.json()['result']['points_count'] == 9
    assert response.json()['result']['vectors_count'] == 10  # We don't propagate deletes to vectors at this time

    response = request_with_validation(
        api='/collections/{collection_name}/points/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [1, 2, 3, 4]
        }
    )
    assert response.ok

    # quantity check if the above point id was deleted
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert response.json()['result']['points_count'] == 5
    assert response.json()['result']['vectors_count'] == 10
