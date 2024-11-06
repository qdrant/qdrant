import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_lock_db_for_writes(collection_name):
    response = request_with_validation(
        api='/locks',
        method="POST",
        path_params={},
        body={
            "write": True,
            "error_message": "integration test"
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/locks',
        method="GET",
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
                    "id": 10,
                    "vector": [0.05, -0.61, -0.76, 0.74],
                    "payload": {"city": "Gdansk"}
                }
            ]
        }
    )
    assert not response.ok
    assert "integration test" in response.text

    response = request_with_validation(
        api='/locks',
        method="POST",
        path_params={},
        body={
            "write": False,
        }
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
                    "id": 10,
                    "vector": [0.05, -0.61, -0.76, 0.74],
                    "payload": {"city": "Gdansk"}
                }
            ]
        }
    )
    assert response.ok
