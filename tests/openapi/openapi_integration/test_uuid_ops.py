import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import basic_collection_setup, drop_collection

collection_name = 'test_collection_uuid'


@pytest.fixture(autouse=True)
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_uuid_operations():
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": "b524a3c4-c568-4383-8019-c9ca08243d6a",
                    "vector": [0.15, 0.21, 0.96, 0.04],
                    "payload": {"city": "Berlin"}
                },
                {
                    "id": "1d675313-d3dd-4646-8b98-8052364872da",
                    "vector": [0.19, 0.81, 0.75, 0.11],
                    "payload": {"city": ["Berlin", "London"]}
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': "b524a3c4-c568-4383-8019-c9ca08243d6a"},
    )
    assert response.ok
