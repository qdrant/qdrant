import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_payload_operations(collection_name):
    # create payload
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1001,
                    "vector": [1.05, 1.61, 1.76, 0.74],
                    "payload": {"location": {"lon": 13.4, "lat": 52.5}}
                },
                {
                    "id": 1002,
                    "vector": [1.19, 1.81, 1.75, 0.11],
                    "payload": {"location": [{"lon": 13.4, "lat": 52.1}, {"lon": 13.2, "lat": 52.5}]}
                },
                {
                    "id": 1003,
                    "vector": [1.36, 1.55, 1.47, 0.94],
                    "payload": {"location": {"lon": 13.4, "lat": 12.5}}
                },
                {
                    "id": 1004,
                    "vector": [1.18, 1.01, 1.85, 0.80],
                    "payload": {"location": {"lon": 12.4, "lat": 52.5}}
                },
                {
                    "id": 1005,
                    "vector": [1.24, 1.18, 1.22, 0.44],
                    "payload": {"location": [{"lon": 12.1, "lat": 62.5}, {"lon": 13.4, "lat": 52.5}]}
                }
            ]
        }
    )
    assert response.ok

    # Create geo index
    # create payload
    # Create index
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": "location",
            "field_schema": "geo"
        }
    )
    assert response.ok

    # Delete point
    # delete point by filter (has_id)
    response = request_with_validation(
        api='/collections/{collection_name}/points/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "filter": {
                "must": [
                    {"has_id": [1004]}
                ]
            }
        }
    )
    assert response.ok
