import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_mix_collection_setup'


@pytest.fixture(autouse=True)
def setup():
    mix_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def mix_collection_setup(
        collection_name='test_collection',
        on_disk_payload=False,
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
                "image": {
                    "size": 2,
                    "distance": "Euclid"
                }
            },
            "sparse_vectors": {
                "text": {}
            },
            "on_disk_payload": on_disk_payload,
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
                    "id": 1, "vector": {"image": [1, 2], "text": {"indices": [100, 500, 10], "values": [0.9, 0.8, 0.5]}}
                },
                {
                    "id": 2, "vector": {"image": [1, 3], "text": {"indices": [10], "values": [0.1]}}
                },
                {
                    "id": 3, "vector": {"image": [2, 0], "text": {"indices": [55, 20, 101], "values": [0.4, 0.6, 0.1]}}
                },
                {
                    "id": 4, "vector": {"image": [1, 1], "text": {"indices": [66, 12], "values": [0.5, 0.5]}}
                },
                {
                    "id": 5, "vector": {"image": [0, 0], "text": {"indices": [1, 2, 3], "values": [0.1, 0.2, 0.3]}}
                }
            ]
        }
    )
    assert response.ok


def test_collection_mix_batch_update():
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1, "vector": {"image": [1, 1], "text": {"indices": [100, 500, 10], "values": [0.9, 0.8, 0.5]}}
                },
                {
                    "id": 2, "vector": {"image": [2, 2], "text": {"indices": [10, 100, 500], "values": [0.1, 0, 0]}}
                },
                {
                    "id": 3, "vector": {"image": [3, 3], "text": {"indices": [55, 20, 101], "values": [0.4, 0.6, 0.1]}}
                },
                {
                    "id": 4, "vector": {"image": [4, 4], "text": {"indices": [66, 12], "values": [0.5, 0.5]}}
                },
                {
                    "id": 5, "vector": {"image": [5, 5], "text": {"indices": [1, 2, 3], "values": [0.1, 0.2, 0.3]}}
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={"limit": 10, "with_vector": True}
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 5
    results = response.json()['result']['points']

    assert results[2]['vector']['image'] == [3, 3]
    assert results[2]['vector']['text']['indices'] == [20, 55, 101]
