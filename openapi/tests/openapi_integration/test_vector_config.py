import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import drop_collection

collection_name = 'test_collection'


@pytest.fixture(autouse=True)
def setup():
    multivec_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def multivec_collection_setup(collection_name='test_collection', on_disk_payload=False):
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
                    "size": 4,
                    "distance": "Dot",
                    "hnsw_config": {
                        "m": 20,
                    }
                },
                "audio": {
                    "size": 4,
                    "distance": "Dot",
                    "hnsw_config": {
                        "ef_construct": 100
                    }
                },
                "text": {
                    "size": 8,
                    "distance": "Cosine"
                }
            },
            "hnsw_config": {
                "m": 10,
                "ef_construct": 80
            },
            "on_disk_payload": on_disk_payload
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
                    "id": 1,
                    "vector": {
                        "image": [0.05, 0.61, 0.76, 0.74],
                        "audio": [0.05, 0.61, 0.76, 0.74],
                        "text": [0.05, 0.61, 0.76, 0.74, 0.05, 0.61, 0.76, 0.74],
                    },
                    "payload": {"city": "Berlin"}
                },
                {
                    "id": 2,
                    "vector": {
                        "image": [0.19, 0.81, 0.75, 0.11],
                        "audio": [0.19, 0.81, 0.75, 0.11],
                        "text": [0.19, 0.81, 0.75, 0.11, 0.19, 0.81, 0.75, 0.11],
                    },
                    "payload": {"city": ["Berlin", "London"]}
                }
            ]
        }
    )
    assert response.ok


def test_retrieve_vector_specific_hnsw():
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    config = response.json()['result']['config']
    vectors = config['params']['vectors']
    assert vectors['image']['hnsw_config']['m'] == 20
    assert vectors['image']['hnsw_config']['ef_construct'] == None
    assert vectors['audio']['hnsw_config']['m'] == None
    assert vectors['audio']['hnsw_config']['ef_construct'] == 100
    assert vectors['text']['hnsw_config'] is None
    assert config['hnsw_config']['m'] == 10
    assert config['hnsw_config']['ef_construct'] == 80
