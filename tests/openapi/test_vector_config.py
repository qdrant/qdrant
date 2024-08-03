import random
from time import sleep
import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import drop_collection

collection_name = 'test_collection'


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, on_disk_payload):
    multivec_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors, on_disk_payload=on_disk_payload)
    yield
    drop_collection(collection_name=collection_name)


def multivec_collection_setup(collection_name='test_collection', on_disk_vectors=False, on_disk_payload=False):
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
                    },
                    "on_disk": on_disk_vectors,
                },
                "audio": {
                    "size": 4,
                    "distance": "Dot",
                    "hnsw_config": {
                        "ef_construct": 100
                    },
                    "quantization_config": {
                        "scalar": {
                            "type": "int8",
                            "quantile": 0.6
                        }
                    },
                    "on_disk": on_disk_vectors,
                },
                "text": {
                    "size": 8,
                    "distance": "Cosine",
                    "quantization_config": {
                        "scalar": {
                            "type": "int8",
                            "always_ram": True
                        }
                    },
                    "on_disk": on_disk_vectors,
                },
            },
            "hnsw_config": {
                "m": 10,
                "ef_construct": 80
            },
            "quantization": {
                "scalar": {
                    "type": "int8",
                    "quantile": 0.5
                }
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


def test_retrieve_vector_specific_hnsw(on_disk_vectors):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    config = response.json()['result']['config']
    vectors = config['params']['vectors']
    assert vectors['image']['hnsw_config']['m'] == 20
    assert 'ef_construct' not in vectors['image']['hnsw_config']
    assert vectors['image']['on_disk'] == on_disk_vectors
    assert 'm' not in vectors['audio']['hnsw_config']
    assert vectors['audio']['hnsw_config']['ef_construct'] == 100
    assert vectors['audio']['on_disk'] == on_disk_vectors
    assert 'hnsw_config' not in vectors['text']
    assert vectors['text']['on_disk'] == on_disk_vectors
    assert config['hnsw_config']['m'] == 10
    assert config['hnsw_config']['ef_construct'] == 80


def test_retrieve_vector_specific_quantization(on_disk_vectors):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    config = response.json()['result']['config']
    vectors = config['params']['vectors']
    assert 'quantization_config' not in vectors['image']
    assert vectors['image']['on_disk'] == on_disk_vectors
    assert vectors['audio']['quantization_config']['scalar']['type'] == "int8"
    assert vectors['audio']['quantization_config']['scalar']['quantile'] == 0.6
    assert 'always_ram' not in vectors['audio']['quantization_config']['scalar']
    assert vectors['audio']['on_disk'] == on_disk_vectors
    assert vectors['text']['quantization_config']['scalar']['type'] == "int8"
    assert 'quantile' not in vectors['text']['quantization_config']['scalar']
    assert vectors['text']['quantization_config']['scalar']['always_ram']
    assert vectors['text']['on_disk'] == on_disk_vectors
    assert config['quantization_config']['scalar']['type'] == "int8"
    assert config['quantization_config']['scalar']['quantile'] == 0.5


@pytest.mark.timeout(20)
def test_disable_indexing(on_disk_vectors):
    indexed_name = 'test_collection_indexed'
    unindexed_name = 'test_collection_unindexed'

    drop_collection(collection_name=indexed_name)
    drop_collection(collection_name=unindexed_name)

    def create_collection(collection_name, indexing_threshold, on_disk_vectors):
        response = request_with_validation(
            api='/collections/{collection_name}',
            method="PUT",
            path_params={'collection_name': collection_name},
            body={
                "vectors": {
                    "size": 256,
                    "distance": "Dot",
                    "on_disk": on_disk_vectors,
                },
                "optimizers_config": {
                    "indexing_threshold": indexing_threshold
                }
            }
        )
        assert response.ok

    amount_of_vectors = 100

    # Collection with indexing enabled
    create_collection(indexed_name, 10, on_disk_vectors)
    insert_vectors(indexed_name, amount_of_vectors)

    # Collection with indexing disabled
    create_collection(unindexed_name, 0, on_disk_vectors)
    insert_vectors(unindexed_name, amount_of_vectors)

    while True:
        try:
            # Get info indexed
            response = request_with_validation(
                method='GET',
                api='/collections/{collection_name}',
                path_params={'collection_name': indexed_name},
            )
            assert response.ok

            assert response.json()['result']['points_count'] == amount_of_vectors
            assert response.json()['result']['indexed_vectors_count'] > 0

            # Get info unindexed
            response = request_with_validation(
                method='GET',
                api='/collections/{collection_name}',
                path_params={'collection_name': unindexed_name},
            )
            assert response.ok
            assert response.json()['result']['points_count'] == amount_of_vectors
            assert response.json()['result']['indexed_vectors_count'] == 0
            break
        except AssertionError:
            sleep(0.1)
            continue

    # Cleanup
    drop_collection(collection_name=indexed_name)
    drop_collection(collection_name=unindexed_name)


def insert_vectors(collection_name='test_collection', count=2000, size=256):
    ids = [x for x in range(count)]
    vectors = [[random.random() for _ in range(size)] for _ in range(count)]

    batch_size = 1000
    start = 0
    end = 0
    while end < count:
        end = min(end + batch_size, count)

        response = request_with_validation(
            api='/collections/{collection_name}/points',
            method='PUT',
            path_params={'collection_name': collection_name},
            query_params={'wait': 'true'},
            body={
                "batch": {
                    "ids": ids[start:end],
                    "vectors": vectors[start:end],
                }
            }
        )
        assert response.ok

        start += batch_size
