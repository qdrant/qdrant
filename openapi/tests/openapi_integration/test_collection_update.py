import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import basic_collection_setup, drop_collection

collection_name = 'test_collection_uuid'


@pytest.fixture(autouse=True)
def setup():
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_collection_update():
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "hnsw_config": {
                    "m": 32,
                    "ef_construct": 123,
                },
            },
            "optimizers_config": {
                "default_segment_number": 6,
                "indexing_threshold": 10000,
            },
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
                    "id": 7,
                    "vector": [0.15, 0.31, 0.76, 0.74],
                    "payload": {"city": "Rome"}
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 7},
    )
    assert response.ok


def test_hnsw_update():
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    result = response.json()["result"]
    config = result["config"]
    assert "hnsw_config" not in config["params"]["vectors"]
    assert config["hnsw_config"]["m"] == 16
    assert config["hnsw_config"]["ef_construct"] == 100

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "hnsw_config": {
                    "m": 32,
                },
            },
            "hnsw_config": {
                "ef_construct": 123,
            },
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    result = response.json()["result"]
    config = result["config"]
    assert config["params"]["vectors"]["hnsw_config"]["m"] == 32
    assert config["hnsw_config"]["m"] == 16
    assert config["hnsw_config"]["ef_construct"] == 123

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "hnsw_config": {
                    "m": 10,
                    "ef_construct": 100,
                },
            },
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    result = response.json()["result"]
    config = result["config"]
    assert config["params"]["vectors"]["hnsw_config"]["m"] == 10
    assert config["params"]["vectors"]["hnsw_config"]["ef_construct"] == 100
