import pytest

from .helpers.collection_setup import drop_collection, multivec_collection_setup
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, collection_name):
    multivec_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_collection_update_multivec(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "image": {
                    "hnsw_config": {
                        "m": 32,
                        "ef_construct": 123,
                    },
                    "quantization_config": {
                        "scalar": {
                            "type": "int8",
                            "quantile": 0.8
                        }
                    },
                }
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
                    "vector": {
                        "image": [0.15, 0.31, 0.76, 0.74]
                    },
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


def test_edit_collection_params(on_disk_vectors, on_disk_payload, collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    result = response.json()["result"]
    config = result["config"]
    assert "hnsw_config" not in config["params"]["vectors"]
    assert "quantization_config" not in config["params"]["vectors"]
    assert config["params"]["vectors"]["text"]["on_disk"] == on_disk_vectors
    assert not config["params"]["on_disk_payload"]
    assert config["hnsw_config"]["m"] == 16
    assert config["hnsw_config"]["ef_construct"] == 100
    assert config["quantization_config"] is None

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "text": {
                    "hnsw_config": {
                        "m": 32,
                    },
                    "quantization_config": {
                        "scalar": {
                            "type": "int8",
                            "quantile": 0.8
                        }
                    },
                    "on_disk": True,
                }
            },
            "hnsw_config": {
                "ef_construct": 123,
            },
            "quantization_config": {
                "scalar": {
                    "type": "int8",
                    "quantile": 0.99,
                    "always_ram": True
                }
            },
            "params": {
                "on_disk_payload": on_disk_payload,
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
    assert config["params"]["vectors"]["text"]["hnsw_config"]["m"] == 32
    assert config["params"]["vectors"]["text"]["quantization_config"]["scalar"]["type"] == "int8"
    assert config["params"]["vectors"]["text"]["quantization_config"]["scalar"]["quantile"] == 0.8
    assert "always_ram" not in config["params"]["vectors"]["text"]["quantization_config"]["scalar"]
    assert config["params"]["vectors"]["text"]["on_disk"]
    assert config["params"]["on_disk_payload"] == on_disk_payload
    assert config["hnsw_config"]["m"] == 16
    assert config["hnsw_config"]["ef_construct"] == 123
    assert config["quantization_config"]["scalar"]["type"] == "int8"
    assert config["quantization_config"]["scalar"]["quantile"] == 0.99
    assert config["quantization_config"]["scalar"]["always_ram"]

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "text": {
                    "hnsw_config": {
                        "m": 10,
                        "ef_construct": 100,
                    },
                    "quantization_config": {
                        "product": {
                            "compression": "x32",
                            "always_ram": True
                        }
                    },
                    "on_disk": False,
                },
            },
            "params": {
                "on_disk_payload": not on_disk_payload,
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
    assert config["params"]["vectors"]["text"]["hnsw_config"]["m"] == 10
    assert config["params"]["vectors"]["text"]["hnsw_config"]["ef_construct"] == 100
    assert config["params"]["vectors"]["text"]["quantization_config"]["product"]["compression"] == "x32"
    assert config["params"]["vectors"]["text"]["quantization_config"]["product"]["always_ram"]
    assert not config["params"]["vectors"]["text"]["on_disk"]
    assert config["params"]["on_disk_payload"] != on_disk_payload
    assert config["quantization_config"]["scalar"]["type"] == "int8"
    assert config["quantization_config"]["scalar"]["quantile"] == 0.99
    assert config["quantization_config"]["scalar"]["always_ram"]

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "text": {
                    "quantization_config": "Disabled"
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
    assert config["params"]["vectors"]["text"].get("quantization_config") is None


def test_invalid_vector_name(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "i_do_no_exist": {
                    "hnsw_config": {
                        "m": 32,
                    }
                }
            },
        }
    )
    assert not response.ok
    assert response.status_code == 400
    error = response.json()["status"]["error"]
    assert error == "Wrong input: Not existing vector name error: i_do_no_exist"
