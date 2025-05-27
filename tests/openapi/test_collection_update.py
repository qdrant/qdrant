import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

default_name = ""


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_collection_update(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                default_name: {
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
                },
            },
            "sparse_vectors": {
                "sparse-text": {
                    "index": {
                        "on_disk": True,
                    }
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
                },
            ],
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
    assert config["params"]["vectors"]["on_disk"] == on_disk_vectors
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
                default_name: {
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
                },
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
    assert config["params"]["vectors"]["hnsw_config"]["m"] == 32
    assert config["params"]["vectors"]["quantization_config"]["scalar"]["type"] == "int8"
    assert config["params"]["vectors"]["quantization_config"]["scalar"]["quantile"] == 0.8
    assert "always_ram" not in config["params"]["vectors"]["quantization_config"]["scalar"]
    assert config["params"]["vectors"]["on_disk"]
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
                default_name: {
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
    assert config["params"]["vectors"]["hnsw_config"]["m"] == 10
    assert config["params"]["vectors"]["hnsw_config"]["ef_construct"] == 100
    assert config["params"]["vectors"]["quantization_config"]["product"]["compression"] == "x32"
    assert config["params"]["vectors"]["quantization_config"]["product"]["always_ram"]
    assert not config["params"]["vectors"]["on_disk"]
    assert config["params"]["on_disk_payload"] == on_disk_payload
    assert config["quantization_config"]["scalar"]["type"] == "int8"
    assert config["quantization_config"]["scalar"]["quantile"] == 0.99
    assert config["quantization_config"]["scalar"]["always_ram"]


def test_collection_properties_endpoints(collection_name):
    # Initially, properties should be empty
    response = request_with_validation(
        api='/collections/{collection_name}/properties',
        method='GET',
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert response.json() == {}

    # Set multiple properties
    response = request_with_validation(
        api='/collections/{collection_name}/properties',
        method='PATCH',
        path_params={'collection_name': collection_name},
        body={"main_column": "column a", "foo": 123, "bar": {"baz": True}},
    )
    assert response.ok
    assert response.json()["result"] == "ok"

    # Get properties and check values
    response = request_with_validation(
        api='/collections/{collection_name}/properties',
        method='GET',
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    props = response.json()
    assert props["main_column"] == "column a"
    assert props["foo"] == 123
    assert props["bar"] == {"baz": True}

    # Update a property
    response = request_with_validation(
        api='/collections/{collection_name}/properties',
        method='PATCH',
        path_params={'collection_name': collection_name},
        body={"main_column": "column b"},
    )
    assert response.ok
    assert response.json()["result"] == "ok"

    # Confirm update
    response = request_with_validation(
        api='/collections/{collection_name}/properties',
        method='GET',
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert response.json()["main_column"] == "column b"

    # Delete a property
    response = request_with_validation(
        api='/collections/{collection_name}/properties/{key}',
        method='DELETE',
        path_params={'collection_name': collection_name, 'key': 'foo'},
    )
    assert response.ok
    assert response.json()["result"] == "ok"

    # Confirm deletion
    response = request_with_validation(
        api='/collections/{collection_name}/properties',
        method='GET',
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    props = response.json()
    assert "foo" not in props
    assert "main_column" in props

    # Try to PATCH with invalid (non-object) payload
    response = request_with_validation(
        api='/collections/{collection_name}/properties',
        method='PATCH',
        path_params={'collection_name': collection_name},
        body=[1, 2, 3],
    )
    assert response.status_code == 400
    assert "Payload must be a JSON object" in response.text
