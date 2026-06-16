import random

import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


VECTOR_SIZE = 64
NUM_POINTS = 50


def _random_vector(rng):
    return [rng.uniform(-1.0, 1.0) for _ in range(VECTOR_SIZE)]


def turboquant_collection_setup(collection_name, on_disk_vectors, on_disk_payload):
    drop_collection(collection_name=collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "image": {
                    "size": VECTOR_SIZE,
                    "distance": "Cosine",
                    "on_disk": on_disk_vectors,
                    "quantization_config": {
                        "turbo": {
                            "bits": "bits4",
                            "always_ram": True,
                        }
                    },
                },
                "audio": {
                    "size": VECTOR_SIZE,
                    "distance": "Dot",
                    "on_disk": on_disk_vectors,
                    "quantization_config": {
                        "turbo": {
                            "bits": "bits2",
                        }
                    },
                },
                # Euclid + bits1_5 covers the L2 score path and the only
                # bit size whose `padded_dim` rounds up to 3*dim/2.
                "text": {
                    "size": VECTOR_SIZE,
                    "distance": "Euclid",
                    "on_disk": on_disk_vectors,
                    "quantization_config": {
                        "turbo": {
                            "bits": "bits1_5",
                        }
                    },
                },
            },
            "quantization_config": {
                "turbo": {
                    "bits": "bits1",
                    "always_ram": True,
                }
            },
            "on_disk_payload": on_disk_payload,
        }
    )
    assert response.ok

    rng = random.Random(42)
    points = []
    for point_id in range(1, NUM_POINTS + 1):
        points.append({
            "id": point_id,
            "vector": {
                "image": _random_vector(rng),
                "audio": _random_vector(rng),
                "text": _random_vector(rng),
            },
            "payload": {"city": "Berlin" if point_id % 2 == 0 else "London"},
        })

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"points": points}
    )
    assert response.ok


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, on_disk_payload, collection_name):
    turboquant_collection_setup(
        collection_name=collection_name,
        on_disk_vectors=on_disk_vectors,
        on_disk_payload=on_disk_payload,
    )
    yield
    drop_collection(collection_name=collection_name)


def test_turboquant_config_persists(on_disk_vectors, collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    config = response.json()['result']['config']
    vectors = config['params']['vectors']

    assert vectors['image']['quantization_config']['turbo']['bits'] == "bits4"
    assert vectors['image']['quantization_config']['turbo']['always_ram'] is True
    assert vectors['image']['on_disk'] == on_disk_vectors

    assert vectors['audio']['quantization_config']['turbo']['bits'] == "bits2"
    assert 'always_ram' not in vectors['audio']['quantization_config']['turbo']
    assert vectors['audio']['on_disk'] == on_disk_vectors

    assert vectors['text']['quantization_config']['turbo']['bits'] == "bits1_5"
    assert 'always_ram' not in vectors['text']['quantization_config']['turbo']
    assert vectors['text']['on_disk'] == on_disk_vectors

    assert config['quantization_config']['turbo']['bits'] == "bits1"
    assert config['quantization_config']['turbo']['always_ram'] is True


# (vector_name, descending) — Euclid orders ascending (lower = closer).
@pytest.mark.parametrize("vector_name,descending", [
    ("image", True),
    ("audio", True),
    ("text", False),
])
def test_turboquant_search(collection_name, vector_name, descending):
    rng = random.Random(123)
    query_vector = _random_vector(rng)

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": {"name": vector_name, "vector": query_vector},
            "limit": 10,
        }
    )
    assert response.ok
    result = response.json()['result']
    assert len(result) == 10
    assert all('score' in hit for hit in result)
    assert all('id' in hit for hit in result)
    scores = [hit['score'] for hit in result]
    assert scores == sorted(scores, reverse=descending)


@pytest.mark.parametrize("vector_name", ["image", "audio", "text"])
def test_turboquant_search_with_filter(collection_name, vector_name):
    rng = random.Random(7)
    query_vector = _random_vector(rng)

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": {"name": vector_name, "vector": query_vector},
            "filter": {
                "must": [{"key": "city", "match": {"value": "Berlin"}}]
            },
            "limit": 5,
            "with_payload": True,
        }
    )
    assert response.ok
    result = response.json()['result']
    assert len(result) > 0
    for hit in result:
        assert hit['payload']['city'] == "Berlin"


def test_turboquant_query(collection_name):
    rng = random.Random(99)
    query_vector = _random_vector(rng)

    response = request_with_validation(
        api='/collections/{collection_name}/points/query',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "query": query_vector,
            "using": "image",
            "limit": 5,
            "with_payload": True,
        }
    )
    assert response.ok
    points = response.json()['result']['points']
    assert len(points) == 5


@pytest.mark.parametrize("rescore", [True, False])
def test_turboquant_search_with_quantization_params(collection_name, rescore):
    # rescore=True hits the original-vector lookup path after the quantized
    # first pass; rescore=False returns quantized scores directly.
    rng = random.Random(2024)
    query_vector = _random_vector(rng)

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": {"name": "image", "vector": query_vector},
            "params": {
                "quantization": {
                    "ignore": False,
                    "rescore": rescore,
                    "oversampling": 2.0,
                }
            },
            "limit": 10,
        }
    )
    assert response.ok
    assert len(response.json()['result']) == 10


def test_turboquant_via_patch():
    # Mirrors the scalar/product PATCH pattern in test_collection_update.py:
    # a plain collection becomes a turbo-quantized one through PATCH, exercising
    # the QuantizationConfigDiff::Turbo branch in lib/collection/src/operations/config_diff.rs.
    name = "test_turboquant_via_patch"
    drop_collection(collection_name=name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': name},
        body={"vectors": {"size": VECTOR_SIZE, "distance": "Cosine"}},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': name},
        body={
            "quantization_config": {
                "turbo": {"bits": "bits2", "always_ram": True}
            }
        },
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': name},
    )
    assert response.ok
    quant = response.json()['result']['config']['quantization_config']
    assert quant['turbo']['bits'] == "bits2"
    assert quant['turbo']['always_ram'] is True

    drop_collection(collection_name=name)


def test_turboquant_default_bits():
    # Smoke test that omitting `bits` (server picks the default) is accepted
    # end-to-end through the API.
    name = "test_turboquant_default_bits"
    drop_collection(collection_name=name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': name},
        body={
            "vectors": {
                "size": VECTOR_SIZE,
                "distance": "Cosine",
            },
            "quantization_config": {
                "turbo": {}
            },
        }
    )
    assert response.ok

    rng = random.Random(0)
    points = [
        {"id": i, "vector": _random_vector(rng)}
        for i in range(1, 11)
    ]
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': name},
        query_params={'wait': 'true'},
        body={"points": points},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': name},
        body={"vector": _random_vector(random.Random(1)), "limit": 5},
    )
    assert response.ok
    assert len(response.json()['result']) == 5

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': name},
    )
    assert response.ok
    assert 'turbo' in response.json()['result']['config']['quantization_config']

    drop_collection(collection_name=name)