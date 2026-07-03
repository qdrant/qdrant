import random
import time

import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


VECTOR_SIZE = 64
NUM_POINTS = 50


def _random_vector(rng, size=VECTOR_SIZE):
    return [rng.uniform(-1.0, 1.0) for _ in range(size)]


def _make_points(rng, num_points, vector_names):
    points = []
    for point_id in range(1, num_points + 1):
        points.append({
            "id": point_id,
            "vector": {name: _random_vector(rng) for name in vector_names},
        })
    return points


def turbo4_collection_setup(collection_name, on_disk_vectors, on_disk_payload):
    drop_collection(collection_name=collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                name: {
                    "size": VECTOR_SIZE,
                    "distance": distance,
                    "on_disk": on_disk_vectors,
                    "datatype": "turbo4",
                }
                for name, distance in
                (("image", "Cosine"), ("audio", "Dot"), ("text", "Euclid"))
            },
            "on_disk_payload": on_disk_payload,
        }
    )
    assert response.ok

    rng = random.Random(42)
    points = _make_points(rng, NUM_POINTS, ["image", "audio", "text"])

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
    turbo4_collection_setup(
        collection_name=collection_name,
        on_disk_vectors=on_disk_vectors,
        on_disk_payload=on_disk_payload,
    )
    yield
    drop_collection(collection_name=collection_name)


def test_turbo4_config_persists(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    vectors = response.json()['result']['config']['params']['vectors']
    for name in ("image", "audio", "text"):
        assert vectors[name]['datatype'] == "turbo4"


# (vector_name, descending) — Euclid orders ascending (lower = closer).
@pytest.mark.parametrize("vector_name,descending", [
    ("image", True),
    ("audio", True),
    ("text", False),
])
def test_turbo4_search(collection_name, vector_name, descending):
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
    scores = [hit['score'] for hit in result]
    assert scores == sorted(scores, reverse=descending)


###############################################################################
# Quantization on top of Turbo4 storage.
#
# A Turbo4 source is dequantized to `f32` before re-quantization (see
# `quantized_scorer_builder.rs`), so any quantization config may be layered
# on top of the turbo4 datatype.
###############################################################################

QUANT_NUM_POINTS = 100

QUANT_CONFIGS = {
    # (distance, quantization_config)
    # No scalar here: int8 over a 4-bit source doubles memory for nothing,
    # so it is not a practical combination (unit tests still cover it).
    "binary": ("Dot", {"binary": {"always_ram": True}}),
    "turbo": ("Cosine", {"turbo": {"bits": "bits2", "always_ram": True}}),
}


def _wait_collection_green(collection_name, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        response = request_with_validation(
            api='/collections/{collection_name}',
            method="GET",
            path_params={'collection_name': collection_name},
        )
        assert response.ok
        if response.json()['result']['status'] == 'green':
            return
        time.sleep(0.2)
    raise Exception(f"Collection {collection_name} did not turn green in {timeout}s")


@pytest.fixture(scope="module")
def quant_collection_name(on_disk_vectors, collection_name):
    name = f"{collection_name}_quantized"
    drop_collection(collection_name=name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': name},
        body={
            "vectors": {
                vector_name: {
                    "size": VECTOR_SIZE,
                    "distance": distance,
                    "on_disk": on_disk_vectors,
                    "datatype": "turbo4",
                    "quantization_config": quantization,
                }
                for vector_name, (distance, quantization) in QUANT_CONFIGS.items()
            },
            # Force the optimizer to build index + quantized data so searches
            # actually exercise the quantized-over-turbo4 path.
            "optimizers_config": {
                "default_segment_number": 1,
                "indexing_threshold": 1,
            },
        }
    )
    assert response.ok

    rng = random.Random(4)
    points = _make_points(rng, QUANT_NUM_POINTS, list(QUANT_CONFIGS))

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': name},
        query_params={'wait': 'true'},
        body={"points": points}
    )
    assert response.ok

    _wait_collection_green(name)
    yield name
    drop_collection(collection_name=name)


def test_quantized_turbo4_config_persists(quant_collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': quant_collection_name},
    )
    assert response.ok

    vectors = response.json()['result']['config']['params']['vectors']
    for name, (_, quantization) in QUANT_CONFIGS.items():
        assert vectors[name]['datatype'] == "turbo4"
        quant_kind = next(iter(quantization))
        assert quant_kind in vectors[name]['quantization_config']


@pytest.mark.parametrize("vector_name", list(QUANT_CONFIGS))
@pytest.mark.parametrize("rescore", [True, False])
def test_quantized_turbo4_search(quant_collection_name, vector_name, rescore):
    # rescore=True re-scores candidates against the (dequantized) turbo4
    # storage after the quantized first pass.
    rng = random.Random(2024)
    query_vector = _random_vector(rng)

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': quant_collection_name},
        body={
            "vector": {"name": vector_name, "vector": query_vector},
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
    result = response.json()['result']
    assert len(result) == 10
    scores = [hit['score'] for hit in result]
    assert scores == sorted(scores, reverse=True)
