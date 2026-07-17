import hashlib
import math
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


###############################################################################
# Vector roundtrip & mutation.
#
# Vectors come back dequantized (and normalized for Cosine), so checks
# against the written input are cosine-similarity based, never exact.
# Re-reads of the same stored bytes, however, must match exactly — turbo4
# quantizes once at ingest, so any drift between reads means an extra
# dequantize→requantize pass slipped in. These tests run after the search
# tests above (pytest definition order) and mutate disjoint point ids, so the
# shared collection stays valid for everything else.
###############################################################################


def _cosine_sim(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    return dot / (norm_a * norm_b)


def _norm(vector):
    return sum(x * x for x in vector) ** 0.5


def _original_vectors(point_id):
    # The module setup fills the collection from a fixed seed, so the
    # pre-mutation vectors of any point can be regenerated exactly.
    points = _make_points(random.Random(42), NUM_POINTS, ["image", "audio", "text"])
    return points[point_id - 1]["vector"]


def _retrieve_point(collection_name, point_id):
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': point_id},
    )
    assert response.ok
    return response.json()['result']


def test_turbo4_retrieve_with_vector(collection_name):
    point = _retrieve_point(collection_name, 10)
    original = _original_vectors(10)
    for name in ("image", "audio", "text"):
        retrieved = point['vector'][name]
        assert len(retrieved) == VECTOR_SIZE
        # Dequantized f32 values — raw 4-bit codes would all be integers.
        assert any(v != int(v) for v in retrieved)
        # Rotated back into the input space: still near-parallel to what was
        # written; without the inverse rotation the direction would be random.
        assert _cosine_sim(retrieved, original[name]) > 0.9

    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={"limit": 3, "with_vector": True},
    )
    assert response.ok
    points = response.json()['result']['points']
    assert len(points) == 3
    # Scroll reads vectors through the batched storage path (the single-point
    # GET above reads one by one), so it gets its own fidelity check.
    for point in points:
        original = _original_vectors(point['id'])
        for name in ("image", "audio", "text"):
            retrieved = point['vector'][name]
            assert len(retrieved) == VECTOR_SIZE
            assert _cosine_sim(retrieved, original[name]) > 0.9


def test_turbo4_overwrite_point(collection_name):
    old = _original_vectors(1)
    rng = random.Random(1001)
    new = {name: _random_vector(rng) for name in ("image", "audio", "text")}

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"points": [{"id": 1, "vector": new}]},
    )
    assert response.ok

    point = _retrieve_point(collection_name, 1)
    for name in ("image", "audio", "text"):
        retrieved = point['vector'][name]
        assert _cosine_sim(retrieved, new[name]) > 0.9
        assert _cosine_sim(retrieved, new[name]) > _cosine_sim(retrieved, old[name])
    # Cosine ("image") is normalized at write time, so magnitude is only
    # preserved for the Dot and Euclid vectors.
    for name in ("audio", "text"):
        assert 0.8 < _norm(point['vector'][name]) / _norm(new[name]) < 1.25


def test_turbo4_update_single_vector(collection_name):
    # Partial update through PUT /points/vectors: only the named vector is
    # replaced. The untouched vectors' storages see no write at all, so
    # their dequantized values must come back identical — near-identical
    # would mean they were re-quantized along the way.
    old = _original_vectors(2)
    before = _retrieve_point(collection_name, 2)['vector']
    new_audio = _random_vector(random.Random(2002))

    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"points": [{"id": 2, "vector": {"audio": new_audio}}]},
    )
    assert response.ok

    point = _retrieve_point(collection_name, 2)
    audio = point['vector']['audio']
    assert _cosine_sim(audio, new_audio) > 0.9
    assert _cosine_sim(audio, new_audio) > _cosine_sim(audio, old['audio'])
    assert 0.8 < _norm(audio) / _norm(new_audio) < 1.25
    for untouched in ("image", "text"):
        assert point['vector'][untouched] == pytest.approx(
            before[untouched], abs=1e-6)
        assert _cosine_sim(point['vector'][untouched], old[untouched]) > 0.9


def test_turbo4_delete_vectors_and_points(collection_name):
    # Deletion flags are turbo4-storage-owned state (each named storage
    # persists its own): deleting one named vector must hide the point from
    # searches on that name only, while the other storages keep serving it.
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"points": [3], "vector": ["audio"]},
    )
    assert response.ok

    point = _retrieve_point(collection_name, 3)
    assert 'audio' not in point['vector']
    original = _original_vectors(3)
    for name in ("image", "text"):
        assert _cosine_sim(point['vector'][name], original[name]) > 0.9

    response = request_with_validation(
        api='/collections/{collection_name}/points/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"points": [4]},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 4},
    )
    assert response.status_code == 404

    # Point 3 is gone from "audio" searches but still found via "image";
    # the deleted point 4 appears in neither.
    for vector_name, absent, present in (
        ("audio", [3, 4], []),
        ("image", [4], [3]),
    ):
        response = request_with_validation(
            api='/collections/{collection_name}/points/search',
            method="POST",
            path_params={'collection_name': collection_name},
            body={
                "vector": {"name": vector_name,
                           "vector": original[vector_name]},
                "limit": NUM_POINTS,
            },
        )
        assert response.ok
        ids = [hit['id'] for hit in response.json()['result']]
        for point_id in absent:
            assert point_id not in ids
        for point_id in present:
            assert point_id in ids


###############################################################################
# Multivector over turbo4 storage.
###############################################################################


def test_turbo4_multivector(on_disk_vectors, collection_name):
    name = f"{collection_name}_multivec"
    drop_collection(collection_name=name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': name},
        body={
            "vectors": {
                "multi": {
                    "size": VECTOR_SIZE,
                    "distance": "Cosine",
                    "on_disk": on_disk_vectors,
                    "datatype": "turbo4",
                    "multivector_config": {"comparator": "max_sim"},
                }
            },
        },
    )
    assert response.ok

    rng = random.Random(7)
    points = [
        {"id": i, "vector": {"multi": [_random_vector(rng) for _ in range(3)]}}
        for i in range(1, 21)
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
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': name},
    )
    assert response.ok
    config = response.json()['result']['config']['params']['vectors']['multi']
    assert config['datatype'] == "turbo4"
    assert config['multivector_config']['comparator'] == "max_sim"

    response = request_with_validation(
        api='/collections/{collection_name}/points/query',
        method="POST",
        path_params={'collection_name': name},
        body={
            "query": [_random_vector(rng) for _ in range(2)],
            "using": "multi",
            "limit": 10,
        },
    )
    assert response.ok
    hits = response.json()['result']['points']
    assert len(hits) == 10
    scores = [hit['score'] for hit in hits]
    assert scores == sorted(scores, reverse=True)

    multi = _retrieve_point(name, 1)['vector']['multi']
    assert len(multi) == 3
    for sub, original in zip(multi, points[0]["vector"]["multi"]):
        assert len(sub) == VECTOR_SIZE
        assert _cosine_sim(sub, original) > 0.9

    drop_collection(collection_name=name)


###############################################################################
# HNSW-indexed search over plain turbo4 (no secondary quantization).
#
# The shared collection above stays below the default indexing threshold, so
# its searches run unindexed; this section forces index builds. Graph
# construction scores stored vectors against each other in the quantized
# domain — a path no unindexed search exercises.
###############################################################################


def _wait_indexed_vectors(collection_name, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        response = request_with_validation(
            api='/collections/{collection_name}',
            method="GET",
            path_params={'collection_name': collection_name},
        )
        assert response.ok
        if response.json()['result']['indexed_vectors_count'] > 0:
            return
        time.sleep(0.2)
    raise Exception(f"Collection {collection_name} built no index in {timeout}s")


def test_turbo4_indexed_search(on_disk_vectors, collection_name):
    name = f"{collection_name}_indexed"
    drop_collection(collection_name=name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': name},
        body={
            # Unnamed vector also covers the default-vector path over turbo4.
            "vectors": {
                "size": VECTOR_SIZE,
                "distance": "Cosine",
                "on_disk": on_disk_vectors,
                "datatype": "turbo4",
            },
            "optimizers_config": {
                "default_segment_number": 1,
                "indexing_threshold": 1,
            },
        },
    )
    assert response.ok

    rng = random.Random(11)
    points = [{"id": i, "vector": _random_vector(rng)} for i in range(1, 101)]
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': name},
        query_params={'wait': 'true'},
        body={"points": points},
    )
    assert response.ok

    _wait_indexed_vectors(name)

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': name},
        body={"vector": _random_vector(random.Random(12)), "limit": 10},
    )
    assert response.ok
    result = response.json()['result']
    assert len(result) == 10
    scores = [hit['score'] for hit in result]
    assert scores == sorted(scores, reverse=True)

    # Recall through the graph: a stored point's own (pre-quantization)
    # vector must come back as the top hit, near-perfectly similar.
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': name},
        body={"vector": points[16]["vector"], "limit": 1},
    )
    assert response.ok
    top = response.json()['result'][0]
    assert top['id'] == 17
    assert top['score'] > 0.95

    drop_collection(collection_name=name)


def test_turbo4_optimizer_rebuild_preserves_vectors(on_disk_vectors, collection_name):
    # Segment rebuilds move turbo4 vectors as raw encoded bytes: quantization
    # happens once at ingest and never again. Forcing the optimizer over the
    # data afterwards must reproduce identical dequantized values — any drift
    # means a dequantize→requantize snuck into the rebuild (the roundtrip is
    # not idempotent, so vectors would degrade a little on every rebuild).
    name = f"{collection_name}_rebuild"
    drop_collection(collection_name=name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': name},
        body={
            "vectors": {
                "size": VECTOR_SIZE,
                "distance": "Dot",
                "on_disk": on_disk_vectors,
                "datatype": "turbo4",
            },
        },
    )
    assert response.ok

    rng = random.Random(61)
    points = [{"id": i, "vector": _random_vector(rng)} for i in range(1, 101)]
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': name},
        query_params={'wait': 'true'},
        body={"points": points},
    )
    assert response.ok

    sample_ids = [1, 42, 100]
    before = {pid: _retrieve_point(name, pid)['vector'] for pid in sample_ids}

    # Drop the indexing threshold so the optimizer rebuilds the segment (and
    # builds an HNSW index) from the already-quantized data.
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': name},
        body={
            "optimizers_config": {
                "default_segment_number": 1,
                "indexing_threshold": 1,
            },
        },
    )
    assert response.ok

    _wait_indexed_vectors(name)

    for pid in sample_ids:
        after = _retrieve_point(name, pid)['vector']
        assert after == pytest.approx(before[pid], abs=1e-6)

    # The rebuilt, indexed segment still finds the right point.
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': name},
        body={"vector": points[41]["vector"], "limit": 1},
    )
    assert response.ok
    assert response.json()['result'][0]['id'] == 42

    drop_collection(collection_name=name)


###############################################################################
# Manhattan distance over turbo4.
#
# The fourth supported distance; kept out of the shared three-distance
# collection to leave its layout (and every `_original_vectors` consumer)
# untouched. Like Euclid it is a true distance: scores order ascending.
###############################################################################


def test_turbo4_manhattan(on_disk_vectors, collection_name):
    name = f"{collection_name}_manhattan"
    drop_collection(collection_name=name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': name},
        body={
            "vectors": {
                "size": VECTOR_SIZE,
                "distance": "Manhattan",
                "on_disk": on_disk_vectors,
                "datatype": "turbo4",
            },
        },
    )
    assert response.ok

    rng = random.Random(71)
    points = [{"id": i, "vector": _random_vector(rng)} for i in range(1, 21)]
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': name},
        query_params={'wait': 'true'},
        body={"points": points},
    )
    assert response.ok

    retrieved = _retrieve_point(name, 5)['vector']
    assert _cosine_sim(retrieved, points[4]["vector"]) > 0.9
    assert 0.8 < _norm(retrieved) / _norm(points[4]["vector"]) < 1.25

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': name},
        body={"vector": points[4]["vector"], "limit": 20},
    )
    assert response.ok
    result = response.json()['result']
    assert len(result) == 20
    scores = [hit['score'] for hit in result]
    assert scores == sorted(scores)
    assert result[0]['id'] == 5

    drop_collection(collection_name=name)


###############################################################################
# Snapshot roundtrip: no vector degradation allowed.
#
# A snapshot must carry the turbo4 storages, the HNSW index and the
# secondary quantized data intact: snapshot, drop the collection, recover
# from the downloaded snapshot alone, and verify config, exact vector
# values, and search. The API mechanics themselves (checksums, non-wait,
# security) are covered by test_snapshot.py.
###############################################################################


def _snapshot_and_recover(collection_name, srv_dir, srv_url):
    # Snapshot the collection, download the file (verifying its checksum),
    # drop everything server-side, then recover from the download alone.
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
    )
    assert response.ok
    snapshot_name = response.json()['result']['name']
    snapshot_checksum = response.json()['result']['checksum']

    response = request_with_validation(
        api='/collections/{collection_name}/snapshots/{snapshot_name}',
        method="GET",
        path_params={'collection_name': collection_name,
                     'snapshot_name': snapshot_name},
    )
    assert response.ok
    assert hashlib.sha256(response.content).hexdigest() == snapshot_checksum
    with open(srv_dir / f"{collection_name}.tar", 'wb') as f:
        f.write(response.content)

    response = request_with_validation(
        api='/collections/{collection_name}/snapshots/{snapshot_name}',
        method="DELETE",
        path_params={'collection_name': collection_name,
                     'snapshot_name': snapshot_name},
        query_params={'wait': 'true'},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/snapshots/recover',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "location": f"{srv_url}/{collection_name}.tar",
            "checksum": snapshot_checksum,
        },
    )
    assert response.ok


def test_turbo4_snapshot_roundtrip(http_server, on_disk_vectors, collection_name):
    (srv_dir, srv_url) = http_server
    name = f"{collection_name}_snapshot"
    drop_collection(collection_name=name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': name},
        body={
            "vectors": {
                # One vector keeps a secondary quantization so the snapshot
                # carries quantized data next to the turbo4 storages.
                "image": {
                    "size": VECTOR_SIZE,
                    "distance": "Cosine",
                    "on_disk": on_disk_vectors,
                    "datatype": "turbo4",
                    "quantization_config": {
                        "turbo": {"bits": "bits2", "always_ram": True},
                    },
                },
                "audio": {
                    "size": VECTOR_SIZE,
                    "distance": "Dot",
                    "on_disk": on_disk_vectors,
                    "datatype": "turbo4",
                },
                "text": {
                    "size": VECTOR_SIZE,
                    "distance": "Euclid",
                    "on_disk": on_disk_vectors,
                    "datatype": "turbo4",
                },
            },
            # Index so the snapshot carries the HNSW graphs too.
            "optimizers_config": {
                "default_segment_number": 1,
                "indexing_threshold": 1,
            },
        },
    )
    assert response.ok

    points = _make_points(random.Random(31), NUM_POINTS, ["image", "audio", "text"])
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': name},
        query_params={'wait': 'true'},
        body={"points": points},
    )
    assert response.ok

    _wait_indexed_vectors(name)

    vectors_before = _retrieve_point(name, 7)['vector']

    _snapshot_and_recover(name, srv_dir, srv_url)

    # config survived
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': name},
    )
    assert response.ok
    vectors = response.json()['result']['config']['params']['vectors']
    for vector_name in ("image", "audio", "text"):
        assert vectors[vector_name]['datatype'] == "turbo4"
    assert "turbo" in vectors["image"]['quantization_config']

    # the index survived (restored, not rebuilt from scratch: the count is
    # there as soon as the collection loads)
    _wait_indexed_vectors(name)

    # all points survived
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': name},
        body={"limit": NUM_POINTS + 1},
    )
    assert response.ok
    assert len(response.json()['result']['points']) == NUM_POINTS

    # The stored turbo4 vectors survived intact: recovery must reproduce the
    # same dequantized values to float precision. The epsilon only absorbs
    # serialization noise — a lossy re-encode would move values by whole
    # quantization steps, orders of magnitude past it.
    point = _retrieve_point(name, 7)
    original = points[6]["vector"]
    for vector_name in ("image", "audio", "text"):
        retrieved = point['vector'][vector_name]
        assert retrieved == pytest.approx(vectors_before[vector_name], abs=1e-6)
        assert len(retrieved) == VECTOR_SIZE
        assert any(v != int(v) for v in retrieved)
        assert _cosine_sim(retrieved, original[vector_name]) > 0.9

    # and quantized search over the recovered index still works
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': name},
        body={
            "vector": {"name": "image", "vector": _random_vector(random.Random(32))},
            "params": {
                "quantization": {
                    "ignore": False,
                    "rescore": True,
                    "oversampling": 2.0,
                }
            },
            "limit": 10,
        },
    )
    assert response.ok
    result = response.json()['result']
    assert len(result) == 10
    scores = [hit['score'] for hit in result]
    assert scores == sorted(scores, reverse=True)

    drop_collection(collection_name=name)


###############################################################################
# Datatype boundary semantics.
#
# The uint8 datatype has targeted tests for its conversion semantics
# (truncation and saturation, test_multi_vector_uint8.py). The turbo4
# analogue is per-vector scaling and rotation: degenerate and extreme
# inputs must quantize without NaN/Inf, and dimensions that are not a
# power of two make the rotation decompose into power-of-two blocks and
# leave a padding tail in the packed codes that the API must hide.
###############################################################################


def _assert_finite(vector):
    # NaN/Inf serialize to JSON null, so a non-number element is the first
    # sign of a poisoned value.
    for v in vector:
        assert isinstance(v, (int, float))
        assert math.isfinite(v)


def test_turbo4_degenerate_vectors(on_disk_vectors, collection_name):
    # Zero and constant vectors collapse the per-vector scale. Dot distance
    # keeps raw values end to end: a zero vector is stored with length 0
    # and must come back as exact zeros, not NaN from a divide-by-range.
    name = f"{collection_name}_degenerate"
    drop_collection(collection_name=name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': name},
        body={
            "vectors": {
                "size": VECTOR_SIZE,
                "distance": "Dot",
                "on_disk": on_disk_vectors,
                "datatype": "turbo4",
            },
        },
    )
    assert response.ok

    constant = [0.5] * VECTOR_SIZE
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': name},
        query_params={'wait': 'true'},
        body={"points": [
            {"id": 1, "vector": [0.0] * VECTOR_SIZE},
            {"id": 2, "vector": constant},
        ]},
    )
    assert response.ok

    zero = _retrieve_point(name, 1)['vector']
    assert len(zero) == VECTOR_SIZE
    _assert_finite(zero)
    assert zero == pytest.approx([0.0] * VECTOR_SIZE, abs=1e-6)

    const_back = _retrieve_point(name, 2)['vector']
    assert len(const_back) == VECTOR_SIZE
    _assert_finite(const_back)
    assert _cosine_sim(const_back, constant) > 0.9
    assert 0.8 < _norm(const_back) / _norm(constant) < 1.25

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': name},
        body={"vector": _random_vector(random.Random(51)), "limit": 2},
    )
    assert response.ok
    result = response.json()['result']
    assert len(result) == 2
    for hit in result:
        assert math.isfinite(hit['score'])

    drop_collection(collection_name=name)


def test_turbo4_extreme_magnitudes(on_disk_vectors, collection_name):
    # The per-vector scale must absorb any input magnitude: huge, tiny and
    # outlier-dominated vectors have to come back finite and direction-true.
    name = f"{collection_name}_extreme"
    drop_collection(collection_name=name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': name},
        body={
            "vectors": {
                "size": VECTOR_SIZE,
                "distance": "Dot",
                "on_disk": on_disk_vectors,
                "datatype": "turbo4",
            },
        },
    )
    assert response.ok

    rng = random.Random(52)
    vectors = {
        1: [x * 1e30 for x in _random_vector(rng)],
        2: [x * 1e-30 for x in _random_vector(rng)],
        3: [1e6] + _random_vector(rng, VECTOR_SIZE - 1),
    }
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': name},
        query_params={'wait': 'true'},
        body={"points": [
            {"id": point_id, "vector": vector}
            for point_id, vector in vectors.items()
        ]},
    )
    assert response.ok

    for point_id, original in vectors.items():
        retrieved = _retrieve_point(name, point_id)['vector']
        assert len(retrieved) == VECTOR_SIZE
        _assert_finite(retrieved)
        assert _cosine_sim(retrieved, original) > 0.9
        # Direction alone is scale-invariant — the magnitude must survive too.
        assert 0.8 < _norm(retrieved) / _norm(original) < 1.25

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': name},
        body={"vector": _random_vector(random.Random(53)), "limit": 3},
    )
    assert response.ok
    result = response.json()['result']
    assert len(result) == 3
    for hit in result:
        assert math.isfinite(hit['score'])

    drop_collection(collection_name=name)


def test_turbo4_odd_dimension(on_disk_vectors, collection_name):
    # A size that is not a power of two; 63 decomposes into the maximum
    # number of rotation blocks (32+16+8+4+2+1). The quantizer unit tests
    # sweep many sizes — at the API level one representative checks that
    # the storage still returns exactly `size` values, padding hidden.
    size = 63
    name = f"{collection_name}_dim_{size}"
    drop_collection(collection_name=name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': name},
        body={
            "vectors": {
                "size": size,
                "distance": "Cosine",
                "on_disk": on_disk_vectors,
                "datatype": "turbo4",
            },
        },
    )
    assert response.ok

    rng = random.Random(54)
    points = [{"id": i, "vector": _random_vector(rng, size)} for i in range(1, 11)]
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': name},
        query_params={'wait': 'true'},
        body={"points": points},
    )
    assert response.ok

    retrieved = _retrieve_point(name, 3)['vector']
    assert len(retrieved) == size
    assert any(v != int(v) for v in retrieved)
    assert _cosine_sim(retrieved, points[2]["vector"]) > 0.9

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': name},
        body={"vector": _random_vector(random.Random(55), size), "limit": 5},
    )
    assert response.ok
    result = response.json()['result']
    assert len(result) == 5
    scores = [hit['score'] for hit in result]
    assert scores == sorted(scores, reverse=True)

    drop_collection(collection_name=name)
