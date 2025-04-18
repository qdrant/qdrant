import pytest
from datetime import datetime

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_metrics():
    response = request_with_validation(
        api='/metrics',
        method="GET",
    )
    assert response.ok

    # Probe some strings that must exist in the metrics output
    assert '# HELP app_info information about qdrant server' in response.text
    assert '# TYPE app_info gauge' in response.text
    assert 'app_info{name="qdrant",version="' in response.text
    assert 'collections_total ' in response.text


def test_telemetry():
    response = request_with_validation(
        api='/telemetry',
        method="GET",
    )

    assert response.ok

    result = response.json()['result']

    assert result['collections']['number_of_collections'] >= 1

    endpoint = result['requests']['rest']['responses']['PUT /collections/{name}/points']
    assert endpoint['200']['count'] > 0

    assert 'avg_duration_micros' in endpoint['200']


@pytest.mark.parametrize("level", [0, 1, 2, 3, 10])
def test_telemetry_detail(level: int):
    response = request_with_validation(
        api='/telemetry',
        method="GET",
        query_params={'details_level': level},
    )

    assert response.ok

    result = response.json()['result']

    assert result['collections']['number_of_collections'] >= 1

    endpoint = result['requests']['rest']['responses']['PUT /collections/{name}/points']
    assert endpoint['200']['count'] > 0
    
    if level == 0:
        'collections' not in result['collections']
        return

    last_queried = endpoint['200']['last_responded']
    last_queried = datetime.fromisoformat(last_queried)
    # Assert today
    assert last_queried.date() == datetime.now().date()

    collection = result['collections']['collections'][0]

    if level == 1:
        assert list(collection.keys()) == ['vectors', 'optimizers_status', 'params']
    elif level == 2:
        assert list(collection.keys()) == ['id', 'init_time_ms', 'config']
    elif level >= 3:
        assert list(collection.keys()) == ['id', 'init_time_ms', 'config', 'shards', 'transfers', 'resharding']

    if level >= 3:
        shard = collection['shards'][0]
        assert list(shard.keys()) == ['id', 'key', 'local', 'remote', 'replicate_states']

        local_shard = shard['local']

        if level == 3:
            assert list(local_shard.keys()) == [
                'variant_name', 'status', 'total_optimized_points', 'vectors_size_bytes',
                'payloads_size_bytes', 'num_points', 'num_vectors', 'optimizations', 'async_scorer'
            ]
        elif level > 3:
            assert list(local_shard.keys()) == [
                'variant_name', 'status', 'total_optimized_points', 'vectors_size_bytes',
                'payloads_size_bytes', 'num_points', 'num_vectors', 'segments', 'optimizations', 'async_scorer'
            ]

    if level >= 4:
        segment = local_shard['segments'][0]
        assert list(segment.keys()) == ['info', 'config', 'vector_index_searches', 'payload_field_indices']
