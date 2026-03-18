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


def test_metrics_default_no_per_collection(collection_name):
    """By default (per_collection not requested), request metrics must NOT have a collection label."""
    # Make a request that hits a whitelisted endpoint so metrics are populated
    request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={"limit": 1},
    )

    response = request_with_validation(
        api='/metrics',
        method="GET",
    )
    assert response.ok

    # REST request metrics should exist (global mode)
    assert 'rest_responses_total' in response.text

    # Existing collection-level metrics should use "id" label, not "collection"
    assert f'collection_points{{id="{collection_name}"}}' in response.text \
        or f'id="{collection_name}"' in response.text

    # Per-collection request metrics (collection= label on rest_/grpc_ metrics) should NOT appear
    for line in response.text.splitlines():
        if line.startswith('#'):
            continue
        if 'rest_responses_' in line or 'grpc_responses_' in line:
            assert 'collection=' not in line, (
                f"Per-collection label found in default mode: {line}"
            )


def test_metrics_with_per_collection(collection_name):
    """When per_collection=true is requested, request metrics must have a collection label."""
    # Make a request that hits a whitelisted endpoint so metrics are populated
    request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={"limit": 1},
    )

    response = request_with_validation(
        api='/metrics',
        method="GET",
        query_params={'per_collection': 'true'},
    )
    assert response.ok

    # Per-collection mode: collection label should appear
    found_per_collection = False
    for line in response.text.splitlines():
        if line.startswith('#'):
            continue
        if 'rest_responses_' in line and f'collection="{collection_name}"' in line:
            found_per_collection = True
            break

    assert found_per_collection, "Per-collection label not found when per_collection=true"


def test_telemetry():
    response = request_with_validation(
        api='/telemetry',
        method="GET",
    )

    assert response.ok

    result = response.json()['result']

    assert result['collections']['number_of_collections'] >= 1

    endpoint = result['requests']['rest']['responses']['PUT /collections/{collection_name}/points']
    assert endpoint['200']['count'] > 0

    assert 'avg_duration_micros' in endpoint['200']

    # By default, per_collection_responses should be absent (per_collection not requested)
    assert 'per_collection_responses' not in result['requests']['rest'], \
        "per_collection_responses should not appear when per_collection is not requested"
    assert 'per_collection_responses' not in result['requests']['grpc'], \
        "per_collection_responses should not appear when per_collection is not requested"


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

    endpoint = result['requests']['rest']['responses']['PUT /collections/{collection_name}/points']
    assert endpoint['200']['count'] > 0

    if level == 0:
        assert set(result.keys()) == {'id', 'app', 'collections', 'cluster', 'requests'}
        assert set(result['collections'].keys()) == {'number_of_collections'}
        return
    else:
        assert set(result.keys()) == {'id', 'app', 'collections', 'cluster', 'requests', 'memory', 'hardware'}
        assert set(result['collections'].keys()) == {'number_of_collections', 'collections', 'snapshots'}

    last_queried = endpoint['200']['last_responded'].replace('Z', '+00:00')
    last_queried = datetime.fromisoformat(last_queried)
    # Assert today
    assert last_queried.date() == datetime.now().date()
    
    collection = result['collections']['collections'][0]

    if level == 1:
        assert set(collection.keys()) == {'vectors', 'optimizers_status', 'params'}
    elif level == 2:
        assert set(collection.keys()) == {'id', 'init_time_ms', 'config'}
    elif level >= 3:
        assert set(collection.keys()) == {'id', 'init_time_ms', 'config', 'shards', 'transfers', 'resharding'}

        shard = collection['shards'][0]
        assert set(shard.keys()) == {'id', 'key', 'local', 'remote', 'replicate_states', 'partial_snapshot'}

        local_shard = shard['local']

        if level == 3:
            assert set(local_shard.keys()) == {
                'variant_name', 'status', 'total_optimized_points', 'vectors_size_bytes',
                'payloads_size_bytes', 'num_points', 'num_vectors', 'num_vectors_by_name', 'optimizations', 'async_scorer',
                'update_queue'
            }
        elif level >= 4:
            assert set(local_shard.keys()) == {
                'variant_name', 'status', 'total_optimized_points', 'vectors_size_bytes',
                'payloads_size_bytes', 'num_points', 'num_vectors', 'num_vectors_by_name', 'segments', 'optimizations',
                'async_scorer', 'indexed_only_excluded_vectors', 'update_queue'
            }

            segment = local_shard['segments'][0]
            assert set(segment.keys()) == {'info', 'config', 'vector_index_searches', 'payload_field_indices'}


def test_issues():
    response = request_with_validation(
        api='/issues',
        method="GET",
    )
    assert response.ok
    result = response.json()['result']['issues']
    assert len(result) == 0

    response = request_with_validation(
        api='/issues',
        method="DELETE",
    )
    assert response.ok
    result = response.json()['result']
    assert result
