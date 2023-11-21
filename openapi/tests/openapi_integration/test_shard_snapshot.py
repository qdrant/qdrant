from time import sleep
import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection_snapshot'


@pytest.fixture(autouse=True)
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_shard_snapshot_operations():
    # no snapshot on collection
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots',
        method="GET",
        path_params={'shard_id': 0, 'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']) == 0

    # create snapshot on collection
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots',
        method="POST",
        path_params={'shard_id': 0, 'collection_name': collection_name},
        query_params={'wait': 'true'},
    )
    assert response.ok
    snapshot_name = response.json()['result']['name']

    # validate it exists
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots',
        method="GET",
        path_params={'shard_id': 0, 'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']) == 1
    assert response.json()['result'][0]['name'] == snapshot_name

    # delete it
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}',
        method="DELETE",
        path_params={'shard_id': 0, 'collection_name': collection_name,
                     'snapshot_name': snapshot_name},
        query_params={'wait': 'true'},
    )
    assert response.ok

    # validate it is gone
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots',
        method="GET",
        path_params={'shard_id': 0, 'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']) == 0


@pytest.mark.timeout(20)
def test_shard_snapshot_operations_non_wait():
    # there no snapshot on collection
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots',
        method="GET",
        path_params={'shard_id': 0, 'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']) == 0

    # create snapshot on collection
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots',
        method="POST",
        path_params={'shard_id': 0, 'collection_name': collection_name},
        query_params={'wait': 'false'},
    )
    assert response.status_code == 202

    # validate it exists
    snapshot_name = None
    while True:
        try:
            response = request_with_validation(
                api='/collections/{collection_name}/shards/{shard_id}/snapshots',
                method="GET",
                path_params={'shard_id': 0, 'collection_name': collection_name},
            )
            assert response.ok
            assert len(response.json()['result']) == 1
            snapshot_name = response.json()['result'][0]['name']
            break
        except AssertionError:
            # wait for snapshot to be created
            sleep(0.1)
            continue

    # delete it
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}',
        method="DELETE",
        path_params={'shard_id': 0, 'collection_name': collection_name,
                     'snapshot_name': snapshot_name},
        query_params={'wait': 'false'},
    )
    assert response.status_code == 202

    # validate it is gone
    while True:
        try:
            response = request_with_validation(
                api='/collections/{collection_name}/shards/{shard_id}/snapshots',
                method="GET",
                path_params={'shard_id': 0, 'collection_name': collection_name},
            )
            assert response.ok
            assert len(response.json()['result']) == 0
            break
        except AssertionError:
            # wait for snapshot to be deleted
            sleep(0.1)
            continue


def test_shard_snapshot_recovery_errors():

    # Invalid collection name
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/recover',
        method="PUT",
        path_params={'shard_id': 0, 'collection_name': "somethingthatdoesnotexist"},
        body={
            "location": "whatever",
        }
    )
    assert response.status_code == 404

    # Invalid file url
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/recover',
        method="PUT",
        path_params={'shard_id': 0, 'collection_name': collection_name},
        body={
            "location": "file:///whatever.snapshot",
        }
    )
    assert response.status_code == 400

    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/recover',
        method="PUT",
        path_params={'shard_id': 0, 'collection_name': collection_name},
        body={
            "location": "http://localhost:6333/snapshots/whatever.snapshot",
        }
    )
    assert response.status_code == 400
