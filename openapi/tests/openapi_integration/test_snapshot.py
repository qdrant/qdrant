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


def test_snapshot_operations():
    # no snapshot on collection
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']) == 0

    # create snapshot on collection
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
    )
    assert response.ok
    snapshot_name = response.json()['result']['name']

    # validate it exists
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']) == 1
    assert response.json()['result'][0]['name'] == snapshot_name

    # delete it
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots/{snapshot_name}',
        method="DELETE",
        path_params={'collection_name': collection_name,
                     'snapshot_name': snapshot_name},
        query_params={'wait': 'true'},
    )
    assert response.ok

    # validate it is gone
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']) == 0

    # no full snapshot
    response = request_with_validation(
        api='/snapshots',
        method="GET",
    )
    assert response.ok
    assert len(response.json()['result']) == 0

    # create full snapshot
    response = request_with_validation(
        api='/snapshots',
        method="POST",
    )
    assert response.ok
    snapshot_name = response.json()['result']['name']

    # validate it exists
    response = request_with_validation(
        api='/snapshots',
        method="GET",
    )
    assert response.ok
    assert len(response.json()['result']) == 1
    assert response.json()['result'][0]['name'] == snapshot_name

    # delete it
    response = request_with_validation(
        api='/snapshots/{snapshot_name}',
        path_params={'snapshot_name': snapshot_name},
        method="DELETE",
        query_params={'wait': 'true'},
    )
    assert response.ok

    response = request_with_validation(
        api='/snapshots',
        method="GET",
    )
    assert response.ok
    assert len(response.json()['result']) == 0


@pytest.mark.timeout(30)
def test_snapshot_operations_non_wait():
    # there no snapshot on collection
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']) == 0

    # create snapshot on collection
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'false'},
    )
    assert response.status_code == 202

    # validate it exists
    snapshot_name = None
    while True:
        try:
            response = request_with_validation(
                api='/collections/{collection_name}/snapshots',
                method="GET",
                path_params={'collection_name': collection_name},
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
        api='/collections/{collection_name}/snapshots/{snapshot_name}',
        method="DELETE",
        path_params={'collection_name': collection_name,
                     'snapshot_name': snapshot_name},
        query_params={'wait': 'false'},
    )
    assert response.status_code == 202

    # validate it is gone
    while True:
        try:
            response = request_with_validation(
                api='/collections/{collection_name}/snapshots',
                method="GET",
                path_params={'collection_name': collection_name},
            )
            assert response.ok
            assert len(response.json()['result']) == 0
            break
        except AssertionError:
            # wait for snapshot to be deleted
            sleep(0.1)
            continue

    # no full snapshot
    response = request_with_validation(
        api='/snapshots',
        method="GET",
    )
    assert response.ok
    assert len(response.json()['result']) == 0

    # create full snapshot
    response = request_with_validation(
        api='/snapshots',
        method="POST",
        query_params={'wait': 'false'},
    )
    assert response.status_code == 202

    # validate it exists
    while True:
        try:
            response = request_with_validation(
                api='/snapshots',
                method="GET",
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
        api='/snapshots/{snapshot_name}',
        path_params={'snapshot_name': snapshot_name},
        method="DELETE",
        query_params={'wait': 'false'},
    )
    assert response.status_code == 202

    while True:
        try:
            response = request_with_validation(
                api='/snapshots',
                method="GET",
            )
            assert response.ok
            assert len(response.json()['result']) == 0
            break
        except AssertionError:
            # wait for snapshot to be deleted
            sleep(0.1)
            continue


def test_snapshot_invalid_file_uri():
    # Invalid file:// host
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots/recover',
        method="PUT",
        path_params={'collection_name': "somethingthatdoesnotexist"},
        body={
            "location": "file://whatever.snapshot",
        }
    )
    assert response.status_code == 400
    assert response.json()["status"]["error"] == "Bad request: Invalid snapshot URI, file path must be absolute or on localhost"

    # Absolute path that does not exist
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots/recover',
        method="PUT",
        path_params={'collection_name': "somethingthatdoesnotexist"},
        body={
            "location": "file:///whatever.snapshot",
        }
    )
    assert response.status_code == 400
    assert response.json()["status"]["error"] == "Bad request: Snapshot file \"/whatever.snapshot\" does not exist"

    # Path that does not exist
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots/recover',
        method="PUT",
        path_params={'collection_name': "somethingthatdoesnotexist"},
        body={
            "location": "file://localhost/whatever.snapshot",
        }
    )
    assert response.status_code == 400
    assert response.json()["status"]["error"] == "Bad request: Snapshot file \"/whatever.snapshot\" does not exist"
