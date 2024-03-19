from time import sleep
import hashlib
import os
import pytest
import requests

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

QDRANT_HOST = os.environ.get("QDRANT_HOST", "localhost:6333")

collection_name = 'test_collection_snapshot'


@pytest.fixture(autouse=True)
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_snapshots(collection_name)
    drop_collection(collection_name=collection_name)


def drop_snapshots(collection_name: str) -> None:
    """Delete all snapshots on the collection."""
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    for snapshot in response.json()['result']:
        response = request_with_validation(
            api='/collections/{collection_name}/snapshots/{snapshot_name}',
            method="DELETE",
            path_params={'collection_name': collection_name,
                         'snapshot_name': snapshot['name']},
            query_params={'wait': 'true'},
        )
        assert response.ok


def test_collection_snapshot_operations(http_server):
    (srv_dir, srv_url) = http_server

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
    snapshot_checksum = response.json()['result']['checksum']

    # validate it exists
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']) == 1
    assert response.json()['result'][0]['name'] == snapshot_name
    assert response.json()['result'][0]['checksum'] == snapshot_checksum

    # download it, save, and validate checksum
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots/{snapshot_name}',
        method="GET",
        path_params={'collection_name': collection_name, 'snapshot_name': snapshot_name},
    )
    assert response.ok
    with open(srv_dir / "snapshot.tar", 'wb') as f:
        f.write(response.content)
    assert snapshot_checksum == hashlib.sha256(response.content).hexdigest()

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

    # delete collection
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    # validate that the collection is deleted
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={},
    )
    assert response.status_code == 404

    # try to recover collection from snapshot with wrong checksum
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots/recover',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "location": f"{srv_url}/snapshot.tar",
            "checksum": "3" * len(snapshot_checksum),
            "wait": "true",
        },
    )
    assert response.status_code == 400

    # validate that the collection is not recovered
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={},
    )
    assert response.status_code == 404

    # recover collection from snapshot with correct checksum
    response = request_with_validation(
        api='/collections/{collection_name}/snapshots/recover',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "location": f"{srv_url}/snapshot.tar",
            "checksum": snapshot_checksum,
            "wait": "true",
        },
    )
    assert response.ok

    # validate that the collection is recovered
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={},
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 10


def test_full_snapshot_operations():
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

    # do full snapshot
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


def test_security():
    # ensure we cannot do simple arbitrary path traversal
    name = "/etc/passwd"
    response = requests.get(
        f"http://{QDRANT_HOST}/snapshots/{name}",
        headers={"Content-Type": "application/json"},
    )
    assert not response.ok
    assert response.status_code == 404

    name = "../../../../../../../etc/passwd"
    response = requests.get(
        f"http://{QDRANT_HOST}/snapshots/{name}",
        headers={"Content-Type": "application/json"},
    )
    assert not response.ok
    assert response.status_code == 404

    # ensure we cannot do arbitrary path traversal with encoded slashes
    response = request_with_validation(
        api='/snapshots/{snapshot_name}',
        path_params={'snapshot_name': '..%2F..%2F..%2F..%2F..%2F..%2F..%2F..%2F..%2Fetc%2Fpasswd'},
        method="GET",
    )
    assert not response.ok
    assert response.status_code == 404
