from time import sleep
import hashlib
import pytest
import requests

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import qdrant_host_headers, request_with_validation
from .helpers.settings import QDRANT_HOST


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors, wal_capacity=1)
    yield
    drop_collection(collection_name=collection_name)


def assert_malformed_checksum_rejected_before_upload(endpoint):
    response = requests.post(
        f"{QDRANT_HOST}{endpoint}",
        params={'checksum': 'abc'},
        files={'snapshot': ('snapshot.tar', b'not a snapshot', 'application/octet-stream')},
        headers=qdrant_host_headers(),
    )
    assert response.status_code == 422
    assert 'invalid_sha256_hash' in response.text


def test_upload_shard_snapshot_rejects_malformed_checksum(collection_name):
    assert_malformed_checksum_rejected_before_upload(
        f"/collections/{collection_name}/shards/0/snapshots/upload",
    )


def test_recover_partial_snapshot_rejects_malformed_checksum(collection_name):
    assert_malformed_checksum_rejected_before_upload(
        f"/collections/{collection_name}/shards/0/snapshot/partial/recover",
    )


def test_delete_vector_advances_partial_snapshot_manifest(collection_name):
    # Pin the vector storage tracker to a known version with a vector update.
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"points": [{"id": 1, "vector": [1.0, 2.0, 3.0, 4.0]}]},
    )
    assert response.ok

    # Delete the default named vector of another point; this mutates the
    # vector storage's deleted-flags file.
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"points": [1], "vector": [""]},
    )
    assert response.ok
    delete_op = response.json()['result']['operation_id']

    # Sanity: the vector is gone locally.
    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 1},
    )
    assert response.ok
    assert '' not in (response.json()['result']['vector'] or {})

    # The manifest must stamp the mutated vector storage files with the delete
    # op version; a stale version makes partial snapshot recovery skip the
    # changed deleted-flags file and resurrect the vector on the replica.
    response = requests.get(
        f"{QDRANT_HOST}/collections/{collection_name}/shards/0/snapshot/partial/manifest",
        headers=qdrant_host_headers(),
    )
    assert response.ok
    vector_file_versions = [
        version
        for segment in response.json()['result'].values()
        for path, version in segment['file_versions'].items()
        if path.startswith('vector_storage/') and version is not None
    ]
    assert max(vector_file_versions) >= delete_op


def test_shard_snapshot_operations(http_server, collection_name):
    (srv_dir, srv_url) = http_server

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
    snapshot_checksum = response.json()['result']['checksum']

    # validate it exists
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots',
        method="GET",
        path_params={'shard_id': 0, 'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']) == 1
    assert response.json()['result'][0]['name'] == snapshot_name
    assert response.json()['result'][0]['checksum'] == snapshot_checksum

    # download it, save, and validate checksum
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}',
        method="GET",
        path_params={'shard_id': 0, 'collection_name': collection_name,
                     'snapshot_name': snapshot_name},
    )
    assert response.ok
    with open(srv_dir / "snapshot.tar", 'wb') as f:
        f.write(response.content)
    assert snapshot_checksum == hashlib.sha256(response.content).hexdigest()

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

    # try to recover shard from snapshot with wrong checksum
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/recover',
        method="PUT",
        path_params={'shard_id': 0, 'collection_name': collection_name},
        body={
            "location": f"{srv_url}/snapshot.tar",
            "checksum": "3" * len(snapshot_checksum),
            "wait": "true",
        },
    )
    assert response.status_code == 400

    # recover shard from snapshot with correct checksum
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/recover',
        method="PUT",
        path_params={'shard_id': 0, 'collection_name': collection_name},
        body={
            "location": f"{srv_url}/snapshot.tar",
            "checksum": snapshot_checksum,
            "wait": "true",
        },
    )
    assert response.ok


@pytest.mark.timeout(20)
def test_shard_snapshot_operations_non_wait(collection_name):
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


def test_shard_snapshot_recovery_errors(collection_name):

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


def test_shard_snapshot_security(collection_name):
    # ensure we cannot do simple arbitrary path traversal
    snapshot_name = "/etc/passwd"
    response = requests.get(
        f"{QDRANT_HOST}/collections/{collection_name}/shards/0/snapshots/{snapshot_name}",
        headers={**qdrant_host_headers(), "Content-Type": "application/json"},
    )
    assert not response.ok
    assert response.status_code == 404

    snapshot_name = "../../../../../../../etc/passwd"
    response = requests.get(
        f"{QDRANT_HOST}/collections/{collection_name}/shards/0/snapshots/{snapshot_name}",
        headers={**qdrant_host_headers(), "Content-Type": "application/json"},
    )
    assert not response.ok
    assert response.status_code == 404

    # ensure we cannot do arbitrary path traversal with encoded slashes
    response = request_with_validation(
        api='/collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}',
        path_params={
            'collection_name': collection_name,
            'shard_id': 0,
            'snapshot_name': '..%2F..%2F..%2F..%2F..%2F..%2F..%2F..%2F..%2Fetc%2Fpasswd',
        },
        method="GET",
    )
    assert not response.ok
    assert response.status_code == 404
