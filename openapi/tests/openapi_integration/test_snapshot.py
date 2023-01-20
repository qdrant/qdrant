import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import basic_collection_setup, drop_collection

collection_name = 'test_collection_snapshot'


@pytest.fixture(autouse=True)
def setup():
    basic_collection_setup(collection_name=collection_name)
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
        path_params={'collection_name': collection_name, 'snapshot_name': snapshot_name},
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
    )
    assert response.ok

    response = request_with_validation(
        api='/snapshots',
        method="GET",
    )
    assert response.ok
    assert len(response.json()['result']) == 0


