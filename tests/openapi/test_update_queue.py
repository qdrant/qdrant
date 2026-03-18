import pytest
import requests

from openapi.helpers.helpers import (
    skip_if_no_feature,
    get_api_string,
    request_with_validation,
)
from openapi.helpers.settings import QDRANT_HOST
from openapi.helpers.collection_setup import drop_collection


@pytest.fixture(autouse=True)
def setup(collection_name):
    drop_collection(collection_name)
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 4,
                "distance": "Dot",
            },
        }
    )
    assert response.ok
    yield
    drop_collection(collection_name=collection_name)


def get_queue_info(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    return response.json()["result"]["update_queue"]

def get_queue_shard_telemetry(collection_name):
    response = request_with_validation(
        api='/telemetry',
        method="GET",
        query_params={'details_level': 10},
    )
    assert response.ok

    collections = response.json()['result']['collections']['collections']
    collection = next(
        (c for c in collections if c["id"] == collection_name),
        None,
    )
    assert collection is not None, f"Collection {collection_name} not found"

    shards = collection['shards']
    assert len(shards) == 1, (
        f"Expected exactly one shard for collection {collection_name}, "
        f"found {len(shards)}"
    )

    local_shard = shards[0]['local']
    return local_shard["update_queue"]


def test_queue_op_num(collection_name):
    queue_info = get_queue_info(collection_name)
    queue_telemetry = get_queue_shard_telemetry(collection_name)

    # empty collection
    assert queue_info["length"] == 0
    assert queue_telemetry["op_num"] == 0

    # apply first update
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": [0.05, 0.61, 0.76, 0.74],
                    "payload": {}
                }
            ]}
    )
    assert response.ok

    queue_info = get_queue_info(collection_name)
    queue_telemetry = get_queue_shard_telemetry(collection_name)

    # wait=true so ack. after application
    assert queue_info["length"] == 0
    assert queue_telemetry["op_num"] == 1

    # apply second update
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": [0.05, 0.61, 0.76, 0.74],
                    "payload": {}
                }
            ]}
    )
    assert response.ok

    queue_info = get_queue_info(collection_name)
    queue_telemetry = get_queue_shard_telemetry(collection_name)
    # wait=true so ack. after application
    assert queue_info["length"] == 0
    assert queue_telemetry["op_num"] == 2


def test_queue_length(collection_name):
    skip_if_no_feature("staging")

    queue_info = get_queue_info(collection_name)
    queue_telemetry = get_queue_shard_telemetry(collection_name)

    # empty collection
    assert queue_info["length"] == 0
    assert queue_telemetry["op_num"] == 0

    # apply update delay
    response = requests.post(
        url=get_api_string(QDRANT_HOST, '/collections/{collection_name}/debug', {"collection_name": collection_name}),
        json={"delay": {"duration_sec": 3.0}}
    )
    assert response.ok

    # apply first update
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'false'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": [0.05, 0.61, 0.76, 0.74],
                    "payload": {}
                }
            ]}
    )
    assert response.ok

    queue_info = get_queue_info(collection_name)
    queue_telemetry = get_queue_shard_telemetry(collection_name)

    # wait=false so updates are enqueued but not yet applied
    assert queue_info["length"] == 1
    assert queue_telemetry["op_num"] == 0

    # apply second update
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'false'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": [0.05, 0.61, 0.76, 0.74],
                    "payload": {}
                }
            ]}
    )
    assert response.ok

    queue_info = get_queue_info(collection_name)
    queue_telemetry = get_queue_shard_telemetry(collection_name)
    # wait=false so updates are enqueued but not yet applied
    assert queue_info["length"] == 2
    assert queue_telemetry["op_num"] == 0
