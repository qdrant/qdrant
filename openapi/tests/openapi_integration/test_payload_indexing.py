import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection_payload_indexing'


@pytest.fixture(autouse=True)
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_payload_indexing_operations():
    # create payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "payload": {"test_payload": "keyword"},
            "points": [6]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    # Create index
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": "test_payload",
            "field_schema": "keyword"
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert response.json()['result']['payload_schema']['test_payload']['data_type'] == "keyword"

    # Delete index
    response = request_with_validation(
        api='/collections/{collection_name}/index/{field_name}',
        method="DELETE",
        path_params={'collection_name': collection_name, 'field_name': 'test_payload'},
        query_params={'wait': 'true'},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']['payload_schema']) == 0


def set_payload(payload, points):
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "payload": payload,
            "points": points
        }
    )
    assert response.ok


def test_boolean_index():
    bool_key = "boolean_payload"
    # create payload
    set_payload({bool_key: False}, [1, 2, 3, 4])
    set_payload({bool_key: [True, False]}, [5])
    set_payload({bool_key: True}, [6, 7])

    # Create index
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": bool_key,
            "field_schema": "bool"
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert response.json()['result']['payload_schema'][bool_key]['data_type'] == "bool"
    assert response.json()['result']['payload_schema'][bool_key]['points'] == 7

    # Delete index
    response = request_with_validation(
        api='/collections/{collection_name}/index/{field_name}',
        method="DELETE",
        path_params={'collection_name': collection_name, 'field_name': bool_key},
        query_params={'wait': 'true'},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert len(response.json()['result']['payload_schema']) == 0


def test_datetime_indexing():
    datetime_key = "datetime_payload"
    # create payload
    set_payload({datetime_key: "2015-01-01T00:00:00Z"}, [1])
    set_payload({datetime_key: "2015-02-01T08:00:00+02:00"}, [2])

    # Create index
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": datetime_key,
            "field_schema": "datetime"
        }
    )
    assert response.ok

    data = [
        ({"gte": "2015-01-01T00:00:00Z", "lte": "2015-01-01T00:00:00Z"}, [1]),
        ({"gte": "2015-01-01T01:00:00+01:00", "lte": "2015-01-01T01:00:00+01:00"}, [1]),
        ({"gte": "2015-02-01T06:00:00Z", "lte": "2015-02-01T06:00:00Z"}, [2]),
    ]
    for range_, expected_ids in data:
        response = request_with_validation(
            api="/collections/{collection_name}/points/scroll",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "with_vector": False,
                "filter": {"must": [{"key": datetime_key, "range": range_}]},
            },
        )
        assert response.ok, response.json()
        
        point_ids = [p["id"] for p in response.json()["result"]["points"]]
        assert all(id in point_ids for id in expected_ids)

def test_update_payload_on_indexed_field():
    keyword_field = "city"

    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": keyword_field,
            "field_schema": "keyword"
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': collection_name},
        body={
            "optimizers_config": {
                "indexing_threshold": 100
            }
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "with_vector": False,
            "filter": {
                "must": [
                    {"key": "city", "match": {"value": "Berlin"} }
                ]
            }
        }
    )
    assert response.ok
    assert [p['id'] for p in response.json()['result']['points']] == [1, 2, 3]

    # 2: city: [Berlin, London]
    # 4: city: [London, Moscow]
    set_payload({"foo": "bar"}, [2, 4])

    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "with_vector": False,
            "filter": {
                "must": [
                    {"key": "city", "match": {"value": "Berlin"} }
                ]
            }
        }
    )
    assert response.ok
    assert [p['id'] for p in response.json()['result']['points']] == [1, 2, 3]

