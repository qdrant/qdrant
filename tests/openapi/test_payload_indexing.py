import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(collection_name):
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)

def test_payload_indexing_validation(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": "test_payload",
            "field_schema": {
              "type": "integer",
              "lookup": False,
              "range": False,
            }
        }
    )
    assert response.status_code == 422
    assert "Validation error: the 'lookup' and 'range' capabilities can't be both disabled" in response.json()["status"]["error"]

def test_payload_indexing_operations(collection_name):
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
    assert response.json()[
        'result']['payload_schema']['test_payload']['data_type'] == "keyword"

    # Delete index
    response = request_with_validation(
        api='/collections/{collection_name}/index/{field_name}',
        method="DELETE",
        path_params={'collection_name': collection_name,
                     'field_name': 'test_payload'},
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


def set_payload(collection_name, payload, points):
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

def test_index_with_numeric_key(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": "123",
            "field_schema": "keyword"
        }
    )
    assert response.ok


def test_boolean_index(collection_name):
    bool_key = "boolean_payload"
    # create payload
    set_payload(collection_name, {bool_key: False}, [1, 2, 3, 4])
    set_payload(collection_name, {bool_key: [True, False]}, [5])
    set_payload(collection_name, {bool_key: True}, [6, 7])

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
    assert response.json()[
        'result']['payload_schema'][bool_key]['data_type'] == "bool"
    assert response.json()['result']['payload_schema'][bool_key]['points'] == 7

    # Delete index
    response = request_with_validation(
        api='/collections/{collection_name}/index/{field_name}',
        method="DELETE",
        path_params={'collection_name': collection_name,
                     'field_name': bool_key},
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


def test_datetime_indexing(collection_name):
    datetime_key = "datetime_payload"
    # create payload
    set_payload(collection_name, {datetime_key: "2015-01-01T00:00:00Z"}, [1])
    set_payload(collection_name, {datetime_key: "2015-02-01T08:00:00+02:00"}, [2])

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

    # test with mixed datetime format
    data = [
        ({"gte": "2015-01-01", "lte": "2015-01-01 00:00"}, [1]),
        ({"gte": "2015-01-01T01:00:00+01:00", "lte": "2015-01-01T01:00:00+01:00"}, [1]),
        ({"gte": "2015-02-01T06:00:00", "lte": "2015-02-01T06:00:00Z"}, [2]),
        # date_optional_time
        ({"gte": "2015-02-01T06:00:00.000000000", "lte": "2015-02-01T06:00:00.000000000"}, [2]),
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

    # test with wrong/unsupported datetime format
    wrong_data = [
        # RFC 2822 format
        ({"gte": "Thu, 01 Jan 2015 01:00:00 +0100",
         "lte": "Thu, 01 Jan 2015 01:00:00 +0100"}, [1]),
        # Unix millisecond timestamp
        ({"gte": "1422758400000", "lte": "1422758400000"}, [1]),
        # Unix second timestamp
        ({"gte": "1420070400", "lte": "1420070400"}, [1]),
        # Human readable format
        ({"gte": "2015-02-01 06:00:00 AM",
         "lte": "2015-02-01 06:00:00 AM"}, [2]),
        # Here are some elasticsearch built-in datetime format
        # Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html#built-in-date-formats
        # basic_date
        ({"gte": "20150201", "lte": "20150201"}, [2]),
        # basic_date_time
        ({"gte": "20150201T060000.000Z", "lte": "20150201T060000.000Z"}, [2]),
        # basic_ordinal_date_time_no_millis
        ({"gte": "2015032T060000Z", "lte": "2015032T060000Z"}, [2]),
        # wrong format mixed with supported format
        ({"gte": "2015032T060000Z", "lte": "2015-02-01T06:00:00"}, [2]),
    ]
    for range_, expected_ids in wrong_data:
        response = request_with_validation(
            api="/collections/{collection_name}/points/scroll",
            method="POST",
            path_params={"collection_name": collection_name},
            body={
                "with_vector": False,
                "filter": {"must": [{"key": datetime_key, "range": range_}]},
            },
        )
        assert not response.ok


def test_update_payload_on_indexed_field(collection_name):
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
                    {"key": "city", "match": {"value": "Berlin"}}
                ]
            }
        }
    )
    assert response.ok
    assert [p['id'] for p in response.json()['result']['points']] == [1, 2, 3]

    # 2: city: [Berlin, London]
    # 4: city: [London, Moscow]
    set_payload(collection_name, {"foo": "bar"}, [2, 4])

    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "with_vector": False,
            "filter": {
                "must": [
                    {"key": "city", "match": {"value": "Berlin"}}
                ]
            }
        }
    )
    assert response.ok
    assert [p['id'] for p in response.json()['result']['points']] == [1, 2, 3]


def test_payload_schemas(collection_name):
    FIELDS = [
        "keyword",
        "integer",
        "float",
        "geo",
        "text",
        "bool",
        "datetime",
        "uuid",
        {
            "type": "keyword",
            "is_tenant": True,
            "on_disk": True,
        },
        {
            "type": "integer",
            "lookup": True,
            "range": True,
            "is_principal": True,
            "on_disk": True,
        },
        {
            "type": "float",
            "on_disk": True,
            "is_principal": True,
        },
        {
            "type": "geo",
        },
        {
            "type": "text",
            "tokenizer": "word",
            "lowercase": True,
            "min_token_len": 2,
            "max_token_len": 10,
        },
        {
            "type": "bool",
        },
        {
            "type": "datetime",
            "on_disk": True,
            "is_principal": True,
        },
        {
            "type": "uuid",
            "is_tenant": True,
            "on_disk": True,
        },
    ]

    try:
        for field_no, schema in enumerate(FIELDS):
            response = request_with_validation(
                api="/collections/{collection_name}/index",
                method="PUT",
                path_params={"collection_name": collection_name},
                query_params={"wait": "true"},
                body={
                    "field_name": f"field_{field_no:02d}",
                    "field_schema": schema,
                },
            )
            assert response.ok

        response = request_with_validation(
            api="/collections/{collection_name}",
            method="GET",
            path_params={"collection_name": collection_name},
        )
        assert response.ok
        for field_no, schema in enumerate(FIELDS):
            actual_schema = response.json()["result"]["payload_schema"][
                f"field_{field_no:02d}"
            ]
            if isinstance(schema, str):
                assert actual_schema["data_type"] == schema
                assert "params" not in actual_schema
            else:
                assert actual_schema["data_type"] == schema["type"]
                assert actual_schema["params"] == schema
            assert actual_schema["points"] == 0
    finally:
        for field_no in range(len(FIELDS)):
            response = request_with_validation(
                api="/collections/{collection_name}/index/{field_name}",
                method="DELETE",
                path_params={
                    "collection_name": collection_name,
                    "field_name": f"field_{field_no:02d}",
                },
            )
