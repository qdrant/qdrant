import pytest
import json

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import drop_collection

collection_name = 'test_collection_payload_indexing'


def nested_payload_collection_setup(collection_name, on_disk_payload=False):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 4,
                "distance": "Dot"
            },
            "on_disk_payload": on_disk_payload
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

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
                    "payload": {
                        "country": {
                            "name": "Germany",
                            "capital": "Berlin",
                            "sightseeing": ["Brandenburg Gate", "Reichstag"]
                        }
                    }
                },
                {
                    "id": 2,
                    "vector": [0.19, 0.81, 0.75, 0.11],
                    "payload": {
                        "country": {
                            "name": "England",
                            "capital": "London",
                            "sightseeing": ["Big Ben", "London Eye"]
                        }
                    }
                },
                {
                    "id": 3,
                    "vector": [0.36, 0.55, 0.47, 0.94],
                    "payload": {
                        "country": {
                            "name": "France",
                            "capital": "Paris",
                            "sightseeing": ["Eiffel Tower", "Louvre"]
                        }
                    }
                },
                {
                    "id": 4,
                    "vector": [0.18, 0.01, 0.85, 0.80],
                    "payload": {
                        "country": {
                            "name": "Japan",
                            "capital": "Tokyo",
                            "sightseeing": ["Tokyo Tower", "Shibuya Crossing"]
                        }
                    }
                },
                {
                    "id": 5,
                    "vector": [0.24, 0.18, 0.22, 0.44],
                    "payload": {
                        "country": {
                            "name": "Nauru",
                            "sightseeing": ["Nauru Island"]
                        }
                    }
                },
                {
                    "id": 6,
                    "vector": [0.35, 0.08, 0.11, 0.44]
                }
            ]
        }
    )
    assert response.ok


@pytest.fixture(autouse=True)
def setup():
    nested_payload_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_payload_indexing_operations():
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    # Create nested index
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": "country.capital",
            "field_schema": "keyword"
        }
    )
    assert response.ok

    # Create nested array index
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": "country.sightseeing",
            "field_schema": "keyword"
        }
    )
    assert response.ok

    # Validate index creation
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
    assert response.json()['result']['payload_schema']['country.capital']['data_type'] == "keyword"
    assert response.json()['result']['payload_schema']['country.capital']['points'] == 4
    assert response.json()['result']['payload_schema']['country.sightseeing']['data_type'] == "keyword"
    assert response.json()['result']['payload_schema']['country.sightseeing']['points'] == 5

    # Search through payload index
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "should": [
                    {
                        "key": "country.capital",
                        "match": {
                            "value": "London"
                        }
                    }
                ]
            },
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 1
    assert response.json()['result']['points'][0]['payload']['country']['name'] == "England"

    # Search without payload index
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "should": [
                    {
                        "key": "country.name",
                        "match": {
                            "value": "France"
                        }
                    }
                ]
            },
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 1
    assert response.json()['result']['points'][0]['payload']['country']['capital'] == "Paris"

    # Search with payload index
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "should": [
                    {
                        "key": "country.sightseeing",
                        "match": {
                            "value": "Eiffel Tower"
                        }
                    }
                ]
            },
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 1
    assert response.json()['result']['points'][0]['payload']['country']['capital'] == "Paris"

    # Delete indexes
    response = request_with_validation(
        api='/collections/{collection_name}/index/{field_name}',
        method="DELETE",
        path_params={'collection_name': collection_name, 'field_name': 'country.capital'},
        query_params={'wait': 'true'},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/index/{field_name}',
        method="DELETE",
        path_params={'collection_name': collection_name, 'field_name': 'country.sightseeing'},
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

