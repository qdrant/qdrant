import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation
from .test_nested_payload_indexing import nested_payload_collection_setup


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, on_disk_payload, collection_name):
    nested_payload_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors, on_disk_payload=on_disk_payload)
    yield
    drop_collection(collection_name=collection_name)


def test_nested_strict_mode(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    # Include strict mode into the test
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PATCH",
        path_params={'collection_name': collection_name},
        body={
            "strict_mode_config": {
                "enabled": True,
                "unindexed_filtering_retrieve": False
            }
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
            "field_name": "country.cities[].population",
            "field_schema": "float"
        }
    )
    assert response.ok

    # Search with nested filter on indexed & non payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "nested": {
                            "key": "country.cities",
                            "filter": {
                                "must": [
                                    {
                                        "key": "population",
                                        "range": {
                                            "gte": 8.0,
                                        }
                                    },
                                    {
                                        "key": "sightseeing",
                                        "values_count": {
                                            "lt": 3
                                        }
                                    }
                                ]
                            }
                        }
                    }
                ]
            },
            "limit": 3
        }
    )
    # One payload index is missing
    assert not response.ok


    # Create missing payload index
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": "country.cities[].sightseeing",
            "field_schema": "integer"
        }
    )
    assert response.ok



    # Search with nested filter on indexed & non payload
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "nested": {
                            "key": "country.cities",
                            "filter": {
                                "must": [
                                    {
                                        "key": "population",
                                        "range": {
                                            "gte": 8.0,
                                        }
                                    },
                                    {
                                        "key": "sightseeing",
                                        "values_count": {
                                            "lt": 3
                                        }
                                    }
                                ]
                            }
                        }
                    }
                ]
            },
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 1
    # Only Tokio has more than 8M inhabitants and less than 3 sightseeing
    assert response.json()['result']['points'][0]['payload']['country']['name'] == "Japan"


