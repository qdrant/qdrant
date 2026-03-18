import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation
from .test_nested_payload_indexing import nested_payload_collection_setup


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, on_disk_payload, collection_name):
    nested_payload_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors, on_disk_payload=on_disk_payload)
    yield
    drop_collection(collection_name=collection_name)


def test_nested_payload_indexing_operations(collection_name):
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
            "field_name": "country.cities[].population",
            "field_schema": "float"
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
    assert response.json()['result']['payload_schema']['country.cities[].population']['data_type'] == "float"
    assert response.json()['result']['payload_schema']['country.cities[].population']['points'] == 4  # indexed records

    # Search nested array without payload index
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "nested": {
                            "key": "country.cities[]",
                            "filter": {
                                "must": [
                                    {
                                        "key": "population",
                                        "range": {
                                            "gte": 9.0,
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

    # Search nested array without payload index (implicit array key)
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "nested": {
                            "key": "country.cities",  # implicitly add array [] to key
                            "filter": {
                                "must": [
                                    {
                                        "key": "population",
                                        "range": {
                                            "gte": 9.0,
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

    # Search with nested filter on non indexed payload
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
                                        "must": [
                                            {
                                                "key": "sightseeing",
                                                "values_count": {
                                                    "gt": 3
                                                }
                                            }
                                        ]
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
    assert response.json()['result']['points'][0]['payload']['country']['name'] == "France"

    # Search with nested filter and must-not
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
                                            "gte": 9.0,
                                        }
                                    }
                                ],
                                "must_not": [
                                    {
                                        "key": "sightseeing",
                                        "values_count": {
                                            "gt": 1
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
    assert len(response.json()['result']['points']) == 0

    # Search with nested filter on indexed payload
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
                                            "gte": 9.0,
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
    assert response.json()['result']['points'][0]['payload']['country']['name'] == "Japan"

    # Search with nested filter on indexed & non payload (match only within point)
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
                                            "gte": 9.0,
                                        }
                                    },
                                    {
                                        "key": "sightseeing",
                                        "values_count": {
                                            "gt": 2
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
    # No single point has in its payload a city with more than 8M inhabitants AND more than 2 sightseeing
    # Japan has 9M inhabitants but only 2 sightseeing
    # London & Paris have 3 sightseeing but less 9M inhabitants
    assert len(response.json()['result']['points']) == 0

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

    # Search with nested filter on indexed & non payload (must_not)
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
                                "must_not": [
                                    {
                                        "key": "population",
                                        "range": {
                                            "lt": 8.0,
                                        }
                                    },
                                    {
                                        "is_null": {
                                            "key": "name"
                                        }
                                    }
                                ],
                            }
                        }
                    }
                ]
            },
            "limit": 3
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 2
    # Only 2 records that do NOT have at least one city population < 8.0
    assert response.json()['result']['points'][0]['payload']['country']['name'] == "England"  # London
    assert response.json()['result']['points'][1]['payload']['country']['name'] == "Japan"  # Tokyo

    # Search with nested filter on indexed & non payload (must_not)
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
                                "must_not": [
                                    {
                                        "key": "population",
                                        "range": {
                                            "lt": 8.0,
                                        }
                                    },
                                    {
                                        "key": "sightseeing",
                                        "values_count": {
                                            "gte": 3
                                        }
                                    },
                                    {
                                        "is_null": {
                                            "key": "name"
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
    # Only 1 record that do NOT have at least one city population < 8.0 and NOT more than 3 sightseeing
    assert response.json()['result']['points'][0]['payload']['country']['name'] == "Japan"  # Tokyo

    # Search nested null field
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
                                        "is_null": {
                                            "key": "name"
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
    assert response.json()['result']['points'][0]['payload']['country']['name'] == "Nauru"

    # Search nested geobox field
    geo_box_berlin = {
        "bottom_right": {
            "lon": 13.76117,
            "lat": 52.33825,
        },
        "top_left": {
            "lon": 13.08835,
            "lat": 52.67551,
        }
    }
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
                                        "key": "location",
                                        "geo_bounding_box": geo_box_berlin
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
    assert response.json()['result']['points'][0]['payload']['country']['name'] == "Germany"

    # Search nested geo-radius field
    berlin_center = {
        "lon": 13.76117,
        "lat": 52.33825,
    }
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
                                        "key": "location",
                                        "geo_radius": {
                                            "center": berlin_center,
                                            "radius": 1000,  # meters
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
    assert response.json()['result']['points'][0]['payload']['country']['name'] == "Germany"

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
        path_params={'collection_name': collection_name, 'field_name': 'country.cities[].population'},
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
