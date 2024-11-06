import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation
from .test_nested_payload_indexing import nested_payload_collection_setup


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, on_disk_payload, collection_name):
    nested_payload_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors, on_disk_payload=on_disk_payload)
    yield
    drop_collection(collection_name=collection_name)


def test_payload_selectors(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    # Search without payload selector
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "key": "country.name",
                        "match": {
                            "value": "Germany",
                        }
                    }
                ]
            },
            "limit": 3,
            "with_payload": False,
        }
    )
    assert response.ok
    assert len(response.json()['result']['points']) == 1
    assert response.json()['result']['points'][0].get('payload') is None

    # Search with payload selector ALL
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "key": "country.name",
                        "match": {
                            "value": "Germany",
                        }
                    }
                ]
            },
            "limit": 3,
            "with_payload": True,
        }
    )
    assert response.ok
    assert response.json()['result']['points'][0]['payload'] == {
        "country": {
            "name": "Germany",
            "capital": "Berlin",
            "cities": [
                {
                    "name": "Berlin",
                    "population": 3.7,
                    "location": {
                        "lon": 13.76116,
                        "lat": 52.33826,
                    },
                    "sightseeing": ["Brandenburg Gate", "Reichstag"]
                },
                {
                    "name": "Munich",
                    "population": 1.5,
                    "location": {
                        "lon": 11.57549,
                        "lat": 48.13743,
                    },
                    "sightseeing": ["Marienplatz", "Olympiapark"]
                },
                {
                    "name": "Hamburg",
                    "population": 1.8,
                    "location": {
                        "lon": 9.99368,
                        "lat": 53.55108,
                    },
                    "sightseeing": ["Reeperbahn", "Elbphilharmonie"]
                }
            ],
        }
    }

    # Search with payload selector include paths
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "key": "country.name",
                        "match": {
                            "value": "Germany",
                        }
                    }
                ]
            },
            "limit": 3,
            "with_payload": {
                "include": ["country.name", "country.capital"],
            },
        }
    )
    assert response.ok
    assert response.json()['result']['points'][0]['payload'] == {
        "country": {
            "name": "Germany",
            "capital": "Berlin",
        }
    }

    # Search with payload selector include paths
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "key": "country.name",
                        "match": {
                            "value": "Germany",
                        }
                    }
                ]
            },
            "limit": 3,
            "with_payload": {
                "include": ["country.cities"],
            },
        }
    )
    assert response.ok
    assert response.json()['result']['points'][0]['payload'] == {
        "country": {
            "cities": [
                {
                    "name": "Berlin",
                    "population": 3.7,
                    "location": {
                        "lon": 13.76116,
                        "lat": 52.33826,
                    },
                    "sightseeing": ["Brandenburg Gate", "Reichstag"]
                },
                {
                    "name": "Munich",
                    "population": 1.5,
                    "location": {
                        "lon": 11.57549,
                        "lat": 48.13743,
                    },
                    "sightseeing": ["Marienplatz", "Olympiapark"]
                },
                {
                    "name": "Hamburg",
                    "population": 1.8,
                    "location": {
                        "lon": 9.99368,
                        "lat": 53.55108,
                    },
                    "sightseeing": ["Reeperbahn", "Elbphilharmonie"]
                }
            ],
        }
    }

    # Search with payload selector include paths
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "key": "country.name",
                        "match": {
                            "value": "Germany",
                        }
                    }
                ]
            },
            "limit": 3,
            "with_payload": {
                "include": ["country.cities[].name"],
            },
        }
    )
    assert response.ok
    assert response.json()['result']['points'][0]['payload'] == {
        "country": {
            "cities": [
                {
                    "name": "Berlin",
                },
                {
                    "name": "Munich"
                },
                {
                    "name": "Hamburg"
                }
            ],
        }
    }

    # Search with payload selector exclude paths
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "key": "country.name",
                        "match": {
                            "value": "Germany",
                        }
                    }
                ]
            },
            "limit": 3,
            "with_payload": {
                "exclude": ["country.cities"],
            },
        }
    )
    assert response.ok
    assert response.json()['result']['points'][0]['payload'] == {
        "country": {
            "name": "Germany",
            "capital": "Berlin",
        }
    }

    # Search with payload selector exclude at array index
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "must": [
                    {
                        "key": "country.name",
                        "match": {
                            "value": "Germany",
                        }
                    }
                ]
            },
            "limit": 3,
            "with_payload": {
                "exclude": ["country.name", "country.capital"],
            },
        }
    )
    assert response.ok
    assert response.json()['result']['points'][0]['payload'] == {
        "country": {
            "cities": [
                {
                    "name": "Berlin",
                    "population": 3.7,
                    "location": {
                        "lon": 13.76116,
                        "lat": 52.33826,
                    },
                    "sightseeing": ["Brandenburg Gate", "Reichstag"]
                },
                {
                    "name": "Munich",
                    "population": 1.5,
                    "location": {
                        "lon": 11.57549,
                        "lat": 48.13743,
                    },
                    "sightseeing": ["Marienplatz", "Olympiapark"]
                },
                {
                    "name": "Hamburg",
                    "population": 1.8,
                    "location": {
                        "lon": 9.99368,
                        "lat": 53.55108,
                    },
                    "sightseeing": ["Reeperbahn", "Elbphilharmonie"]
                }
            ],
        }
    }
