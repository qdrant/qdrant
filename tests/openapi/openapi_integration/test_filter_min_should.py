import pytest

from .helpers.collection_setup import multipayload_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection_filter_min_should'


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    multipayload_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_min_should():
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "with_payload": ["city", "color", "count", "price"],
            "limit": 6,
            "filter": {
                "min_should": {
                    "conditions": [
                        {
                            "key": "city",
                            "match": {
                                "any": ["Berlin", "Moscow"]
                            }
                        },
                        {
                            "key": "color",
                            "match": {
                                "value": "red"
                            }
                        },
                        {
                            "key": "count",
                            "match": {
                                "value": 0
                            }
                        }
                    ],
                    "min_count": 2
                }

            }
        }
    )
    assert response.ok

    json = response.json()
    assert len(json['result']) == 6

    ids = [x['id'] for x in json['result']]
    assert 1 in ids
    assert 4 in ids
    assert 5 in ids
    assert 6 in ids
    assert 7 in ids
    assert 8 in ids


def test_min_should_combined_with_must():
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "with_payload": ["city", "color", "count", "price"],
            "limit": 2,
            "filter": {
                "min_should": {
                    "conditions": [
                        {
                            "key": "city",
                            "match": {
                                "any": ["Berlin", "Moscow"]
                            }
                        },
                        {
                            "key": "color",
                            "match": {
                                "value": "red"
                            }
                        },
                        {
                            "key": "count",
                            "match": {
                                "value": 0
                            }
                        }
                    ],
                    "min_count": 2
                },
                "must": [
                    {
                        "key": "price",
                        "range": {
                            "gt": None,
                            "gte": 50.0,
                            "lt": None,
                            "lte": 200.0
                        }
                    }
                ]
            }
        }
    )
    assert response.ok

    json = response.json()
    assert len(json['result']) == 2

    ids = [x['id'] for x in json['result']]
    assert 7 in ids
    assert 8 in ids


def test_nested_min_should():
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "with_payload": ["city", "color", "count", "price"],
            "limit": 2,
            "filter": {
                "must_not": [
                    {
                        "min_should": {
                            "conditions": [
                                {
                                    "key": "city",
                                    "match": {
                                        "any": ["Berlin", "Moscow"]
                                    }
                                },
                                {
                                    "key": "color",
                                    "match": {
                                        "value": "red"
                                    }
                                },
                                {
                                    "key": "count",
                                    "match": {
                                        "value": 0
                                    }
                                }
                            ],
                            "min_count": 2
                        },
                    }
                ] 
            }
        }
    )
    assert response.ok

    json = response.json()
    assert len(json['result']) == 2

    ids = [x['id'] for x in json['result']]
    assert 2 in ids
    assert 3 in ids


def test_missing_min_count():
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 3,
            "filter": {
                "min_should": {
                    "conditions": [
                        {
                            "key": "city",
                            "match": {
                                "any": ["London", "Moscow"]
                            }
                        }
                    ]
                }
            }
        }
    )
    assert not response.ok
    assert response.status_code == 400