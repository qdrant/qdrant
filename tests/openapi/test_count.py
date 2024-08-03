import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection_threshold'


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_exact_count_search():
    response = request_with_validation(
        api='/collections/{collection_name}/points/count',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "should": [
                    {
                        "key": "city",
                        "match": {
                            "value": "London"
                        }
                    },
                    {
                        "key": "city",
                        "match": {
                            "value": "Berlin"
                        }
                    }
                ]
            }
        }
    )
    assert response.ok
    assert response.json()['result']['count'] == 4


def test_approx_count_search():
    response = request_with_validation(
        api='/collections/{collection_name}/points/count',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {
                "should": [
                    {
                        "key": "city",
                        "match": {
                            "value": "London"
                        }
                    },
                    {
                        "key": "city",
                        "match": {
                            "value": "Berlin"
                        }
                    }
                ]
            },
            "exact": False
        }
    )
    assert response.ok
    assert response.json()['result']['count'] < 10
    assert response.json()['result']['count'] > 0
