import random

import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import basic_collection_setup, drop_collection

collection_name = 'test_collection_filter'


@pytest.fixture(autouse=True, scope="module")
def setup():
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_match_any():
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 3,
            "filter": {
                "must": [
                    {
                        "key": "city",
                        "match": {
                            "any": ["London", "Moscow"]
                        }
                    }
                ]
            }
        }
    )
    assert response.ok

    json = response.json()
    assert len(json['result']) == 3

    ids = [x['id'] for x in json['result']]
    assert 2 in ids
    assert 3 in ids
    assert 4 in ids
