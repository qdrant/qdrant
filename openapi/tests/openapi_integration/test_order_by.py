import os
import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_collection'


def sepical_collection_setup(
    collection_name='test_collection',
    on_disk_payload=False,
    on_disk_vectors=None,
):
    on_disk_vectors = on_disk_vectors or bool(int(os.getenv('QDRANT__ON_DISK_VECTORS', 0)))

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
                "distance": "Dot",
                "on_disk": on_disk_vectors
            },
            "on_disk_payload": on_disk_payload
        }
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
                    "payload": {"city": "Kyiv", "population": 3000000, "square": 12.5, "coords": {"lat": 1.0, "lon": 2.0}}
                },
                {
                    "id": 2,
                    "vector": [0.19, 0.81, 0.75, 0.11],
                    "payload": {"city": "Madrid", "population": 3.305408}
                },
                {
                    "id": 3,
                    "vector": [0.36, 0.55, 0.47, 0.94],
                    "payload": {"city": ["Berlin", "Moscow"]}
                },
                {
                    "id": 4,
                    "vector": [0.18, 0.01, 0.85, 0.80],
                    "payload": {"city": "London","population": 6000000}
                },
                {
                    "id": 5,
                    "vector": [0.24, 0.18, 0.22, 0.44],
                    "payload": {"city": "Moscow", "population": "unknown"}
                },
                {
                    "id": 6,
                    "vector": [0.25, 0.10, 0.54, 0.90],
                    "payload": {"city": "Rome", "population": 3000000}
                }
            ]
        }
    )
    assert response.ok


@pytest.fixture(autouse=True, scope="module")
def setup():
    sepical_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_order():

    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method='POST',
        path_params={'collection_name': collection_name},
        body={ # <--- should make no difference
            "order_by": {
                "key": "population",
                "direction": "DESC"
            }
        }
    )

    assert response.ok
    json = response.json()
    ids = [x['id'] for x in json['result']['points']]
    assert ids == [2, 1, 6, 4, 3, 5]

def test_offset():
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method='POST',
        path_params={'collection_name': collection_name},
        body={ # <--- should make no difference
            "order_by": {
                "key": "population",
                "direction": "DESC",
                "offset": 3
            },
            "offset": 6
        }
    ) 

    assert response.ok

    json = response.json()
    print(json)
    assert False

        
