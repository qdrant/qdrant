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
    #             {
    #   "id": { "num": 1 },
    #   "vectors": {"vector": {"data": [0.05, 0.61, 0.76, 0.74] }},
    #   "payload": {
    #     "city": { "string_value": "Berlin" },
    #     "country":  { "string_value": "Germany" },
    #     "population": { "integer_value":  1000000 },
    #     "square": { "double_value": 12.5 },
    #     "coords": { "struct_value": { "fields": { "lat": { "double_value": 1.0 }, "lon": { "double_value": 2.0 } } } }
    #   }
    # },
            "points": [
                {
                    "id": 1,
                    "vector": [0.05, 0.61, 0.76, 0.74],
                    "payload": {"city": "Berlin", "population": 1000000, "square": 12.5, "coords": {"lat": 1.0, "lon": 2.0}}
                },
                {
                    "id": 2,
                    "vector": [0.19, 0.81, 0.75, 0.11],
                    "payload": {"city": ["Berlin", "London"]}
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


def test_scroll():
    def scroll_with_vector(keyword, key: str = "", direction: str =None,    offset: int = None):
        response = request_with_validation(
            api='/collections/{collection_name}/points/scroll',
            method='POST',
            path_params={'collection_name': collection_name},
            body={
                keyword: True, # <--- should make no difference
                "order_by": {
                    "key": "city",
                }
            }
        )

        print(response.content)
        assert False
        assert response.ok
        body = response.json()

        assert body["result"]["points"][0]["vector"] == vector

    scroll_with_vector("with_vector")
    scroll_with_vector("with_vectors")
        
