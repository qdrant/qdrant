import time

import pytest
import random

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


def setup_big_collection(collection_name, num_points, dim, keywords=5):
    drop_collection(collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": dim,
                "distance": "Dot",
                "on_disk": False,
            },
            "optimizers_config": {
                "default_segment_number": 4,
                "indexing_threshold": 10
            }
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    # Create payload index
    response = request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": "a",
            "field_schema": "keyword"
        }
    )

    assert response.ok

    points = [
        {
            "id": i,
            "vector": [random.random() for _ in range(dim)],
            "payload": {
                "a": f"keyword_{i % keywords}"
            }
        }
        for i in range(num_points)
    ]

    for i in range(0, len(points), 100):
        response = request_with_validation(
            api='/collections/{collection_name}/points',
            method="PUT",
            path_params={'collection_name': collection_name},
            query_params={'wait': 'true'},
            body={"points": points[i:i + 100]}
        )
        assert response.ok


def wait_collection_green(collection_name):
    while True:
        response = request_with_validation(
            api='/collections/{collection_name}',
            method="GET",
            path_params={'collection_name': collection_name},
        )
        assert response.ok

        json = response.json()
        if json['result']['status'] == 'green':
            break

        time.sleep(1)


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors, collection_name):
    setup_big_collection(collection_name=collection_name, num_points=3000, dim=4)
    wait_collection_green(collection_name)
    yield
    drop_collection(collection_name=collection_name)


def delete_payload(collection_name, payload_value):
    response = request_with_validation(
        api='/collections/{collection_name}/points/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "filter": {
                "must": [
                    {
                        "key": "a",
                        "match": {"value": payload_value}
                    }
                ]
            }
        }
    )
    assert response.ok


def test_delete_by_payload_filter(collection_name):
    delete_payload(collection_name, "keyword_0")
    delete_payload(collection_name, "keyword_1")
    delete_payload(collection_name, "keyword_2")
    delete_payload(collection_name, "keyword_3")
    delete_payload(collection_name, "keyword_4")

    response = request_with_validation(
        api='/collections/{collection_name}/points/query',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "limit": 1000,
        }
    )

    assert response.ok
    json = response.json()
    assert len(json['result']['points']) == 0
