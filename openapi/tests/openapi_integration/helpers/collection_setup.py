from openapi_integration.helpers.helpers import request_with_validation


def drop_collection(collection_name='test_collection'):
    response = request_with_validation(
        api='/collections/{name}',
        method="DELETE",
        path_params={'name': collection_name},
    )
    assert response.ok


def basic_collection_setup(collection_name='test_collection'):
    response = request_with_validation(
        api='/collections/{name}',
        method="DELETE",
        path_params={'name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{name}',
        method="PUT",
        path_params={'name': collection_name},
        body={
            "vector_size": 4,
            "distance": "Dot"
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{name}',
        method="GET",
        path_params={'name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{name}/points',
        method="PUT",
        path_params={'name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": [0.05, 0.61, 0.76, 0.74],
                    "payload": {"city": {"type": "keyword", "value": "Berlin"}}
                },
                {
                    "id": 2,
                    "vector": [0.19, 0.81, 0.75, 0.11],
                    "payload": {"city": {"type": "keyword", "value": ["Berlin", "London"]}}
                },
                {
                    "id": 3,
                    "vector": [0.36, 0.55, 0.47, 0.94],
                    "payload": {"city": {"type": "keyword", "value": ["Berlin", "Moscow"]}}
                },
                {
                    "id": 4,
                    "vector": [0.18, 0.01, 0.85, 0.80],
                    "payload": {"city": {"type": "keyword", "value": ["London", "Moscow"]}}
                },
                {
                    "id": 5,
                    "vector": [0.24, 0.18, 0.22, 0.44],
                    "payload": {"count": {"type": "integer", "value": [0]}}
                },
                {
                    "id": 6,
                    "vector": [0.35, 0.08, 0.11, 0.44]
                }
            ]
        }
    )
    assert response.ok
