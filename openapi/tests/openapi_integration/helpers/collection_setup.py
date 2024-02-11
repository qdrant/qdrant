
from .helpers import request_with_validation


def drop_collection(collection_name='test_collection'):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok


def geo_collection_setup(
        collection_name='test_collection',
        on_disk_payload=False,
        on_disk_vectors=False,
):
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
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
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
                    "payload": {
                        "value": 1,
                        "location": {
                                    "lon": 50.5200,
                                    "lat": 50.4050
                                }}
                },
                {
                    "id": 2,
                    "vector": [0.19, 0.81, 0.75, 0.11],
                    "payload": {
                        "value": 2,
                        "location": {
                                    "lon": 60.5200,
                                    "lat": 60.4050
                                }}
                },
                {
                    "id": 3,
                    "vector": [0.36, 0.55, 0.47, 0.94],
                    "payload": {
                        "value": 3,
                        "location": {
                                    "lon": -60.5200,
                                    "lat": -60.4050
                                }}
                },
                {
                    "id": 4,
                    "vector": [0.18, 0.01, 0.85, 0.80],
                    "payload": {
                        "value": 4,
                        "location": {
                                    "lon": 80.5200,
                                    "lat": 80.4050
                                }
                                }
                },
                {
                    "id": 5,
                    "vector": [0.24, 0.18, 0.22, 0.44],
                    "payload": {
                        "value": 5,
                        "location": {
                                    "lon": -72.5200,
                                    "lat": -72.4050
                                }}
                },
                # add a entry doesn't contain location
                {
                    "id": 6,
                    "vector": [0.24, 0.18, 0.22, 0.44],
                    "payload": {
                        "value": 6}
                },
            ]
        }
    )
    assert response.ok


def basic_collection_setup(
        collection_name='test_collection',
        on_disk_payload=False,
        on_disk_vectors=False,
):
    drop_collection(collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 4,
                "distance": "Dot",
                "on_disk": on_disk_vectors,
            },
            "sparse_vectors": {
                "sparse-text": {},
            },
            "on_disk_payload": on_disk_payload
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
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
                    "payload": {"city": "Berlin"}
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
                    "payload": {"city": ["London", "Moscow"]}
                },
                {
                    "id": 5,
                    "vector": [0.24, 0.18, 0.22, 0.44],
                    "payload": {"count": 0}
                },
                {
                    "id": 6,
                    "vector": [0.35, 0.08, 0.11, 0.44]
                },
                {
                    "id": 7,
                    "vector": [0.25, 0.98, 0.14, 0.43],
                    "payload": {"city": None}
                },
                {
                    "id": 8,
                    "vector": [0.79, 0.53, 0.72, 0.15],
                    "payload": {"city": []}
                },
                {
                    "id": 9,
                    "vector": {
                        "sparse-text": {
                            "indices": [66, 12],
                            "values": [0.5, 0.5]
                        }
                    }
                },
                {
                    "id": 10,
                    "vector": {
                        "sparse-text": {
                            "indices": [1, 2, 3],
                            "values": [0.1, 0.2, 0.3]
                        }
                    },
                    "payload": {"city": []}
                }
            ]
        }
    )
    assert response.ok

def multipayload_collection_setup(
    collection_name='test_collection',
    on_disk_payload=False,
    on_disk_vectors=False,
):
    drop_collection(collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 4,
                "distance": "Dot",
                "on_disk": on_disk_vectors,
            },
            "sparse_vectors": {
                "sparse-text": {},
            },
            "on_disk_payload": on_disk_payload
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
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
                    "payload": {"city": "Berlin", "color": "red"}
                },
                {
                    "id": 2,
                    "vector": [0.19, 0.81, 0.75, 0.11],
                    "payload": {"city": ["Berlin", "London"], "color": "green"}
                },
                {
                    "id": 3,
                    "vector": [0.36, 0.55, 0.47, 0.94],
                    "payload": {"city": ["Berlin", "Moscow"], "color": "blue"}
                },
                {
                    "id": 4,
                    "vector": [0.18, 0.01, 0.85, 0.80],
                    "payload": {"city": ["London", "Moscow"], "color": "red"}
                },
                {
                    "id": 5,
                    "vector": [0.24, 0.18, 0.22, 0.44],
                    "payload": {"city": "Seoul", "color": "red", "count": 0}
                },
                {
                    "id": 6,
                    "vector": [0.35, 0.08, 0.11, 0.44],
                    "payload": {"city": "Berlin", "color": "red", "count": 1, "price": 10.0}
                },
                {
                    "id": 7,
                    "vector": [0.25, 0.98, 0.14, 0.43],
                    "payload": {"city": "London", "color": "red", "count": 0, "price": 50.0}
                },
                {
                    "id": 8,
                    "vector": [0.19, 0.53, 0.72, 0.15],
                    "payload": {"city": "Moscow", "color": "red", "count": 1, "price": 100.0}
                },
            ]
        }
    )
    assert response.ok


def multivec_collection_setup(
        collection_name='test_collection',
        on_disk_payload=False,
        on_disk_vectors=False,
        distance=None,
):
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
                "image": {
                    "size": 4,
                    "distance": distance or "Dot",
                    "on_disk": on_disk_vectors,
                },
                "text": {
                    "size": 8,
                    "distance": distance or "Cosine",
                    "on_disk": on_disk_vectors,
                },
            },
            "sparse_vectors": {
                "sparse-image": {},
                "sparse-text": {},
            },
            "on_disk_payload": on_disk_payload,
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
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
                    "vector": {
                        "image": [0.05, 0.61, 0.76, 0.74],
                        "text": [0.05, 0.61, 0.76, 0.74, 0.05, 0.61, 0.76, 0.74],
                    },
                    "payload": {"city": "Berlin"}
                },
                {
                    "id": 2,
                    "vector": {
                        "image": [0.19, 0.81, 0.75, 0.11],
                        "text": [0.19, 0.81, 0.75, 0.11, 0.19, 0.81, 0.75, 0.11],
                    },
                    "payload": {"city": ["Berlin", "London"]}
                },
                {
                    "id": 3,
                    "vector": {
                        "image": [0.36, 0.55, 0.47, 0.94],
                        "text": [0.36, 0.55, 0.47, 0.94, 0.36, 0.55, 0.47, 0.94],
                    },
                    "payload": {"city": ["Berlin", "Moscow"]}
                },
                {
                    "id": 4,
                    "vector": {
                        "image": [0.18, 0.01, 0.85, 0.80],
                        "text": [0.18, 0.01, 0.85, 0.80, 0.18, 0.01, 0.85, 0.80],
                    },
                    "payload": {"city": ["London", "Moscow"]}
                },
                {
                    "id": 5,
                    "vector": {
                        "image": [0.24, 0.18, 0.22, 0.44],
                        "text": [0.24, 0.18, 0.22, 0.44, 0.24, 0.18, 0.22, 0.44],
                    },
                    "payload": {"count": 0}
                },
                {
                    "id": 6,
                    "vector": {
                        "image": [0.35, 0.08, 0.11, 0.44],
                        "text": [0.35, 0.08, 0.11, 0.44, 0.35, 0.08, 0.11, 0.44],
                    }
                },
                {
                    "id": 7,
                    "vector": {
                        "sparse-image": {
                            "indices": [1, 2, 4, 8],
                            "values": [1.5, 1.5, 1.5, 1.5]
                        },
                        "sparse-text": {
                            "indices": [66, 12],
                            "values": [0.5, 0.5]
                        }
                    }
                },
                {
                    "id": 8,
                    "vector": {
                        "sparse-image": {
                            "indices": [2, 8],
                            "values": [2.5, 2.5]
                        },
                        "sparse-text": {
                            "indices": [1, 2, 3],
                            "values": [0.1, 0.2, 0.3]
                        }
                    },
                    "payload": {"count": 0}
                }
            ]
        }
    )
    assert response.ok
