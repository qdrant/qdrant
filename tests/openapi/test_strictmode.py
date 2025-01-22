import time

import pytest

from .conftest import collection_name
from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(collection_name):
    basic_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def set_strict_mode(collection_name, strict_mode_config):
    request_with_validation(
        api="/collections/{collection_name}",
        method="PATCH",
        path_params={"collection_name": collection_name},
        body={
            "strict_mode_config": strict_mode_config,
        },
    ).raise_for_status()


def get_strict_mode(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    config = response.json()['result']['config']
    if "strict_mode_config" not in config:
        return None
    else:
        return config['strict_mode_config']


def strict_mode_enabled(collection_name) -> bool:
    strict_mode = get_strict_mode(collection_name)
    return strict_mode is not None and strict_mode['enabled']


def test_patch_collection_full(collection_name):
    assert not strict_mode_enabled(collection_name)

    set_strict_mode(collection_name, {
        "enabled": True,
        "max_query_limit": 10,
        "max_timeout": 2,
        "unindexed_filtering_retrieve": False,
        "unindexed_filtering_update": False,
        "search_max_hnsw_ef": 3,
        "search_allow_exact": False,
        "search_max_oversampling": 1.5,
    })

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )

    assert response.ok
    new_strict_mode_config = response.json()['result']['config']['strict_mode_config']
    assert new_strict_mode_config['enabled']
    assert new_strict_mode_config['max_query_limit'] == 10
    assert new_strict_mode_config['max_timeout'] == 2
    assert not new_strict_mode_config['unindexed_filtering_retrieve']
    assert not new_strict_mode_config['unindexed_filtering_update']
    assert new_strict_mode_config['search_max_hnsw_ef'] == 3
    assert not new_strict_mode_config['search_allow_exact']
    assert new_strict_mode_config['search_max_oversampling'] == 1.5


def test_patch_collection_partially(collection_name):
    assert not strict_mode_enabled(collection_name)

    set_strict_mode(collection_name,{
        "enabled": True,
        "max_query_limit": 10,
        "search_max_oversampling": 1.5,
    })

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )

    assert response.ok
    new_strict_mode_config = response.json()['result']['config']['strict_mode_config']
    assert new_strict_mode_config['enabled']
    assert new_strict_mode_config['max_query_limit'] == 10
    assert new_strict_mode_config['search_max_oversampling'] == 1.5


def test_strict_mode_query_limit_validation(collection_name):
    def search_request():
        return request_with_validation(
            api='/collections/{collection_name}/points/search',
            method="POST",
            path_params={'collection_name': collection_name},
            body={
                "vector": [0.2, 0.1, 0.9, 0.7],
                "limit": 4
            }
        )

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "max_query_limit": 4,
    })

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "max_query_limit": 3,
    })

    search_fail = search_request()

    assert "limit" in search_fail.json()['status']['error']
    assert not search_fail.ok


def test_strict_mode_timeout_validation(collection_name):
    def search_request_with_timeout(timeout):
        return request_with_validation(
            api='/collections/{collection_name}/points/search',
            method="POST",
            path_params={'collection_name': collection_name},
            query_params={'timeout': timeout},
            body={
                "vector": [0.2, 0.1, 0.9, 0.7],
                "limit": 3
            }
        )

    search_request_with_timeout(3).raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "max_timeout": 2,
    })

    search_request_with_timeout(2).raise_for_status()

    search_fail = search_request_with_timeout(3)

    assert "timeout" in search_fail.json()['status']['error']
    assert not search_fail.ok


def test_strict_mode_unindexed_filter_read_validation(collection_name):
    def search_request_with_filter():
        return request_with_validation(
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
                                "value": "Berlin"
                            }
                        }
                    ]
                },
            }
        )

    search_request_with_filter().raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "unindexed_filtering_retrieve": True,
    })

    search_request_with_filter().raise_for_status()

    set_strict_mode(collection_name, {
        "unindexed_filtering_retrieve": False,
    })

    search_fail = search_request_with_filter()

    assert "city" in search_fail.json()['status']['error']
    assert not search_fail.ok

    request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": "city",
            "field_schema": "keyword"
        }
    ).raise_for_status()

    # We created an index on this field so it should work now
    search_request_with_filter().raise_for_status()


def test_strict_mode_unindexed_filter_write_validation(collection_name):
    def update_request_with_filter():
        return request_with_validation(
            api='/collections/{collection_name}/points/delete',
            method="POST",
            path_params={'collection_name': collection_name},
            query_params={'wait': 'true'},
            body={
                "filter": {
                    "must": [
                        {
                            "key": "city",
                            "match": {
                                "value": "Berlin"
                            }
                        }
                    ]
                }
            })

    update_request_with_filter().raise_for_status()

    # Reset any changes
    basic_collection_setup(collection_name=collection_name)

    set_strict_mode(collection_name, {
        "enabled": True,
        "unindexed_filtering_update": True,
    })

    update_request_with_filter().raise_for_status()

    set_strict_mode(collection_name, {
        "unindexed_filtering_update": False,
    })

    search_fail = update_request_with_filter()

    assert "city" in search_fail.json()['status']['error']
    assert not search_fail.ok

    request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "field_name": "city",
            "field_schema": "keyword"
        }
    ).raise_for_status()

    # We created an index on this field so it should work now
    update_request_with_filter().raise_for_status()


def test_strict_mode_max_ef_hnsw_validation(collection_name):
    def search_request():
        return request_with_validation(
            api='/collections/{collection_name}/points/search',
            method="POST",
            path_params={'collection_name': collection_name},
            body={
                "vector": [0.2, 0.1, 0.9, 0.7],
                "limit": 4,
                "params": {
                    "hnsw_ef": 5,
                }
            }
        )

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "search_max_hnsw_ef": 5,
    })

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "search_max_hnsw_ef": 4,
    })

    search_fail = search_request()

    assert "hnsw_ef" in search_fail.json()['status']['error']
    assert not search_fail.ok


def test_strict_mode_allow_exact_validation(collection_name):
    def search_request():
        return request_with_validation(
            api='/collections/{collection_name}/points/search',
            method="POST",
            path_params={'collection_name': collection_name},
            body={
                "vector": [0.2, 0.1, 0.9, 0.7],
                "limit": 4,
                "params": {
                    "exact": True,
                }
            }
        )

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "search_allow_exact": True,
    })

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "search_allow_exact": False,
    })

    search_fail = search_request()

    assert "exact" in search_fail.json()['status']['error'].lower()
    assert not search_fail.ok


def test_strict_mode_search_max_oversampling_validation(collection_name):
    def search_request():
        return request_with_validation(
            api='/collections/{collection_name}/points/search',
            method="POST",
            path_params={'collection_name': collection_name},
            body={
                "vector": [0.2, 0.1, 0.9, 0.7],
                "limit": 4,
                "params": {
                    "quantization": {
                        "oversampling": 2.0,
                    }
                }
            }
        )

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "search_max_oversampling": 2.0,
    })

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "search_max_oversampling": 1.9,
    })

    search_fail = search_request()

    assert "oversampling" in search_fail.json()['status']['error']
    assert not search_fail.ok


def test_strict_mode_upsert_max_batch_size(collection_name):
    def search_request():
        return request_with_validation(
            api='/collections/{collection_name}/points',
            method="PUT",
            path_params={'collection_name': collection_name},
            body={
                "batch": {
                    "ids": [1, 2, 3, 4, 5, 6],
                    "payloads": [{}, {}, {}, {}, {}, {}],
                    "vectors": [
                        [1, 2, 3, 5],
                        [1, 2, 3, 5],
                        [1, 2, 3, 5],
                        [1, 2, 3, 5],
                        [1, 2, 3, 5],
                        [1, 2, 3, 5]
                    ]
                }
            }
        )

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "upsert_max_batchsize": 6,
    })

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "upsert_max_batchsize": 5,
    })

    search_fail = search_request()

    assert "upsert" in search_fail.json()['status']['error']
    assert not search_fail.ok


def test_strict_mode_update_many_upsert_max_batch_size(collection_name):
    def search_request():
        return request_with_validation(
            api='/collections/{collection_name}/points/batch',
            method="POST",
            path_params={'collection_name': collection_name},
            body={
                "operations": [
                    {
                        "upsert": {
                            "batch": {
                                "ids": [1, 2, 3, 4, 5, 6],
                                "payloads": [{}, {}, {}, {}, {}, {}],
                                "vectors": [
                                    [1, 2, 3, 5],
                                    [1, 2, 3, 5],
                                    [1, 2, 3, 5],
                                    [1, 2, 3, 5],
                                    [1, 2, 3, 5],
                                    [1, 2, 3, 5]
                                ]
                            }
                        }
                    }
                ]
            }
        )

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "upsert_max_batchsize": 6,
    })

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "upsert_max_batchsize": 5,
    })

    search_fail = search_request()

    assert "upsert" in search_fail.json()['status']['error']
    assert not search_fail.ok


def test_strict_mode_update_vectors_max_batch_size(collection_name):
    def search_request():
        return request_with_validation(
            api='/collections/{collection_name}/points/vectors',
            method="PUT",
            path_params={'collection_name': collection_name},
            body={
                "points": [
                    {
                        "id": 1,
                        "vector": [1, 2, 3, 5],
                    },
                    {
                        "id": 2,
                        "vector": [1, 2, 3, 5],
                    },
                    {
                        "id": 3,
                        "vector": [1, 2, 3, 5],
                    },
                    {
                        "id": 4,
                        "vector": [1, 2, 3, 5],
                    },
                ]
            }
        )

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "upsert_max_batchsize": 4,
    })

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "upsert_max_batchsize": 3,
    })

    search_fail = search_request()

    assert "update limit" in search_fail.json()['status']['error']
    assert not search_fail.ok


def test_strict_mode_max_collection_size_upsert(collection_name):
    basic_collection_setup(collection_name=collection_name)  # Clear collection to not depend on other tests

    def upsert_points(ids: list[int]):
        length = len(ids)
        payloads = [{} for _ in range(length)]
        vectors = [[1, 2, 3, 5] for _ in range(length)]
        return request_with_validation(
            api='/collections/{collection_name}/points',
            method="PUT",
            path_params={'collection_name': collection_name},
            body={
                "batch": {
                    "ids": ids,
                    "payloads": payloads,
                    "vectors": vectors
                }
            }
        )

    # Overwriting the same points to trigger cache refreshing
    for _ in range(32):
        upsert_points([1, 2, 3, 4, 5]).raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "max_collection_vector_size_bytes": 240,
    })

    for _ in range(32):
        upsert_points([6, 7, 8, 9, 10]).raise_for_status()

    # Max limit has been reached and one of the next requests must fail. Due to cache it might not be the first call!
    for _ in range(32):
        failed_upsert = upsert_points([12, 13, 14, 15, 16])
        if failed_upsert.ok:
            continue
        assert "Max vector storage size" in failed_upsert.json()['status']['error']
        assert not failed_upsert.ok
        return

    assert False, "Upserting should have failed but didn't"


def test_strict_mode_max_sparse_length_upsert(collection_name):
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
            "sparse_vectors": {
                "sparse-vector": {}
            }
        }
    )
    assert response.ok

    set_strict_mode(collection_name, {
        "enabled": True,
        "sparse_config": {
            "sparse-vector": {
                "max_length": 4
            }
        }
    })

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
                        "sparse-vector": {
                            "indices": [1, 2, 3, 4],
                            "values": [0.0, 0.1, 0.2, 0.3]
                        }
                    }
                }
            ]
        }
    )
    assert response.ok

    failed_upsert = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 2,
                    "vector": {
                        "sparse-vector": {
                            "indices": [1, 2, 3, 4, 5, 6],
                            "values": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]
                        }
                    }
                }
            ]
        }
    )
    assert not failed_upsert.ok
    assert "Sparse vector 'sparse-vector' has a limit of 4 indices" in failed_upsert.json()['status']['error']


def test_strict_mode_max_collection_size_upsert_batch(collection_name):
    basic_collection_setup(collection_name=collection_name)  # Clear collection to not depend on other tests

    def upsert_points(ids: list[int]):
        length = len(ids)
        payloads = [{} for _ in range(length)]
        vectors = [[1, 2, 3, 5] for _ in range(length)]
        return request_with_validation(
            api='/collections/{collection_name}/points/batch',
            method="POST",
            path_params={'collection_name': collection_name},
            body={
                "operations": [
                    {
                        "upsert": {
                            "batch": {
                                "ids": ids,
                                "payloads": payloads,
                                "vectors": vectors
                            }
                        }
                    }
                ]
            }
        )

    for _ in range(32):
        upsert_points([1, 2, 3, 4, 5]).raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "max_collection_vector_size_bytes": 240,
    })

    for _ in range(32):
        upsert_points([6, 7, 8, 9, 10]).raise_for_status()

    # Max limit has been reached and one of the next requests must fail. Due to cache it might not be the first call!
    for _ in range(32):
        failed_upsert = upsert_points([12, 13, 14, 15, 16])
        if failed_upsert.ok:
            continue
        assert "Max vector storage size" in failed_upsert.json()['status']['error']
        assert not failed_upsert.ok
        return

    assert False, "Upserting should have failed but didn't"

def test_strict_mode_max_multivector_size_upsert(collection_name):
    # Clear collection to not depend on other tests
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
                "dense-multi": {
                    "size": 4,
                    "distance": "Dot",
                    "multivector_config": {
                        "comparator": "max_sim"
                    }
                },
            },
        }
    )
    assert response.ok

    set_strict_mode(collection_name, {
        "enabled": True,
        "multivector_config": {
            "dense-multi": {
                "max_vectors": 5,
            }
        }
    })

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
                        "dense-multi": [
                            [1.05, 1.61, 1.76, 1.74],
                            [2.05, 2.61, 2.76, 2.74],
                            [3.05, 3.61, 3.76, 3.74]
                        ],
                    }
                }
            ]
        }
    )
    assert response.ok

    # insert multivectors with 6 vectors (points list)
    failed_upsert = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {
                        "dense-multi": [
                            [1.05, 1.61, 1.76, 1.74],
                            [2.05, 2.61, 2.76, 2.74],
                            [3.05, 3.61, 3.76, 3.74],
                            [3.05, 3.61, 3.76, 3.74],
                            [3.05, 3.61, 3.76, 3.74],
                            [3.05, 3.61, 3.76, 3.74],
                        ],
                    }
                }
            ]
        }
    )
    assert not failed_upsert.ok
    assert "Multivector 'dense-multi' has a limit of 5 vectors, but 6 were provided!" in failed_upsert.json()['status']['error']

    # insert multivectors with 6 vectors (points batch)
    failed_upsert = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "batch": {
                "ids": [1],
                "vectors": {
                    "dense-multi" : [
                         [
                            [1.05, 1.61, 1.76, 1.74],
                            [2.05, 2.61, 2.76, 2.74],
                            [3.05, 3.61, 3.76, 3.74],
                            [3.05, 3.61, 3.76, 3.74],
                            [3.05, 3.61, 3.76, 3.74],
                            [3.05, 3.61, 3.76, 3.74],
                        ]
                    ]
                }
            }
        }
    )
    assert not failed_upsert.ok
    assert "Multivector 'dense-multi' has a limit of 5 vectors, but 6 were provided!" in failed_upsert.json()['status']['error']

    # disable strict mode
    set_strict_mode(collection_name, {
        "enabled": False,
        "multivector_config": {
            "dense-multi": {
                "max_vectors": 5,
            }
        }
    })

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "batch": {
                "ids": [1],
                "vectors": {
                    "dense-multi" : [
                        [
                            [1.05, 1.61, 1.76, 1.74],
                            [2.05, 2.61, 2.76, 2.74],
                            [3.05, 3.61, 3.76, 3.74],
                            [3.05, 3.61, 3.76, 3.74],
                            [3.05, 3.61, 3.76, 3.74],
                            [3.05, 3.61, 3.76, 3.74],
                        ]
                    ]
                }
            }
        }
    )
    assert response.ok

def test_strict_mode_read_rate_limiting(collection_name):
    set_strict_mode(collection_name, {
        "enabled": True,
        "read_rate_limit": 1,
    })

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )

    assert response.ok
    new_strict_mode_config = response.json()['result']['config']['strict_mode_config']
    assert new_strict_mode_config['enabled']
    assert new_strict_mode_config['read_rate_limit'] == 1

    failed_count = 0

    for _ in range(10):
        response = request_with_validation(
            api='/collections/{collection_name}/points/search',
            method="POST",
            path_params={'collection_name': collection_name},
            body={
                "vector": [0.2, 0.1, 0.9, 0.7],
                "limit": 4
            }
        )
        if not response.ok:
            failed_count += 1
            assert response.status_code == 429
            assert "Rate limiting exceeded: Read rate limit exceeded, retry later" in response.json()['status']['error']

    # loose check, as the rate limiting might not be exact
    assert failed_count > 5, "Rate limiting did not work"

    set_strict_mode(collection_name, {
        "enabled": False,
    })

    for _ in range(10):
        response = request_with_validation(
            api='/collections/{collection_name}/points/search',
            method="POST",
            path_params={'collection_name': collection_name},
            body={
                "vector": [0.2, 0.1, 0.9, 0.7],
                "limit": 4
            }
        )
        assert response.ok, "Rate limiting should be disabled now"


def test_strict_mode_max_collection_payload_size_upsert(collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_payload=True)  # Clear collection to not depend on other tests

    def upsert_points(ids: list[int]):
        length = len(ids)
        payloads = [{"city": "Berlin"} for _ in range(length)]
        vectors = [[1, 2, 3, 5] for _ in range(length)]
        return request_with_validation(
            api='/collections/{collection_name}/points',
            method="PUT",
            path_params={'collection_name': collection_name},
            query_params={'wait': 'true'},
            body={
                "batch": {
                    "ids": ids,
                    "payloads": payloads,
                    "vectors": vectors
                }
            }
        )

    # Overwriting the same points to trigger cache refreshing
    for _ in range(32):
        upsert_points([1, 2, 3, 4, 5]).raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "max_collection_payload_size_bytes": 45000,
    })

    for _ in range(32):
        upsert_points([6, 7, 8, 9, 10]).raise_for_status()

    # Max limit has been reached and one of the next requests must fail. Due to cache it might not be the first call!
    for i in range(32):
        failed_upsert = upsert_points([12, 13, 14, 15, 16])
        if failed_upsert.ok:
            continue
        assert "Max payload storage size" in failed_upsert.json()['status']['error']
        assert not failed_upsert.ok
        return

    assert False, "Upserting should have failed but didn't"


def test_strict_mode_max_collection_payload_size_upsert_batch(collection_name):
    basic_collection_setup(collection_name=collection_name, on_disk_payload=True)  # Clear collection to not depend on other tests

    def upsert_points(ids: list[int]):
        length = len(ids)
        payloads = [{"city": "Berlin"} for _ in range(length)]
        vectors = [[1, 2, 3, 5] for _ in range(length)]
        return request_with_validation(
            api='/collections/{collection_name}/points/batch',
            method="POST",
            path_params={'collection_name': collection_name},
            body={
                "operations": [
                    {
                        "upsert": {
                            "batch": {
                                "ids": ids,
                                "payloads": payloads,
                                "vectors": vectors
                            }
                        }
                    }
                ]
            }
        )

    for _ in range(32):
        upsert_points([1, 2, 3, 4, 5]).raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "max_collection_payload_size_bytes": 45000,
    })

    for i in range(32):
        upsert_points([6, 7, 8, 9, 10]).raise_for_status()

    # Max limit has been reached and one of the next requests must fail. Due to cache it might not be the first call!
    for i in range(32):
        failed_upsert = upsert_points([12, 13, 14, 15, 16])
        if failed_upsert.ok:
            continue
        assert "Max payload storage size" in failed_upsert.json()['status']['error']
        assert not failed_upsert.ok
        return

    assert False, "Upserting should have failed but didn't"


def test_strict_mode_write_rate_limiting(collection_name):
    set_strict_mode(collection_name, {
        "enabled": True,
        "write_rate_limit": 1,
    })

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )

    assert response.ok
    new_strict_mode_config = response.json()['result']['config']['strict_mode_config']
    assert new_strict_mode_config['enabled']
    assert new_strict_mode_config['write_rate_limit'] == 1

    failed_count = 0

    for _ in range(10):
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
                    },
                ]
            }
        )

        if not response.ok:
            failed_count += 1
            assert response.status_code == 429
            assert "Rate limiting exceeded: Write rate limit exceeded, retry later" in response.json()['status']['error']

    # loose check, as the rate limiting might not be exact
    assert failed_count > 5, "Rate limiting did not work"

    # Disable rate limiting
    set_strict_mode(collection_name, {
        "enabled": False,
    })

    for _ in range(10):
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
                    },
                ]
            }
        )

        assert response.ok, "Rate limiting should be disabled now"


def test_strict_mode_write_rate_limiting_update_op(collection_name):
    set_strict_mode(collection_name, {
        "enabled": True,
        "write_rate_limit": 7,
    })

    request_with_validation(
        api='/collections/{collection_name}/index',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "field_name": "city",
            "field_schema": "keyword",
        }
    ).raise_for_status()

    # This will pass as we still have tokens in the rate limiter. Those will be used by this call.
    request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "payload": {"City": "Not Berlin"},
            "filter": {"must": [{"key": "City", "match": {"value": "Berlin"}}]}
        }
    ).raise_for_status()

    # Not enough tokens left.
    response = request_with_validation(
        api='/collections/{collection_name}/points/payload',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "payload": {"City": "Not London"},
            "filter": {"must": [{"key": "City", "match": {"value": "London"}}]}
        }
    )
    assert response.status_code == 429
    assert "Rate limiting exceeded: Write rate limit exceeded: Operation requires 5 tokens but only" in response.json()['status']['error']


def test_filter_many_conditions(collection_name):
    def search_request(condition_count: int):
        conditions = []
        for i in range(condition_count):
            conditions.append({
                "key": "price",
                "match": {
                    "value": i
                }
            })

        return request_with_validation(
            api='/collections/{collection_name}/points/search',
            method="POST",
            path_params={'collection_name': collection_name},
            body={
                "vector": [0.2, 0.1, 0.9, 0.7],
                "limit": 4,
                "filter": {
                    "must": conditions
                },
            }
        )

    search_request(5).raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "filter_max_conditions": 5,
    })

    search_request(5).raise_for_status()

    search_fail = search_request(6)
    assert "Filter" in search_fail.json()['status']['error']
    assert "limit" in search_fail.json()['status']['error']
    assert not search_fail.ok


def test_filter_large_condition(collection_name):
    def search_request(condition_size: int):
        conditions = [x for x in range(condition_size)]
        return request_with_validation(
            api='/collections/{collection_name}/points/search',
            method="POST",
            path_params={'collection_name': collection_name},
            body={
                "vector": [0.2, 0.1, 0.9, 0.7],
                "limit": 4,
                "filter": {
                    "must": [
                        {
                            "key": "price",
                            "match": {
                                "any": conditions
                            }
                        }
                    ]
                },
            }
        )

    search_request(5).raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "condition_max_size": 5,
    })

    search_request(5).raise_for_status()

    search_fail = search_request(6)
    assert "Condition" in search_fail.json()['status']['error']
    assert "limit" in search_fail.json()['status']['error']
    assert not search_fail.ok


def test_filter_nested_condition(collection_name):
    def search_request(condition_size: int = 2):
        conditions = [x for x in range(condition_size)]

        return request_with_validation(
            api='/collections/{collection_name}/points/search',
            method="POST",
            path_params={'collection_name': collection_name},
            body={
                "vector": [0.2, 0.1, 0.9, 0.7],
                "limit": 4,
                "filter": {
                    "must": [
                        {
                            "nested": {
                                "key": "city",
                                "filter": {
                                    "must": [
                                        {
                                            "key": "key",
                                            "match": {
                                                "any": conditions,
                                            }
                                        },
                                        {
                                            "key": "key2",
                                            "match": {
                                                "any": conditions,
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                },
            }
        )

    search_request(2).raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "condition_max_size": 2,
    })

    search_request(2).raise_for_status()

    search_fail = search_request(3)
    assert "Condition" in search_fail.json()['status']['error']
    assert "limit" in search_fail.json()['status']['error']
    assert not search_fail.ok

    set_strict_mode(collection_name, {
        "enabled": True,
        "condition_max_size": 1000000,  # Disabled
        "filter_max_conditions": 3,
    })

    search_request().raise_for_status()

    set_strict_mode(collection_name, {
        "enabled": True,
        "condition_max_size": 1000000,  # Disabled
        "filter_max_conditions": 1,
    })

    search_fail = search_request()
    assert "condition" in search_fail.json()['status']['error']
    assert "limit" in search_fail.json()['status']['error']
    assert not search_fail.ok
