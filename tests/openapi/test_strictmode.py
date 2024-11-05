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
