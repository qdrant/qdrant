from random import random
import subprocess
import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import drop_collection

collection_name = 'test_collection_timeout'
num_vectors = 200000
dims = 1024

@pytest.skip("Too big for CI, requires bfb installed", allow_module_level=True)
@pytest.fixture(autouse=True, scope='module')
def setup():
    drop_collection(collection_name)
    
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": dims,
                "distance": "Euclid",
                "on_disk": True,
            },
            "optimizers_config": {
                "indexing_threshold": 0,
            }
        }
    )
    assert response.ok
    
    subprocess.run(['bfb', '--collection-name', collection_name, '--skip-create', '--dim', str(dims), '--num-vectors', str(num_vectors), '--keywords', '300'])
    
    yield
    drop_collection(collection_name=collection_name)

def test_search_timeout():
    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'timeout': 1},
        body={
            "vector": [random() for _ in range(dims)],
            "limit": 100000,
            "filter": {
                "must": [
                    {
                        "key": "a",
                        "match": {
                        "text": "keyword_1"
                        }      
                    }
                ]
            },
            "params": { "exact": False }
        }
    )
    
    assert not response.ok
    assert response.status_code == 500
    assert response.json()['status']['error'].__contains__("Timeout error: Operation 'Search' timed out after 1 seconds")

def test_search_batch_timeout():
    response = request_with_validation(
        api='/collections/{collection_name}/points/search/batch',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'timeout': 1},
        body={
            "searches": [
                {
                    "vector": [0.5] * dims,
                    "limit": 100000,
                    "filter": {
                        "must": [
                            {
                                "key": "a",
                                "match": {
                                "text": "keyword_1"
                                }      
                            }
                        ]
                    },
                },
                {
                    "vector": [0.6] * dims,
                    "limit": 10000,
                },
            ]
        }
    )
    
    assert not response.ok
    assert response.status_code == 500
    assert response.json()['status']['error'].__contains__("Timeout error: Operation 'Search' timed out after 1 seconds")
    
    
def test_search_groups_timeout():
    response = request_with_validation(
        api='/collections/{collection_name}/points/search/groups',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'timeout': 1},
        body={
            "vector": [0.5] * dims,
            "limit": 100,
            "group_by": "a",
            "filter": {"must": [{"key": "a", "match": {"value": "keyword_1"}}]},
            "group_size": 3,
        }
    )
    
    assert not response.ok
    assert response.status_code == 408
    assert response.json()['status']['error'].__contains__("Timeout error: Operation 'GroupBy' timed out after 1 seconds")
    

def test_recommend_timeout():
    response = request_with_validation(
        api='/collections/{collection_name}/points/recommend',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'timeout': 1},
        body={
            "positive": [111,222,333,444,555],
            "negative": [666,777,888,999,1010],
            "limit": 10000,
            "strategy": "best_score",
        }
    )
    
    assert not response.ok
    assert response.status_code == 500
    assert response.json()['status']['error'].__contains__("Timeout error: Operation 'Search' timed out after 1 seconds")
    
    
def test_recommend_batch_timeout():
    response = request_with_validation(
        api='/collections/{collection_name}/points/recommend/batch',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'timeout': 1},
        body={
            "searches": [
                {
                    "positive": [111,222,333,444,555],
                    "negative": [666,777,888,999,1010],
                    "limit": 10000,
                    "strategy": "best_score",
                },
                {
                    "positive": [666,777,888,999,1010],
                    "negative": [111,222,333,444,555],
                    "limit": 10000,
                    "strategy": "best_score",
                },
            ]
        }
    )
    
    assert not response.ok
    assert response.status_code == 500
    assert response.json()['status']['error'].__contains__("Timeout error: Operation 'Search' timed out after 1 seconds")
    

def test_recommend_groups_timeout():
    response = request_with_validation(
        api='/collections/{collection_name}/points/recommend/groups',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'timeout': 1},
        body={
            "positive": [111,555],
            "negative": [666,1010],
            "limit": 100,
            "filter": {"must": [{"key": "a", "match": {"value": "keyword_1"}}]},
            "group_by": "a",
            "group_size": 3,
        }
    )
    
    assert not response.ok
    assert response.status_code == 408
    assert response.json()['status']['error'].__contains__("Timeout error: Operation 'GroupBy' timed out after 1 seconds")
    
    
def test_discover_timeout():
    response = request_with_validation(
        api='/collections/{collection_name}/points/discover',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'timeout': 1},
        body={
            "target": 111,
            "context_pairs": [[666,777],[888,999],[1010, 1111]],
            "limit": 100,
        }
    )
    
    assert not response.ok
    assert response.status_code == 500
    assert response.json()['status']['error'].__contains__("Timeout error: Operation 'Search' timed out after 1 seconds")
