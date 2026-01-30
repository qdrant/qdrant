import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import basic_collection_setup, drop_collection

  
@pytest.fixture(autouse=True)
def setup(collection_name):
    drop_collection(collection_name)
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 4,
                "distance": "Dot",
            },
        }
    )
    assert response.ok
    yield
    drop_collection(collection_name=collection_name)


def get_queue_info(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
        
    return response.json()["result"]["update_queue"]
    

def test_queue_op_num(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok
        
    queue_info = get_queue_info(collection_name)
    
    # empty collection
    assert queue_info["length"] == 0
    assert queue_info["op_num"] == 0
    
    # apply first update
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
                    "payload": {}
                }
            ]}        
    )
    assert response.ok
    
    queue_info = get_queue_info(collection_name)
    
    # wait=true so ack. after application
    assert queue_info["length"] == 0
    assert queue_info["op_num"] == 1
    
    # apply second update
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
                    "payload": {}
                }
            ]}        
    )
    assert response.ok
    
    queue_info = get_queue_info(collection_name)
    # wait=true so ack. after application
    assert queue_info["length"] == 0
    assert queue_info["op_num"] == 2