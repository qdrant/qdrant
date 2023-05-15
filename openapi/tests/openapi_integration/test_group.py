import json

import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import basic_collection_setup, drop_collection

collection_name = 'test_collection_groups'


def upsert_chunked_docs(collection_name, docs=50, chunks=5):
    points = []
    for doc in range(docs):
        for chunk in range(chunks):
            doc_id = f"doc_{doc}"
            i = doc * chunks + chunk
            p = {"id": i, "vector": [1.0, 0.0, 0.0, 0.0], "payload": {"docId": doc_id}}
            points.append(p)

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"points": points}
    )

    assert response.ok


def upsert_points_with_array_fields(collection_name, docs=3, chunks=5, id_offset=5000):
    points = []
    for doc in range(docs):
        for chunk in range(chunks):
            doc_ids = [f"valid_{doc}", f"valid_too_{doc}"]
            i = doc * chunks + chunk + id_offset
            p = {"id": i, "vector": [0.0, 1.0, 0.0, 0.0], "payload": {"multiId": doc_ids}}
            points.append(p)

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"points": points}
    )

    assert response.ok


def upsert_with_heterogenous_fields(collection_name):
    points = [
        {"id": 6000, "vector": [0.0, 0.0, 1.0, 0.0], "payload": {"heterogenousId": "string"}},  # ok -> string
        {"id": 6001, "vector": [0.0, 0.0, 1.0, 0.0], "payload": {"heterogenousId": 123}},  # ok -> 123
        {"id": 6002, "vector": [0.0, 0.0, 1.0, 0.0], "payload": {"heterogenousId": [1, 2, 3]}},  # ok -> 1
        {"id": 6003, "vector": [0.0, 0.0, 1.0, 0.0], "payload": {"heterogenousId": ["a", "b", "c"]}},  # ok -> "a"
        {"id": 6004, "vector": [0.0, 0.0, 1.0, 0.0], "payload": {"heterogenousId": 2.42}},  # ok -> "2.42"
        {"id": 6005, "vector": [0.0, 0.0, 1.0, 0.0], "payload": {"heterogenousId": [["a", "b", "c"]]}},  # invalid
        {"id": 6006, "vector": [0.0, 0.0, 1.0, 0.0], "payload": {"heterogenousId": {"object": "string"}}},  # invalid
        {"id": 6007, "vector": [0.0, 0.0, 1.0, 0.0], "payload": {"heterogenousId": []}},  # invalid
        {"id": 6008, "vector": [0.0, 0.0, 1.0, 0.0], "payload": {"heterogenousId": None}},  # invalid
    ]

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"points": points}
    )

    assert response.ok


@pytest.fixture(autouse=True, scope="module")
def setup():
    basic_collection_setup(collection_name=collection_name)
    upsert_chunked_docs(collection_name=collection_name)
    upsert_points_with_array_fields(collection_name=collection_name)
    upsert_with_heterogenous_fields(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_search():
    response = request_with_validation(
        api='/collections/{collection_name}/points/search/groups',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [1.0, 0.0, 0.0, 0.0],
            "limit": 10,
            "with_payload": True,
            "group_by": "docId",
            "group_size": 3,
        }
    )
    assert response.ok

    groups = response.json()["result"]["groups"]

    assert len(groups) == 10
    for g in groups:
        assert len(g["hits"]) == 3
        for h in g["hits"]:
            assert h["payload"]["docId"] == g["id"]


def test_recommend():
    response = request_with_validation(
        api='/collections/{collection_name}/points/recommend/groups',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "positive": [5, 10, 15],
            "negative": [6, 11, 16],
            "limit": 10,
            "with_payload": True,
            "group_by": "docId",
            "group_size": 3,
        }
    )
    assert response.ok

    groups = response.json()["result"]["groups"]

    assert len(groups) == 10
    for g in groups:
        assert len(g["hits"]) == 3
        for h in g["hits"]:
            assert h["payload"]["docId"] == g["id"]


def test_with_vectors():
    response = request_with_validation(
        api='/collections/{collection_name}/points/search/groups',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [1.0, 0.0, 0.0, 0.0],
            "limit": 5,
            "with_payload": True,
            "with_vector": True,
            "group_by": "docId",
            "group_size": 3,
        }
    )
    assert response.ok

    groups = response.json()["result"]["groups"]

    assert len(groups) == 5
    for g in groups:
        assert len(g["hits"]) == 3
        for h in g["hits"]:
            assert h["payload"]["docId"] == g["id"]
            assert h["vector"] == [1.0, 0.0, 0.0, 0.0]


def test_inexistent_group_by():
    response = request_with_validation(
        api='/collections/{collection_name}/points/search/groups',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [1.0, 0.0, 0.0, 0.0],
            "limit": 10,
            "with_payload": True,
            "with_vector": True,
            "group_by": "inexistentDocId",
            "group_size": 3,
        }
    )
    assert response.ok

    groups = response.json()["result"]["groups"]

    assert len(groups) == 0

def search_array_group_by(group_by: str):
    response = request_with_validation(
        api='/collections/{collection_name}/points/search/groups',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.0, 1.0, 0.0, 0.0],
            "limit": 6,
            "with_payload": True,
            "group_by": group_by,
            "group_size": 3,
        }
    )
    assert response.ok

    groups = response.json()["result"]["groups"]
    assert len(groups) == 6
    
    group_ids = [g["id"] for g in groups]
    
    for i in range(3):
        assert f"valid_{i}" in group_ids
        assert f"valid_too_{i}" in group_ids
    
def test_multi_value_group_by():
    search_array_group_by("multiId")
    search_array_group_by("multiId[]")


def test_groups_by_heterogenous_fields():
    response = request_with_validation(
        api='/collections/{collection_name}/points/search/groups',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.0, 0.0, 1.0, 0.0],
            "limit": 10,
            "with_payload": True,
            "group_by": "heterogenousId",
            "group_size": 3,
        }
    )
    assert response.ok

    groups = response.json()["result"]["groups"]

    group_ids = [g["id"] for g in groups]

    # Expected group ids are: ['c', 3, 1, 123, 2, 'string', 'b', 'a']

    assert len(groups) == 8
    assert "c" in group_ids
    assert 3 in group_ids
    assert 1 in group_ids
    assert 123 in group_ids
    assert 2 in group_ids
    assert "string" in group_ids
    assert "b" in group_ids
    assert "a" in group_ids
