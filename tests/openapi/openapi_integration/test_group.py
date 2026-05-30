import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation

collection_name = "test_collection_groups"
lookup_collection_name = "test_collection_groups_lookup"

POINTS_API = "/collections/{collection_name}/points"
SEARCH_GROUPS_API = "/collections/{collection_name}/points/search/groups"
RECO_GROUPS_API = "/collections/{collection_name}/points/recommend/groups"


def upsert_chunked_docs(collection_name, docs=50, chunks=5):
    points = []
    for doc in range(docs):
        for chunk in range(chunks):
            doc_id = doc
            i = doc * chunks + chunk
            p = {"id": i, "vector": [1.0, 0.0, 0.0, 0.0], "payload": {"docId": doc_id}}
            points.append(p)

    response = request_with_validation(
        api=POINTS_API,
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={"points": points},
    )

    assert response.ok


def upsert_points_with_array_fields(collection_name, docs=3, chunks=5, id_offset=5000):
    points = []
    for doc in range(docs):
        for chunk in range(chunks):
            doc_ids = [f"valid_{doc}", f"valid_too_{doc}"]
            i = doc * chunks + chunk + id_offset
            p = {
                "id": i,
                "vector": [0.0, 1.0, 0.0, 0.0],
                "payload": {"multiId": doc_ids},
            }
            points.append(p)

    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={"points": points},
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
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={"points": points},
    )

    assert response.ok


def upsert_multi_value_payload(collection_name):
    points = [
        {"id": 9000 + i, "vector": [0.0, 0.0, 1.0, 1.0], "payload": {"mkey": ["a"]}}
        for i in range(100)
    ] + [
        {"id": 9100 + i, "vector": [0.0, 0.0, 1.0, 0.0], "payload": {"mkey": ["a", "b"]}}
        for i in range(10)
    ]

    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={"points": points},
    )

    assert response.ok


def upsert_doc_points(collection_name, docs=50):
    points = [
        {"id": i, "vector": [1.0, 0.0, 0.0, 0.0], "payload": {"body": f"doc body {i}"}}
        for i in range(100)
    ]

    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={"points": points},
    )

    assert response.ok


@pytest.fixture(autouse=True, scope="module")
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    upsert_chunked_docs(collection_name=collection_name)
    upsert_points_with_array_fields(collection_name=collection_name)
    upsert_with_heterogenous_fields(collection_name=collection_name)
    upsert_multi_value_payload(collection_name=collection_name)
    basic_collection_setup(collection_name=lookup_collection_name, on_disk_vectors=on_disk_vectors)
    upsert_doc_points(collection_name=lookup_collection_name)
    yield
    drop_collection(collection_name=collection_name)
    drop_collection(collection_name=lookup_collection_name)


def test_search_with_multiple_groups():
    response = request_with_validation(
        api="/collections/{collection_name}/points/search/groups",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [0.0, 0.0, 1.0, 1.0],
            "limit": 2,
            "with_payload": True,
            "group_by": "mkey",
            "group_size": 2,
        },
    )
    assert response.ok
    groups = response.json()["result"]["groups"]
    assert len(groups) == 2

    assert groups[0]["id"] == "a"
    assert groups[1]["id"] == "b"


def test_search():
    response = request_with_validation(
        api=SEARCH_GROUPS_API,
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [1.0, 0.0, 0.0, 0.0],
            "limit": 10,
            "with_payload": True,
            "group_by": "docId",
            "group_size": 3,
        },
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
        api=RECO_GROUPS_API,
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [5, 10, 15],
            "negative": [6, 11, 16],
            "limit": 10,
            "with_payload": True,
            "group_by": "docId",
            "group_size": 3,
        },
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
        api=SEARCH_GROUPS_API,
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [1.0, 0.0, 0.0, 0.0],
            "limit": 5,
            "with_payload": True,
            "with_vector": True,
            "group_by": "docId",
            "group_size": 3,
        },
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
        api=SEARCH_GROUPS_API,
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [1.0, 0.0, 0.0, 0.0],
            "limit": 10,
            "with_payload": True,
            "with_vector": True,
            "group_by": "inexistentDocId",
            "group_size": 3,
        },
    )
    assert response.ok

    groups = response.json()["result"]["groups"]

    assert len(groups) == 0


def search_array_group_by(group_by: str):
    response = request_with_validation(
        api=SEARCH_GROUPS_API,
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [0.0, 1.0, 0.0, 0.0],
            "limit": 6,
            "with_payload": True,
            "group_by": group_by,
            "group_size": 3,
        },
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
        api=SEARCH_GROUPS_API,
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [0.0, 0.0, 1.0, 0.0],
            "limit": 10,
            "with_payload": True,
            "group_by": "heterogenousId",
            "group_size": 3,
        },
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


lookup_params = [
    pytest.param(lookup_collection_name, id="string name"),
    pytest.param({"collection": lookup_collection_name}, id="only collection name"),
    pytest.param(
        {
            "collection": lookup_collection_name,
            "with_payload": True,
            "with_vectors": False,
        },
        id="explicit with_payload and with_vectors",
    )
]


def assert_group_with_default_lookup(group, group_size=3):
    assert group["hits"]
    assert len(group["hits"]) == group_size

    assert group["lookup"]
    assert group["id"] == group["lookup"]["id"]

    lookup = group["lookup"]
    assert lookup["payload"]
    assert not lookup["vector"]


@pytest.mark.parametrize("with_lookup", lookup_params)
def test_search_groups_with_lookup(with_lookup):
    response = request_with_validation(
        api=SEARCH_GROUPS_API,
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [1.0, 0.0, 0.0, 0.0],
            "limit": 10,
            "with_payload": True,
            "group_by": "docId",
            "group_size": 3,
            "with_lookup": with_lookup,
        },
    )

    assert response.ok

    groups = response.json()["result"]["groups"]

    assert len(groups) == 10
    for group in groups:
        assert_group_with_default_lookup(group, 3)


@pytest.mark.parametrize("with_lookup", lookup_params)
def test_recommend_groups_with_lookup(with_lookup):
    response = request_with_validation(
        api=RECO_GROUPS_API,
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "positive": [5, 10, 15],
            "negative": [6, 11, 16],
            "limit": 10,
            "with_payload": True,
            "group_by": "docId",
            "group_size": 3,
            "with_lookup": with_lookup,
        },
    )

    assert response.ok

    groups = response.json()["result"]["groups"]

    assert len(groups) == 10
    for group in groups:
        assert_group_with_default_lookup(group, 3)

@pytest.mark.parametrize(
    "with_lookup", 
    [
        pytest.param(
            {
                "collection": lookup_collection_name,
                "with_payload": False,
                "with_vectors": False,
            },
            id="with_payload and with_vectors",
        ),
        pytest.param(
            {
                "collection": lookup_collection_name,
                "with_payload": False,
                "with_vector": False,
            },
            id="with_vector is alias of with_vectors",
        ),
    ]
)
def test_search_groups_with_lookup_without_payload_nor_vectors(with_lookup):
    response = request_with_validation(
        api=SEARCH_GROUPS_API,
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [1.0, 0.0, 0.0, 0.0],
            "limit": 10,
            "with_payload": True,
            "group_by": "docId",
            "group_size": 3,
            "with_lookup": with_lookup,
        },
    )

    assert response.ok

    groups = response.json()["result"]["groups"]

    assert len(groups) == 10
    for group in groups:
        assert group["hits"]
        assert len(group["hits"]) == 3

        assert group["lookup"]
        assert group["id"] == group["lookup"]["id"]

        lookup = group["lookup"]
        assert not lookup["payload"]
        assert not lookup["vector"]


def test_search_groups_lookup_with_non_existing_collection():
    non_existing_collection = "non_existing_collection"
    response = request_with_validation(
        api=SEARCH_GROUPS_API,
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [1.0, 0.0, 0.0, 0.0],
            "limit": 10,
            "with_payload": True,
            "group_by": "docId",
            "group_size": 3,
            "with_lookup": {
                "collection": non_existing_collection,
                "with_payload": True,
                "with_vector": True,
            },
        },
    )

    assert response.status_code == 404

    assert (
        f"Collection {non_existing_collection} not found"
        in response.json()["status"]["error"]
    )

def test_search_groups_with_full_lookup():
    response = request_with_validation(
        api=SEARCH_GROUPS_API,
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [1.0, 0.0, 0.0, 0.0],
            "limit": 10,
            "with_payload": True,
            "group_by": "docId",
            "group_size": 3,
            "with_lookup": {
                "collection": lookup_collection_name,
                "with_payload": True,
                "with_vector": True,
            },
        },
    )

    assert response.ok

    groups = response.json()["result"]["groups"]

    assert len(groups) == 10
    for group in groups:
        assert group["hits"]
        assert len(group["hits"]) == 3

        assert group["lookup"]
        assert group["id"] == group["lookup"]["id"]

        lookup = group["lookup"]
        assert lookup["payload"]
        assert lookup["vector"]
