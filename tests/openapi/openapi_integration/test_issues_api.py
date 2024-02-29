import pytest
import requests

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .helpers.helpers import request_with_validation
from .helpers.settings import QDRANT_HOST

collection_name = "test_collection"


@pytest.fixture(autouse=True)
def setup(on_disk_vectors):
    basic_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def get_issues():
    response = request_with_validation(
        api="/issues",
        method="GET",
    )
    assert response.ok
    return response.json()["result"]["issues"]


def search_with_city_filter():
    response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 3,
            "filter": {"must": [{"key": "city", "match": {"any": ["London", "Moscow"]}}]},
        },
    )
    assert response.ok


def test_unindexed_field_is_gone_when_deleting_collection():
    expected_issue_code = "test_collection/UNINDEXED_FIELD/city"
    issues = get_issues()
    assert expected_issue_code not in [issue["code"] for issue in issues]

    search_with_city_filter()

    issues = get_issues()

    # check the issue is now active
    assert expected_issue_code in [issue["code"] for issue in issues]

    # delete collection
    drop_collection(collection_name=collection_name)

    # check the issue is not active anymore
    issues = get_issues()
    assert expected_issue_code not in [issue["code"] for issue in issues]


def test_unindexed_field_is_gone_when_indexing():
    expected_issue_code = "test_collection/UNINDEXED_FIELD/city"

    issues = get_issues()
    assert expected_issue_code not in [issue["code"] for issue in issues]

    search_with_city_filter()

    issues = get_issues()

    # check the issue is now active
    issue_present = False
    for issue in issues:
        if issue["code"] == expected_issue_code:
            issue_present = True
            solution = issue["solution"]["immediate"]["action"]
            timestamp = issue["timestamp"]
            break
    assert issue_present

    # search again
    search_with_city_filter()

    issues = get_issues()

    # check the timestamp has not changed
    issue_present = False
    for issue in issues:
        if issue["code"] == expected_issue_code:
            issue_present = True
            assert timestamp == issue["timestamp"]
            break
    assert issue_present

    # use provided solution to create index
    response = requests.request(
        method=solution["method"],
        url=QDRANT_HOST + solution["uri"],
        json=solution["body"],
    )
    assert response.ok

    # check the issue is not active anymore
    issues = get_issues()
    assert expected_issue_code not in [issue["code"] for issue in issues]

    # search again
    search_with_city_filter()

    # check the issue is not triggered again
    issues = get_issues()
    assert expected_issue_code not in [issue["code"] for issue in issues]


def test_too_many_collections():
    num_collections = 31
    # create 31 collections
    for i in range(num_collections):
        collection_name = f"test_collection_{i}"
        basic_collection_setup(collection_name=collection_name)

    # check the issue is active
    issues = get_issues()
    assert "/TOO_MANY_COLLECTIONS" in [issue["code"] for issue in issues]

    # leave less than 30 collections
    for i in range(num_collections-2, num_collections):
        collection_name = f"test_collection_{i}"
        drop_collection(collection_name=collection_name)

    # check that the issue is not active anymore
    issues = get_issues()
    assert "/TOO_MANY_COLLECTIONS" not in [issue["code"] for issue in issues]

    # Teardown
    for i in range(num_collections):
        collection_name = f"test_collection_{i}"
        drop_collection(collection_name=collection_name)
