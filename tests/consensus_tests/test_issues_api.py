import pytest
import requests

from .fixtures import create_collection, drop_collection, upsert_random_points
from .utils import kill_all_processes, start_cluster

COLL_NAME = "test_collection"


@pytest.fixture(scope="module")
def setup(tmp_path_factory: pytest.TempPathFactory):
    extra_env = {
        "QDRANT_SLOW_SEARCH_THRESHOLD": "1", # "Always" try to trigger slow search issue
    }

    tmp_path = tmp_path_factory.mktemp("qdrant")

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path=tmp_path, num_peers=1, extra_env=extra_env
    )

    uri = peer_api_uris[0]

    yield uri

    kill_all_processes()


@pytest.fixture
def setup_with_big_collection(setup):
    uri = setup
    
    create_collection(uri, COLL_NAME)

    upsert_random_points(uri, 10000, collection_name=COLL_NAME, with_sparse_vector=False)

    yield uri

    drop_collection(uri, COLL_NAME)


def get_issues(uri):
    response = requests.get(
        f"{uri}/issues",
    )
    assert response.ok
    return response.json()["result"]["issues"]


def search_with_city_filter(uri):
    response = requests.post(
        f"{uri}/collections/{COLL_NAME}/points/search",
        json={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 1000,
            "filter": {"must": [{"key": "city", "match": {"any": ["London", "Moscow"]}}]},
        },
    )
    assert response.ok, response.json()


def test_unindexed_field_is_gone_when_deleting_collection(setup_with_big_collection):
    uri = setup_with_big_collection

    expected_issue_code = "/test_collection/UNINDEXED_FIELD/city"
    issues = get_issues(uri)
    assert expected_issue_code not in [issue["code"] for issue in issues]

    search_with_city_filter(uri)

    issues = get_issues(uri)

    # check the issue is now active
    assert expected_issue_code in [issue["code"] for issue in issues]

    # delete collection
    drop_collection(uri, COLL_NAME)

    # check the issue is not active anymore
    issues = get_issues(uri)
    assert expected_issue_code not in [issue["code"] for issue in issues]


def test_unindexed_field_is_gone_when_indexing(setup_with_big_collection):
    uri = setup_with_big_collection

    expected_issue_code = "/test_collection/UNINDEXED_FIELD/city"

    issues = get_issues(uri)
    assert expected_issue_code not in [issue["code"] for issue in issues]

    search_with_city_filter(uri)

    issues = get_issues(uri)

    # check the issue is now active
    # breakpoint()
    issue_present = False
    for issue in issues:
        if issue["code"] == expected_issue_code:
            issue_present = True
            solution = issue["solution"]["immediate"]["action"]
            timestamp = issue["timestamp"]
            break
    assert issue_present

    # search again
    search_with_city_filter(uri)

    issues = get_issues(uri)

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
        url=uri + solution["uri"],
        json=solution["body"],
    )
    assert response.ok

    # check the issue is not active anymore
    issues = get_issues(uri)
    assert expected_issue_code not in [issue["code"] for issue in issues]

    # search again
    search_with_city_filter(uri)

    # check the issue is not triggered again
    issues = get_issues(uri)
    assert expected_issue_code not in [issue["code"] for issue in issues]
