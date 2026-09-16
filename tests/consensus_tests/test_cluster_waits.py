from unittest.mock import Mock

import pytest
import requests

from . import utils


@pytest.mark.parametrize("leader, expected", [(None, False), (0, False), (123, True)])
def test_leader_is_defined(monkeypatch, leader, expected):
    response = Mock(status_code=200)
    response.json.return_value = {"result": {"raft_info": {"leader": leader}}}
    get = Mock(return_value=response)
    monkeypatch.setattr(utils.requests, "get", get)
    headers = {"api-key": "test-key"}

    assert utils.leader_is_defined("http://peer", headers=headers) is expected
    get.assert_called_once_with("http://peer/cluster", headers=headers)


def test_leader_is_undefined_when_peer_is_offline(monkeypatch):
    monkeypatch.setattr(utils.requests, "get", Mock(side_effect=requests.ConnectionError))

    assert not utils.leader_is_defined("http://peer")


@pytest.fixture
def cluster_status(monkeypatch):
    states = {
        f"http://peer{index}": {
            "raft_info": {"leader": 123},
            "peers": {str(peer): {} for peer in range(3)},
        }
        for index in range(3)
    }

    def get(url, headers):
        assert headers == {"api-key": "test-key"}
        state = states[url.removesuffix("/cluster")]
        if isinstance(state, Exception):
            raise state
        response = Mock(status_code=200)
        response.json.return_value = {"result": state}
        return response

    monkeypatch.setattr(utils.requests, "get", get)
    return states


@pytest.mark.parametrize("leaders, expected", [
    ([None, None, None], False),
    ([0, 0, 0], False),
    ([123, None, 123], False),
    ([123, 123, 0], False),
    ([123, 456, 123], False),
    ([123, 123, 123], True),
])
def test_cluster_requires_agreement_on_nonzero_leader(cluster_status, leaders, expected):
    for state, leader in zip(cluster_status.values(), leaders):
        state["raft_info"]["leader"] = leader

    assert utils.all_nodes_cluster_info_consistent(
        list(cluster_status), headers={"api-key": "test-key"},
    ) is expected


@pytest.mark.parametrize("leaders", [[None] * 3, [0] * 3, [123, 123, 456]])
def test_cluster_wait_rechecks_leader_on_each_poll(monkeypatch, cluster_status, leaders):
    for state, leader in zip(cluster_status.values(), leaders):
        state["raft_info"]["leader"] = leader

    def elect_new_leader(_):
        for state in cluster_status.values():
            state["raft_info"]["leader"] = 456

    sleep = Mock(side_effect=elect_new_leader)
    monkeypatch.setattr(utils.time, "sleep", sleep)

    utils.wait_for_uniform_cluster_status(list(cluster_status), headers={"api-key": "test-key"})

    sleep.assert_called_once_with(utils.RETRY_INTERVAL_SEC)


@pytest.mark.parametrize("peer", ["http://peer0", "http://peer1", "http://peer2"])
def test_cluster_wait_retries_offline_peers(cluster_status, peer):
    cluster_status[peer] = requests.ConnectionError()

    assert not utils.all_nodes_cluster_info_consistent(
        list(cluster_status), headers={"api-key": "test-key"},
    )


def test_cluster_wait_still_checks_membership_size(cluster_status):
    cluster_status["http://peer2"]["peers"].pop("2")

    assert not utils.all_nodes_cluster_info_consistent(
        list(cluster_status), headers={"api-key": "test-key"},
    )


@pytest.mark.parametrize("expected_leader, expected", [(123, True), (456, False)])
def test_cluster_wait_preserves_explicit_leader_check(cluster_status, expected_leader, expected):
    assert utils.all_nodes_cluster_info_consistent(
        list(cluster_status), expected_leader, headers={"api-key": "test-key"},
    ) is expected
