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
