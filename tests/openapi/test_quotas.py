import pytest
import requests

from .helpers.helpers import qdrant_host_headers, request_with_validation
from .helpers.settings import QDRANT_HOST

# Disabled, so the limit is stored but never enforced and other tests sharing
# this server are not affected. The limit is what a bad request must not wipe.
BASELINE = {"enabled": False, "max_disk_usage_percent": 90}


def set_quotas(body: dict) -> requests.Response:
    # Bypass schema validation on purpose: the point is what the server accepts.
    return requests.put(
        f"{QDRANT_HOST}/quotas",
        params={"wait": "true"},
        json=body,
        headers=qdrant_host_headers(),
    )


def get_quota_config() -> dict:
    response = request_with_validation(api="/quotas", method="GET")
    assert response.ok, response.text
    return response.json()["result"]["config"]


@pytest.fixture(autouse=True)
def baseline_quota():
    response = set_quotas(BASELINE)
    assert response.ok, response.text
    yield
    response = set_quotas({"enabled": False})
    assert response.ok, response.text


@pytest.mark.parametrize(
    "body",
    [
        pytest.param({"surelythisiswrong": True}, id="unknown-field-only"),
        pytest.param({"enabled": True, "max_disk_usage": 90}, id="misspelled-limit"),
        pytest.param({}, id="empty-body"),
        pytest.param({"max_disk_usage_percent": 80}, id="missing-enabled"),
    ],
)
def test_update_quotas_rejects_invalid_body(body):
    response = set_quotas(body)
    assert response.status_code == 400, response.text

    # The stored config must be left as it was
    assert get_quota_config() == BASELINE


def test_update_quotas_accepts_valid_body():
    body = {
        "enabled": False,
        "max_resident_memory_percent": 80,
        "max_disk_usage_percent": 70,
        "release_margin_percent": 3,
    }

    response = request_with_validation(
        api="/quotas",
        method="PUT",
        query_params={"wait": "true"},
        body=body,
    )
    assert response.ok, response.text

    assert get_quota_config() == body
