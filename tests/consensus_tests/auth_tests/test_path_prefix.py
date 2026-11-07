"""
Authentication with ``service.path_prefix`` configured.

Requests under the prefix are served as if the prefix was stripped by a reverse
proxy, so they must be authenticated exactly like the same request without the
prefix. In particular the dashboard whitelist must not extend to API routes.
"""

import pytest
import requests
from consensus_tests.utils import kill_all_processes

from .utils import API_KEY_HEADERS, REST_URI, start_jwt_protected_cluster

PREFIX = "/qdrant"


@pytest.fixture(scope="module")
def prefixed_cluster(tmp_path_factory: pytest.TempPathFactory):
    tmp_path = tmp_path_factory.mktemp("path_prefix")

    static_dir = tmp_path_factory.mktemp("static_content")
    (static_dir / "index.html").write_text(
        "<!doctype html><title>dashboard</title>"
    )

    peer_api_uris, peer_dirs, bootstrap_uri = start_jwt_protected_cluster(
        tmp_path,
        extra_env={
            "QDRANT__SERVICE__ENABLE_STATIC_CONTENT": "true",
            "QDRANT__SERVICE__STATIC_CONTENT_DIR": str(static_dir),
            "QDRANT__SERVICE__PATH_PREFIX": PREFIX,
        },
    )

    try:
        yield peer_api_uris, peer_dirs, bootstrap_uri
    finally:
        kill_all_processes()


@pytest.mark.parametrize("path", ["/", "/dashboard", "/dashboard/"])
def test_whitelisted_paths_do_not_require_api_key(prefixed_cluster, path):
    response = requests.get(f"{REST_URI}{PREFIX}{path}")
    assert response.status_code == 200, response.text


@pytest.mark.parametrize(
    "path", ["/collections", "/telemetry", "/dashboard%2f..%2fcollections"]
)
def test_api_requires_api_key(prefixed_cluster, path):
    response = requests.get(f"{REST_URI}{PREFIX}{path}")
    assert response.status_code == 401, response.text


def test_api_with_api_key(prefixed_cluster):
    response = requests.get(f"{REST_URI}{PREFIX}/collections", headers=API_KEY_HEADERS)
    assert response.status_code == 200, response.text


def test_unprefixed_api_still_served(prefixed_cluster):
    response = requests.get(f"{REST_URI}/collections", headers=API_KEY_HEADERS)
    assert response.status_code == 200, response.text
