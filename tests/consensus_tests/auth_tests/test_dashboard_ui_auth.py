"""
Web UI username/password authentication.

When UI auth is enabled, static dashboard assets must not be served until the
user signs in. Login and credential management endpoints stay reachable without
an API key because the /dashboard prefix remains API-key-whitelisted.
"""

import pytest
import requests
from consensus_tests.utils import kill_all_processes

from .utils import REST_URI, start_jwt_protected_cluster


@pytest.fixture(scope="module")
def dashboard_ui_auth_cluster(tmp_path_factory: pytest.TempPathFactory):
    tmp_path = tmp_path_factory.mktemp("dashboard_ui_auth")
    static_dir = tmp_path_factory.mktemp("static_content_ui_auth")
    (static_dir / "index.html").write_text(
        "<!doctype html><title>dashboard</title><body>secret-ui</body>"
    )

    peer_api_uris, peer_dirs, bootstrap_uri = start_jwt_protected_cluster(
        tmp_path,
        extra_env={
            "QDRANT__SERVICE__ENABLE_STATIC_CONTENT": "true",
            "QDRANT__SERVICE__STATIC_CONTENT_DIR": str(static_dir),
            "QDRANT__SERVICE__UI_USERNAME": "tester",
            "QDRANT__SERVICE__UI_PASSWORD": "s3cret-pass",
        },
    )

    try:
        yield peer_api_uris, peer_dirs, bootstrap_uri
    finally:
        kill_all_processes()


def test_dashboard_requires_login(dashboard_ui_auth_cluster):
    response = requests.get(f"{REST_URI}/dashboard", allow_redirects=False)
    assert response.status_code == 302
    assert response.headers["Location"].startswith("/dashboard/login")


def test_dashboard_login_grants_access(dashboard_ui_auth_cluster):
    session = requests.Session()
    login = session.post(
        f"{REST_URI}/dashboard/auth/login",
        json={"username": "tester", "password": "s3cret-pass"},
    )
    assert login.status_code == 200, login.text
    assert login.json()["status"] == "ok"

    dashboard = session.get(f"{REST_URI}/dashboard")
    assert dashboard.status_code == 200
    assert "secret-ui" in dashboard.text


def test_dashboard_rejects_invalid_login(dashboard_ui_auth_cluster):
    response = requests.post(
        f"{REST_URI}/dashboard/auth/login",
        json={"username": "tester", "password": "wrong"},
    )
    assert response.status_code == 401


def test_dashboard_logout(dashboard_ui_auth_cluster):
    session = requests.Session()
    login = session.post(
        f"{REST_URI}/dashboard/auth/login",
        json={"username": "tester", "password": "s3cret-pass"},
    )
    assert login.status_code == 200, login.text

    dashboard = session.get(f"{REST_URI}/dashboard")
    assert dashboard.status_code == 200
    assert "Log out" in dashboard.text

    logout = session.post(f"{REST_URI}/dashboard/auth/logout")
    assert logout.status_code == 200
    assert logout.json()["status"] == "ok"

    locked = session.get(f"{REST_URI}/dashboard", allow_redirects=False)
    assert locked.status_code == 302
    assert locked.headers["Location"].startswith("/dashboard/login")
