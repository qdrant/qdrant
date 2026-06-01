"""
Regression test for the dashboard path-whitelist authentication bypass.

The static Web UI is served under ``/dashboard`` and that prefix is whitelisted
from API-key authentication, so the dashboard can load without a key. Crucially,
whitelisting must be decided on the *matched route pattern*, not on the raw
request path. Otherwise a request whose path merely *starts with* ``/dashboard``
— but which does not actually resolve to the dashboard route — would inherit the
whitelist and bypass authentication entirely.

``GET /dashboard%2f..%2fcollections`` is exactly such a request: the raw path
starts with ``/dashboard`` (so the old prefix check whitelisted it), but it does
not match the dashboard route. On a vulnerable build the request is therefore
whitelisted, the API key is never checked, and the request simply falls through
to a 404. On a fixed build the request is not whitelisted, so the missing API
key is rejected up front.

Without an API key the server answers ``401 Unauthorized`` ("Must provide an API
key..."). Note this is 401, not 403: a *missing* credential is an
authentication failure (401), whereas 403 is reserved for a valid credential
with insufficient permissions.

See commit "Don't whitelist endpoints on user provided path, but on endpoint
pattern".
"""

import pytest
import requests
from consensus_tests.utils import kill_all_processes

from .utils import REST_URI, start_jwt_protected_cluster


@pytest.fixture(scope="module")
def dashboard_cluster(tmp_path_factory: pytest.TempPathFactory):
    tmp_path = tmp_path_factory.mktemp("dashboard_whitelist")

    # The /dashboard prefix is only whitelisted when the static Web UI content
    # is actually available (see `web_ui_folder` in src/actix/web_ui.rs). Point
    # the server at a minimal static dir so the dashboard whitelist is genuinely
    # active; otherwise this regression test would pass trivially, because the
    # prefix would never be whitelisted in the first place.
    static_dir = tmp_path_factory.mktemp("static_content")
    (static_dir / "index.html").write_text(
        "<!doctype html><title>dashboard</title>"
    )

    peer_api_uris, peer_dirs, bootstrap_uri = start_jwt_protected_cluster(
        tmp_path,
        extra_env={
            "QDRANT__SERVICE__ENABLE_STATIC_CONTENT": "true",
            "QDRANT__SERVICE__STATIC_CONTENT_DIR": str(static_dir),
        },
    )

    try:
        yield peer_api_uris, peer_dirs, bootstrap_uri
    finally:
        kill_all_processes()


def test_dashboard_prefix_is_whitelisted(dashboard_cluster):
    """Precondition: the dashboard itself loads without an API key.

    This guards the regression test below: if the /dashboard prefix were not
    whitelisted here, the bypass test could not distinguish a fixed build from a
    vulnerable one.
    """
    response = requests.get(f"{REST_URI}/dashboard")
    assert response.status_code == 200, (
        f"expected the whitelisted /dashboard to load without an API key, "
        f"but got HTTP {response.status_code}: {response.text!r}"
    )


def test_dashboard_path_traversal_requires_api_key(dashboard_cluster):
    """`/dashboard%2f..%2fcollections` must not inherit the dashboard whitelist.

    Without an API key it must be rejected with 401 Unauthorized. On a
    vulnerable build the raw-path prefix match whitelists it, the API key is
    never checked, and the request falls through to a 404 — so any non-401
    status (e.g. 404, or 200 leaking collection data) means the authentication
    boundary was bypassed.
    """
    response = requests.get(f"{REST_URI}/dashboard%2f..%2fcollections")
    assert response.status_code == 401, (
        f"expected 401 Unauthorized for GET /dashboard%2f..%2fcollections "
        f"without an API key, but got HTTP {response.status_code}: "
        f"{response.text!r}. A non-401 status means the /dashboard whitelist was "
        f"applied to a path that does not resolve to the dashboard, bypassing "
        f"authentication."
    )
