import http.client
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, urlencode

import pytest

from openapi.helpers.collection_setup import drop_collection
from openapi.helpers.helpers import (
    get_api_string,
    qdrant_host_headers,
    request_with_validation,
    skip_if_no_feature,
)
from openapi.helpers.settings import QDRANT_HOST


class _HttpResponse:
    """Minimal wrapper to match the requests.Response interface used in tests."""

    def __init__(self, resp):
        self.status_code = resp.status
        self.ok = 200 <= resp.status < 300
        self._body = resp.read()

    def json(self):
        return json.loads(self._body)


def _request_with_signal(started, **kwargs):
    api = kwargs["api"]
    path_params = kwargs.get("path_params") or {}
    query_params = kwargs.get("query_params") or {}
    body = kwargs.get("body")

    url = get_api_string(QDRANT_HOST, api, path_params)
    parsed = urlparse(url)

    path = parsed.path
    if query_params:
        path += "?" + urlencode(query_params)

    headers = {**(qdrant_host_headers() or {}), "Content-Type": "application/json"}

    conn = http.client.HTTPConnection(parsed.hostname, parsed.port)
    # http.client.request() sends the request bytes and returns immediately,
    # unlike requests.post() which blocks until the full response arrives.
    # This lets us signal *after* the server received the request but *before*
    # waiting for the response — ensuring the delay lock is acquired before
    # the second request is dispatched.
    conn.request("POST", path, body=json.dumps(body), headers=headers)
    started.set()
    return _HttpResponse(conn.getresponse())


def run_parallel(first_call, second_call):
    started = threading.Event()
    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(_request_with_signal, started, **first_call)
        started.wait(timeout=5)
        second_future = executor.submit(request_with_validation, **second_call)
        return first_future.result(), second_future.result()


@pytest.fixture(autouse=True)
def setup(collection_name):
    drop_collection(collection_name)

    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={"sparse_vectors": {"bm25": {}}},
    )

    assert response.ok

    yield
    drop_collection(collection_name=collection_name)


def test_wait_timeout_ack(collection_name):
    skip_if_no_feature("staging")
    sleep, op = run_parallel(
        {
            "api": "/collections/{collection_name}/debug",
            "method": "POST",
            "path_params": {"collection_name": collection_name},
            "body": {"delay": {"duration_sec": 5.0}},
        },
        {
            "api": "/collections/{collection_name}/points",
            "method": "PUT",
            "path_params": {"collection_name": collection_name},
            "query_params": {"wait": "true", "timeout": 1},
            "body": {
                "points": [
                    {
                        "id": 1,
                        "vector": {
                            "bm25": {
                                "text": "Lorem ipsum, dolor sit amet.",
                                "model": "qdrant/bm25",
                            }
                        },
                    }
                ]
            },
        },
    )
    assert sleep.ok and sleep.json()["result"]["status"] == "acknowledged"
    assert op.ok and op.json()["result"]["status"] == "wait_timeout"


def test_wait_timeout_completed(collection_name):
    skip_if_no_feature("staging")
    sleep, op = run_parallel(
        {
            "api": "/collections/{collection_name}/debug",
            "method": "POST",
            "path_params": {"collection_name": collection_name},
            "query_params": {"wait": "true"},
            "body": {"delay": {"duration_sec": 5.0}},
        },
        {
            "api": "/collections/{collection_name}/points",
            "method": "PUT",
            "path_params": {"collection_name": collection_name},
            "query_params": {"wait": "true", "timeout": 1},
            "body": {
                "points": [
                    {
                        "id": 1,
                        "vector": {
                            "bm25": {
                                "text": "Lorem ipsum, dolor sit amet.",
                                "model": "qdrant/bm25",
                            }
                        },
                    }
                ]
            },
        },
    )
    assert sleep.ok and sleep.json()["result"]["status"] == "completed"
    assert op.ok and op.json()["result"]["status"] == "wait_timeout"


def test_wait_timeout_twice(collection_name):
    skip_if_no_feature("staging")
    sleep, op = run_parallel(
        {
            "api": "/collections/{collection_name}/debug",
            "method": "POST",
            "path_params": {"collection_name": collection_name},
            "query_params": {"wait": "true", "timeout": 5},
            "body": {"delay": {"duration_sec": 10.0}},
        },
        {
            "api": "/collections/{collection_name}/points",
            "method": "PUT",
            "path_params": {"collection_name": collection_name},
            "query_params": {"wait": "true", "timeout": 1},
            "body": {
                "points": [
                    {
                        "id": 1,
                        "vector": {
                            "bm25": {
                                "text": "Lorem ipsum, dolor sit amet.",
                                "model": "qdrant/bm25",
                            }
                        },
                    }
                ]
            },
        },
    )
    assert sleep.ok and sleep.json()["result"]["status"] == "wait_timeout"
    assert op.ok and op.json()["result"]["status"] == "wait_timeout"
