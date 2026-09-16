"""Exercise snapshot gates over real HTTP sockets without a Qdrant binary."""

from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import Empty, Queue
import socket
from threading import Event, Thread
from types import SimpleNamespace

import pytest
import requests

from .peer_proxy import PeerProxy, RequestGate


TIMEOUT = 5
PAYLOAD = b"\x00\xffsnapshot" * (128 * 1024)


@pytest.fixture
def snapshot_source():
    calls = Queue()
    disconnected = Event()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            calls.put((self.path, dict(self.headers)))
            self.send_response(404 if self.path == "/missing" else 200)
            self.send_header("Content-Type", "application/octet-stream")
            if self.path == "/chunked" or self.path == "/blocked":
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                if self.path == "/blocked":
                    self.connection.settimeout(TIMEOUT)
                    if self.connection.recv(1) == b"":
                        disconnected.set()
                else:
                    for part in (PAYLOAD[:100], PAYLOAD[100:]):
                        self.wfile.write(f"{len(part):x}\r\n".encode() + part + b"\r\n")
                    self.wfile.write(b"0\r\n\r\n")
            else:
                self.send_header("Content-Length", str(len(PAYLOAD)))
                self.end_headers()
                self.wfile.write(PAYLOAD)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = False
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05})
    thread.start()
    try:
        yield SimpleNamespace(uri=f"http://127.0.0.1:{server.server_port}", calls=calls, disconnected=disconnected)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(TIMEOUT)


def download(proxy, url, **kwargs):
    with requests.Session() as client:
        client.trust_env = False
        return client.get(url, proxies={"http": proxy.http_uri}, timeout=TIMEOUT, **kwargs)


@pytest.mark.parametrize("path", ["/snapshot?key=value", "/chunked", "/missing"])
def test_snapshot_proxy_preserves_response_and_request_headers(snapshot_source, path):
    with PeerProxy("127.0.0.1:1") as proxy:
        response = download(proxy, snapshot_source.uri + path, headers={"api-key": "test-key"})
        assert response.status_code == (404 if path == "/missing" else 200)
        assert response.headers["Content-Type"] == "application/octet-stream"
        assert response.content == PAYLOAD
        received_path, headers = snapshot_source.calls.get(timeout=TIMEOUT)
        assert received_path == path
        assert headers["api-key"] == "test-key"


def test_snapshot_proxy_holds_one_exact_url_and_allows_later_downloads(snapshot_source):
    selected_url = snapshot_source.uri + "/collections/test/shards/0/snapshot"
    with PeerProxy("127.0.0.1:1") as proxy, ThreadPoolExecutor() as executor:
        with proxy.hold_snapshot_download(snapshot_source.uri, "test", 0) as gate:
            held = executor.submit(download, proxy, selected_url)
            assert isinstance(gate, RequestGate)
            assert gate.wait_for_request(TIMEOUT) == selected_url
            with pytest.raises(Empty):
                snapshot_source.calls.get_nowait()
            assert download(proxy, selected_url + "?other=request").content == PAYLOAD
            assert snapshot_source.calls.get(timeout=TIMEOUT)[0].endswith("?other=request")
            assert download(proxy, selected_url).content == PAYLOAD
            snapshot_source.calls.get(timeout=TIMEOUT)
            assert not held.done()
            gate.release()
            assert held.result(TIMEOUT).content == PAYLOAD


def test_snapshot_proxy_does_not_forward_cancelled_held_download(snapshot_source):
    url = snapshot_source.uri + "/collections/test/shards/0/snapshot"
    with PeerProxy("127.0.0.1:1") as proxy, proxy.hold_snapshot_download(snapshot_source.uri, "test", 0) as gate:
        with socket.create_connection(("127.0.0.1", proxy.http_port), timeout=TIMEOUT) as caller:
            caller.sendall(f"GET {url} HTTP/1.1\r\nHost: ignored\r\n\r\n".encode())
            gate.wait_for_request(TIMEOUT)
        assert gate.cancelled.wait(TIMEOUT)
        gate.release()
        with pytest.raises(Empty):
            snapshot_source.calls.get_nowait()


def test_snapshot_proxy_does_not_forward_expired_held_download(snapshot_source):
    url = snapshot_source.uri + "/collections/test/shards/0/snapshot"
    with PeerProxy("127.0.0.1:1") as proxy, ThreadPoolExecutor() as executor:
        with proxy.hold_snapshot_download(snapshot_source.uri, "test", 0) as gate, requests.Session() as client:
            client.trust_env = False
            held = executor.submit(client.get, url, proxies={"http": proxy.http_uri}, timeout=1)
            gate.wait_for_request(TIMEOUT)
            with pytest.raises(requests.Timeout):
                held.result(TIMEOUT)
            assert gate.cancelled.wait(TIMEOUT)
            gate.release()
            with pytest.raises(Empty):
                snapshot_source.calls.get_nowait()


@pytest.mark.parametrize("close_proxy", [False, True], ids=["caller-disconnect", "proxy-shutdown"])
def test_snapshot_proxy_closes_upstream_download(snapshot_source, close_proxy):
    with PeerProxy("127.0.0.1:1") as proxy:
        response = download(proxy, snapshot_source.uri + "/blocked", stream=True)
        try:
            if close_proxy:
                proxy.close()
            else:
                response.close()
            assert snapshot_source.disconnected.wait(TIMEOUT)
        finally:
            response.close()


def test_snapshot_proxy_shutdown_cancels_held_download(snapshot_source):
    url = snapshot_source.uri + "/collections/test/shards/0/snapshot"
    with PeerProxy("127.0.0.1:1") as proxy, proxy.hold_snapshot_download(snapshot_source.uri, "test", 0) as gate:
        with socket.create_connection(("127.0.0.1", proxy.http_port), timeout=TIMEOUT) as caller:
            caller.sendall(f"GET {url} HTTP/1.1\r\nHost: ignored\r\n\r\n".encode())
            gate.wait_for_request(TIMEOUT)
            proxy.close()
            assert caller.recv(1) == b""
        assert gate.cancelled.is_set()
        assert not proxy._thread.is_alive()
        with pytest.raises(Empty):
            snapshot_source.calls.get_nowait()


def test_snapshot_proxy_reports_missing_download_and_removes_unused_gate(snapshot_source):
    url = snapshot_source.uri + "/collections/test/shards/0/snapshot"
    with PeerProxy("127.0.0.1:1") as proxy:
        with pytest.raises(ValueError, match="test failed"):
            with proxy.hold_snapshot_download(snapshot_source.uri, "test", 0) as gate:
                with pytest.raises(TimeoutError, match="No request reached the gate"):
                    gate.wait_for_request(timeout=0)
                raise ValueError("test failed")
        assert download(proxy, url).content == PAYLOAD


def test_snapshot_proxy_releases_download_after_test_error(snapshot_source):
    url = snapshot_source.uri + "/collections/test/shards/0/snapshot"
    with PeerProxy("127.0.0.1:1") as proxy, ThreadPoolExecutor() as executor:
        with pytest.raises(ValueError, match="test failed"):
            with proxy.hold_snapshot_download(snapshot_source.uri, "test", 0) as gate:
                held = executor.submit(download, proxy, url)
                gate.wait_for_request(TIMEOUT)
                raise ValueError("test failed")
        assert held.result(TIMEOUT).content == PAYLOAD


@pytest.mark.parametrize("url", ["https://127.0.0.1:6333/snapshot", "http://example.com/snapshot"])
def test_snapshot_proxy_rejects_nonlocal_or_https_targets(url):
    with PeerProxy("127.0.0.1:1") as proxy:
        with pytest.raises(ValueError, match="HTTP URL"):
            with proxy.hold_snapshot_download(url, "test", 0):
                pass


def test_snapshot_helper_matches_encoded_collection_and_exact_shard(snapshot_source):
    selected_url = snapshot_source.uri + "/collections/with%20space%2Fslash/shards/2/snapshot"
    other_shard_url = snapshot_source.uri + "/collections/with%20space%2Fslash/shards/1/snapshot"
    with PeerProxy("127.0.0.1:1") as proxy, ThreadPoolExecutor() as executor:
        with proxy.hold_snapshot_download(snapshot_source.uri + "/", "with space/slash", 2) as gate:
            assert download(proxy, other_shard_url).content == PAYLOAD
            snapshot_source.calls.get(timeout=TIMEOUT)
            held = executor.submit(download, proxy, selected_url)
            assert gate.wait_for_request(TIMEOUT) == selected_url
            assert not held.done()
            gate.release()
            assert held.result(TIMEOUT).content == PAYLOAD
