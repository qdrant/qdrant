"""Exercise snapshot gates over real HTTP sockets without a Qdrant binary."""

from concurrent.futures import ThreadPoolExecutor
from email.message import Message
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import Empty, Queue
import socket
from threading import Event, Lock, Thread
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
import requests

from .peer_proxy import PeerProxy, RequestGate
from . import peer_proxy


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


def test_snapshot_proxies_have_independent_download_gates(snapshot_source):
    url = snapshot_source.uri + "/collections/test/shards/0/snapshot"
    with (
        PeerProxy("127.0.0.1:1") as first,
        PeerProxy("127.0.0.1:1") as second,
        ThreadPoolExecutor(max_workers=2) as executor,
    ):
        with (
            first.hold_snapshot_download(snapshot_source.uri, "test", 0) as first_gate,
            second.hold_snapshot_download(snapshot_source.uri, "test", 0) as second_gate,
        ):
            first_download = executor.submit(download, first, url, headers={"receiver": "first"})
            second_download = executor.submit(download, second, url, headers={"receiver": "second"})
            assert first_gate.wait_for_request(TIMEOUT) == url
            assert second_gate.wait_for_request(TIMEOUT) == url
            with pytest.raises(Empty):
                snapshot_source.calls.get_nowait()

            first_gate.release()
            assert first_download.result(TIMEOUT).content == PAYLOAD
            assert snapshot_source.calls.get(timeout=TIMEOUT)[1]["receiver"] == "first"
            assert not second_download.done()
            with pytest.raises(Empty):
                snapshot_source.calls.get_nowait()

            second_gate.release()
            assert second_download.result(TIMEOUT).content == PAYLOAD
            assert snapshot_source.calls.get(timeout=TIMEOUT)[1]["receiver"] == "second"


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


@pytest.fixture
def relay_handler(monkeypatch):
    handler = peer_proxy._DownloadHandler.__new__(peer_proxy._DownloadHandler)
    handler.path = "http://127.0.0.1:6333/snapshot"
    handler.headers = Message()
    handler.connection = Mock()
    handler.send_error = Mock()
    handler.server = SimpleNamespace(proxy=SimpleNamespace(
        _closed=Event(), _lock=Lock(), _http_connections=set(),
        _take_gate_and_notify=Mock(return_value=None), http_port=6334,
    ))
    upstream = MagicMock()
    upstream.__enter__.return_value = upstream
    connect = Mock(return_value=upstream)
    monkeypatch.setattr(peer_proxy.socket, "create_connection", connect)
    monkeypatch.setattr(peer_proxy.select, "select", lambda *_: ([upstream], [], []))
    return handler, upstream, connect


@pytest.mark.parametrize("stage", ["connect", "request", "read"])
@pytest.mark.parametrize("error_type", [socket.timeout, ConnectionResetError])
def test_snapshot_proxy_reports_502_before_response_output(relay_handler, caplog, stage, error_type):
    handler, upstream, connect = relay_handler
    operation = {"connect": connect, "request": upstream.sendall, "read": upstream.recv}[stage]
    operation.side_effect = error_type("source failed")

    handler.do_GET()

    handler.send_error.assert_called_once_with(502, "Snapshot source request failed")
    handler.connection.sendall.assert_not_called()
    assert "failed before response output" in caplog.text
    assert handler.close_connection
    assert not handler.server.proxy._http_connections


@pytest.mark.parametrize("failed_write", [1, 2])
@pytest.mark.parametrize("error_type", [socket.timeout, ConnectionResetError])
def test_snapshot_proxy_does_not_append_error_after_partial_write(relay_handler, caplog, failed_write, error_type):
    handler, upstream, connect = relay_handler
    upstream.recv.side_effect = [b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n", b"snapshot data"]
    transmitted = bytearray()

    def partial_send(data):
        if handler.connection.sendall.call_count == failed_write:
            transmitted.extend(data[:5])
            raise error_type("partial write failed")
        transmitted.extend(data)

    handler.connection.sendall.side_effect = partial_send
    handler.do_GET()

    assert transmitted.startswith(b"HTTP/")
    handler.send_error.assert_not_called()
    assert "Snapshot transfer truncated" in caplog.text
    assert "partial write failed" in caplog.text
    connect.assert_called_once_with(("127.0.0.1", 6333), timeout=handler.timeout)
    handler.connection.settimeout.assert_called_once_with(peer_proxy.RELAY_TIMEOUT)
    assert peer_proxy.RELAY_TIMEOUT > handler.timeout
    assert handler.close_connection
    assert not handler.server.proxy._http_connections


def test_snapshot_proxy_shutdown_interrupts_blocked_relay_write(snapshot_source, monkeypatch):
    blocked = Event()
    original_sendall = socket.socket.sendall
    with PeerProxy("127.0.0.1:1") as proxy:
        def blocked_sendall(connection, data, *args):
            if connection.getsockname()[1] == proxy.http_port:
                blocked.set()
                # A socket operation stays blocked until close() shuts it down.
                assert connection.recv(1) == b""
                raise ConnectionResetError("relay closed")
            return original_sendall(connection, data, *args)

        monkeypatch.setattr(socket.socket, "sendall", blocked_sendall)
        with socket.create_connection(("127.0.0.1", proxy.http_port), timeout=TIMEOUT) as caller:
            caller.sendall(f"GET {snapshot_source.uri}/blocked HTTP/1.1\r\nHost: ignored\r\n\r\n".encode())
            assert blocked.wait(TIMEOUT)
            proxy.close()
            assert caller.recv(1) == b""
        assert snapshot_source.disconnected.wait(TIMEOUT)
        assert not proxy._http_connections
