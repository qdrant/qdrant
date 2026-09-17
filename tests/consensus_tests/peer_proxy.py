"""A test-only peer proxy with gates for RPCs and snapshot downloads.

The proxy forwards protobuf bytes unchanged. A gate holds one matching request
before forwarding it. Later requests, including calls to the same method, pass
through so recovery traffic can continue. Matchers run on the server loop and
must not block. They can decode the bytes to select a collection or shard.

RPC bodies do not identify the source peer. Tests must establish which peer
sends the selected request. HTTP gates select the full source URL and intercept
downloads made by the peer configured with this proxy's environment.

Only unary gRPC and bodyless HTTP GETs to local peers are supported, not
streaming RPCs, HTTPS, or a general-purpose HTTP proxy.
"""

import asyncio
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import logging
import select
import socket
from threading import Event, Lock, Thread
from typing import Callable
from urllib.parse import quote, urlsplit

import grpc


logger = logging.getLogger(__name__)
# Snapshot consumers can pause while unpacking or writing data to disk.
RELAY_TIMEOUT = 30


class RequestGate:
    def __init__(self, proxy, key, matches):
        self._key = key
        self.cancelled = Event()
        self._proxy = proxy
        self._matches = matches
        self._arrived = Future()
        self._released = Future()

    def wait_for_request(self, timeout: float = 30):
        """Return the RPC bytes or download URL once held before forwarding.

        This does not prove that the transfer has copied any data.

        With timeout=0, return an already-arrived request or raise TimeoutError
        immediately. This checks the current state without waiting for an
        in-flight request.
        """
        try:
            return self._arrived.result(timeout)
        except FutureTimeoutError as error:
            raise TimeoutError(f"No request reached the gate for {self._key[1]}") from error

    def release(self):
        """Release the held request, or remove a gate that has not been reached."""
        with self._proxy._lock:
            if self._proxy._gates.get(self._key) is self:
                del self._proxy._gates[self._key]
                self._arrived.cancel()
            if not self._released.done():
                self._released.set_result(None)


class PeerProxy(grpc.GenericRpcHandler):
    """Own the peer's RPC and HTTP handlers, gates, and cleanup.

    `target` is the real peer's `host:port`. Advertise `uri` as the peer address
    when wiring a cluster through the proxy. Port zero lets the OS reserve a
    free port without racing pytest workers.

    Pass `env` to the peer so its outgoing snapshot downloads use `http_uri`.
    Use as a context manager. Closing cancels held and forwarded requests and
    joins both listeners and their request handlers. One gate can wait for each
    RPC method or download URL. Later matching requests pass through.

    With the peer configured to advertise the proxy URI, a test can use:

        with proxy.hold_rpc("/qdrant.CollectionsInternal/GetShardRecoveryPoint") as gate:
            replicate_shard(...)
            request = gate.wait_for_request()
            # Check the transfer identity, then remove or stop the source.
            gate.release()

    Reaching this gate proves that the RPC was sent, not that any data was
    copied. The test must separately check membership, shard state, and data.
    """

    def __init__(self, target: str, port: int = 0):
        self._target = target
        self._port = port
        self._gates = {}
        self._lock = Lock()
        self._http_connections = set()
        self._closed = Event()
        self._ready = Future()
        self._thread = Thread(target=self._run, name="consensus-peer-proxy", daemon=True)
        self._thread.start()
        try:
            self.port = self._ready.result(10)
        except BaseException:
            self._thread.join(timeout=10)
            raise
        self.address = f"127.0.0.1:{self.port}"
        self.uri = f"http://{self.address}"

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def wait_for_peer_connection(self, timeout: float = 30):
        """Wait for the internal gRPC connection, including after a restart.

        This does not check peer health, consensus progress, or replica state.
        """
        try:
            self._submit(self._channel.channel_ready(), timeout=timeout)
        except FutureTimeoutError as error:
            raise TimeoutError(
                f"Proxy did not establish a gRPC connection to {self._target} within {timeout} seconds"
            ) from error

    def close(self):
        self._closed.set()
        # Interrupt slow writes instead of waiting for the longer relay timeout.
        with self._lock:
            for connection in self._http_connections:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        if self._thread.is_alive():
            self._loop.call_soon_threadsafe(self._stop.set)
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                raise TimeoutError("Peer proxy did not stop")

    def hold_rpc(self, method: str, matches: Callable[[bytes], bool] = lambda _: True):
        """Hold one matching RPC before the peer receives it."""
        if not method.startswith("/") or method.count("/") != 2:
            raise ValueError("Use the full gRPC method path: /service/method")
        return self._hold(("rpc", method), matches)

    def hold_snapshot_download(self, source_uri: str, collection: str, shard_id: int):
        """Hold this peer's download from the given source, collection, and shard.

        For a shard transfer, the receiver has already cleared its old shard
        when this request arrives. User-triggered URL recovery does not clear it.
        """
        source = _local_http_url(source_uri)
        if source.path not in ("", "/") or source.query:
            raise ValueError("Use the source peer's base HTTP URI without a path or query")
        url = f"{source_uri.rstrip('/')}/collections/{quote(collection, safe='')}/shards/{shard_id}/snapshot"
        return self._hold(("http", url), lambda _: True)

    @contextmanager
    def _hold(self, key, matches):
        with self._lock:
            if self._closed.is_set():
                raise RuntimeError("Peer proxy is closed")
            if key in self._gates:
                raise RuntimeError(f"A gate is already waiting for {key[1]}")
            gate = RequestGate(self, key, matches)
            self._gates[key] = gate
        try:
            yield gate
        finally:
            gate.release()

    def _take_gate(self, key, request):
        with self._lock:
            gate = self._gates.get(key)
            if gate is not None and gate._matches(request):
                # Another peer's recovery must not wait behind this request.
                del self._gates[key]
                gate._arrived.set_result(request)
                return gate
        return None

    def _submit(self, coroutine, timeout: float = 10):
        if not self._thread.is_alive():
            coroutine.close()
            raise RuntimeError("Peer proxy is closed")
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout)
        except FutureTimeoutError:
            future.cancel()
            raise

    def _run(self):
        try:
            asyncio.run(self._serve())
        except BaseException as error:
            if not self._ready.done():
                self._ready.set_exception(error)
            else:
                raise

    async def _serve(self):
        self._loop = asyncio.get_running_loop()
        self._stop = asyncio.Event()
        # Shard batches and Raft snapshots can exceed gRPC's default 4 MiB.
        options = (
            ("grpc.max_receive_message_length", -1),
            ("grpc.max_send_message_length", -1),
            ("grpc.so_reuseport", 0),
        )
        self._channel = grpc.aio.insecure_channel(self._target, options=options)
        self._server = grpc.aio.server(handlers=(self,), options=options)
        http_server = None
        http_thread = None
        try:
            port = self._server.add_insecure_port(f"127.0.0.1:{self._port}")
            await self._server.start()
            http_server = ThreadingHTTPServer(("127.0.0.1", 0), _DownloadHandler)
            # Cleanup must join the HTTP request handlers as well as the listener.
            http_server.daemon_threads = False
            http_server.proxy = self
            self.http_port = http_server.server_port
            self.http_uri = f"http://127.0.0.1:{self.http_port}"
            # Clear inherited bypass rules so local downloads use this peer's gate.
            self.env = {"http_proxy": self.http_uri, "HTTP_PROXY": self.http_uri, "no_proxy": "", "NO_PROXY": ""}
            http_thread = Thread(target=http_server.serve_forever, kwargs={"poll_interval": 0.05})
            http_thread.start()
            self._ready.set_result(port)
            await self._stop.wait()
        finally:
            self._closed.set()
            with self._lock:
                for gate in self._gates.values():
                    gate._arrived.cancel()
                self._gates.clear()
            await self._server.stop(0)
            await self._channel.close()
            if http_server is not None:
                if http_thread is not None:
                    http_server.shutdown()
                http_server.server_close()
                if http_thread is not None:
                    http_thread.join()

    def service(self, handler_call_details):
        async def forward(request, context):
            gate = self._take_gate(("rpc", handler_call_details.method), request)
            if gate is not None:
                try:
                    # RPC cancellation must not cancel the shared release future.
                    await asyncio.shield(asyncio.wrap_future(gate._released))
                except asyncio.CancelledError:
                    gate.cancelled.set()
                    raise

            call = self._channel.unary_unary(handler_call_details.method)(
                request,
                metadata=context.invocation_metadata(),
                timeout=context.time_remaining(),
            )
            try:
                await context.send_initial_metadata(await call.initial_metadata())
                response = await call
                context.set_trailing_metadata(await call.trailing_metadata())
                return response
            except grpc.aio.AioRpcError as error:
                await context.abort(error.code(), error.details(), tuple(error.trailing_metadata()))
            finally:
                # A disconnected caller must not leave work running upstream.
                call.cancel()

        return grpc.unary_unary_rpc_method_handler(forward)


def _local_http_url(url):
    parsed = urlsplit(url)
    if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or not parsed.port
            or parsed.username is not None or parsed.password is not None or parsed.fragment):
        raise ValueError("Use an HTTP URL with an explicit port on 127.0.0.1")
    return parsed


class _DownloadHandler(BaseHTTPRequestHandler):
    timeout = 5

    def log_message(self, *_):
        pass

    def do_GET(self):
        proxy = self.server.proxy
        try:
            target = _local_http_url(self.path)
        except ValueError as error:
            self.send_error(400, str(error))
            return
        if self.headers.get("Transfer-Encoding") or self.headers.get("Content-Length", "0") != "0":
            self.send_error(400, "Only bodyless snapshot GETs are supported")
            return
        if target.port == proxy.http_port:
            self.send_error(400, "Cannot forward a download back to this proxy")
            return

        gate = proxy._take_gate(("http", self.path), self.path)
        if gate is not None:
            while not gate._released.done():
                if proxy._closed.is_set() or self._client_disconnected():
                    gate.cancelled.set()
                    return
                try:
                    gate._released.result(timeout=0.05)
                except FutureTimeoutError:
                    pass
            if proxy._closed.is_set() or self._client_disconnected():
                gate.cancelled.set()
                return

        with proxy._lock:
            if proxy._closed.is_set():
                return
            proxy._http_connections.add(self.connection)
        response_started = False
        try:
            with socket.create_connection((target.hostname, target.port), timeout=self.timeout) as upstream:
                path = target.path or "/"
                if target.query:
                    path += "?" + target.query
                connection_headers = {
                    name.strip().lower() for name in self.headers.get("Connection", "").split(",")
                }
                connection_headers.update({"connection", "proxy-connection", "proxy-authorization", "host"})
                headers = "".join(
                    f"{name}: {value}\r\n" for name, value in self.headers.items()
                    if name.lower() not in connection_headers
                )
                upstream.sendall(
                    f"GET {path} HTTP/1.1\r\nHost: {target.netloc}\r\n{headers}Connection: close\r\n\r\n".encode("latin-1")
                )
                self.connection.settimeout(RELAY_TIMEOUT)
                # Relay bytes unchanged, including chunked snapshot framing.
                # Watch the caller so cancellation also closes a stalled download.
                while not proxy._closed.is_set():
                    readable, _, _ = select.select([upstream, self.connection], [], [], 0.05)
                    if self.connection in readable:
                        return
                    if upstream in readable:
                        data = upstream.recv(64 * 1024)
                        if not data:
                            return
                        # sendall can transmit some bytes before raising an error.
                        response_started = True
                        self.connection.sendall(data)
        except OSError as error:
            # socket.timeout is an OSError too. Once output may have reached the
            # caller, close the transfer without appending another HTTP response.
            if response_started:
                logger.warning("Snapshot transfer truncated while relaying %s: %s", self.path, error)
            else:
                logger.warning("Snapshot request failed before response output for %s: %s", self.path, error)
                try:
                    self.send_error(502, "Snapshot source request failed")
                except OSError:
                    pass
        finally:
            self.close_connection = True
            with proxy._lock:
                proxy._http_connections.discard(self.connection)

    def _client_disconnected(self):
        readable, _, _ = select.select([self.connection], [], [], 0)
        return bool(readable)
