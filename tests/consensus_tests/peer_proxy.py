"""A test-only proxy for Qdrant's unary internal gRPC calls.

The proxy forwards protobuf bytes unchanged. A gate holds one matching request
before forwarding it. Later requests, including calls to the same method, pass
through so recovery traffic can continue. Matchers run on the server loop and
must not block. They can decode the bytes to select a collection or shard.

This does not proxy REST snapshot downloads or streaming RPCs. A request body
does not identify its source peer. Tests must establish which peer sends the
selected request before creating a gate.
"""

import asyncio
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from threading import Event, Thread
from typing import Callable

import grpc


class RequestGate:
    def __init__(self, proxy, method: str, matches: Callable[[bytes], bool]):
        self.method = method
        self.cancelled = Event()
        self._proxy = proxy
        self._matches = matches
        self._arrived = Future()
        self._released = asyncio.Event()

    def wait(self, timeout: float = 30) -> bytes:
        """Wait until the request is held and return its protobuf bytes."""
        try:
            return self._arrived.result(timeout)
        except FutureTimeoutError as error:
            raise TimeoutError(f"No request reached the gate for {self.method}") from error

    def release(self):
        """Release the held request, or remove a gate that has not been reached."""
        if self._proxy._thread.is_alive():
            self._proxy._submit(self._proxy._release(self))


class PeerProxy(grpc.GenericRpcHandler):
    """Run an internal gRPC proxy in a background thread for synchronous tests.

    `target` is the real peer's `host:port`. Advertise `uri` as the peer address
    when wiring a cluster through the proxy. Port zero lets the OS reserve a
    free port without racing pytest workers.

    Use as a context manager. Closing cancels held and forwarded calls and
    joins the server thread. Each instance can have one gate waiting for a
    request at a time.

    With the peer configured to advertise the proxy URI, a test can use:

        with proxy.hold("/qdrant.CollectionsInternal/GetShardRecoveryPoint") as gate:
            replicate_shard(...)
            request = gate.wait()
            # Check the transfer identity, then remove or stop the source.
            gate.release()

    Reaching this gate proves that the RPC was sent, not that any data was
    copied. The test must separately check membership, shard state, and data.
    """

    def __init__(self, target: str, port: int = 0):
        self._target = target
        self._port = port
        self._gate = None
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

    def wait_for_peer(self, timeout: float = 30):
        """Wait for the proxy's connection to the real peer, including on restart."""
        try:
            self._submit(self._channel.channel_ready(), timeout=timeout)
        except FutureTimeoutError as error:
            raise TimeoutError(f"Peer at {self._target} did not become available within {timeout} seconds") from error

    def close(self):
        if self._thread.is_alive():
            self._loop.call_soon_threadsafe(self._stop.set)
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                raise TimeoutError("Peer proxy did not stop")

    @contextmanager
    def hold(self, method: str, matches: Callable[[bytes], bool] = lambda _: True):
        """Hold the next matching request until the gate is released."""
        gate = self._submit(self._create_gate(method, matches))
        try:
            yield gate
        finally:
            gate.release()

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

    async def _create_gate(self, method, matches):
        if self._gate is not None:
            raise RuntimeError("A gate is already waiting for a request")
        if not method.startswith("/") or method.count("/") != 2:
            raise ValueError("Use the full gRPC method path: /service/method")
        gate = RequestGate(self, method, matches)
        self._gate = gate
        return gate

    async def _release(self, gate):
        if self._gate is gate:
            self._gate = None
            gate._arrived.cancel()
        gate._released.set()

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
        try:
            port = self._server.add_insecure_port(f"127.0.0.1:{self._port}")
            await self._server.start()
            self._ready.set_result(port)
            await self._stop.wait()
        finally:
            if self._gate is not None:
                self._gate._arrived.cancel()
            await self._server.stop(0)
            await self._channel.close()

    def service(self, handler_call_details):
        async def forward(request, context):
            gate = self._gate
            if gate is not None and gate.method == handler_call_details.method and gate._matches(request):
                # Consume the gate before waiting. Another peer's recovery must
                # not get held just because it calls the same method.
                self._gate = None
                gate._arrived.set_result(request)
                try:
                    await gate._released.wait()
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
