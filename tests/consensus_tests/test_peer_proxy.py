"""Exercise the proxy over real sockets without requiring a Qdrant binary."""

import socket
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue
from threading import Event
from types import SimpleNamespace

import grpc
import pytest

from .peer_proxy import PeerProxy


TRANSFER = "/qdrant.CollectionsInternal/GetShardRecoveryPoint"
RAFT = "/qdrant.Raft/Send"
TIMEOUT = 5


@pytest.fixture
def upstream():
    calls = Queue()
    blocked = Event()
    cancelled = Event()

    class Handler(grpc.GenericRpcHandler):
        def service(self, details):
            def echo(request, context):
                calls.put((details.method, request, dict(context.invocation_metadata())))
                context.send_initial_metadata((("upstream-header", "present"),))
                context.set_trailing_metadata((("upstream-trailer", "present"),))
                if request == b"error":
                    context.abort(grpc.StatusCode.FAILED_PRECONDITION, "replica is not ready")
                if request == b"block-upstream":
                    context.add_callback(cancelled.set)
                    blocked.set()
                    assert cancelled.wait(TIMEOUT), "Proxy did not cancel the upstream call"
                return request

            return grpc.unary_unary_rpc_method_handler(echo)

    with ThreadPoolExecutor(max_workers=4) as executor:
        server = grpc.server(
            executor,
            handlers=(Handler(),),
            options=(("grpc.max_receive_message_length", -1), ("grpc.max_send_message_length", -1)),
        )
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            yield SimpleNamespace(address=f"127.0.0.1:{port}", calls=calls, blocked=blocked, cancelled=cancelled)
        finally:
            cancelled.set()
            server.stop(0).wait(TIMEOUT)


def assert_no_calls(upstream):
    with pytest.raises(Empty):
        upstream.calls.get_nowait()


def test_peer_proxy_preserves_payload_metadata_and_errors(upstream):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(
        proxy.address, options=(("grpc.max_receive_message_length", -1),)
    ) as channel:
        proxy.wait_for_peer_connection(timeout=TIMEOUT)
        rpc = channel.unary_unary(TRANSFER)
        # Larger than the default gRPC receive limit, as a shard batch can be.
        payload = b"\x00\xff" * (3 * 1024 * 1024)
        response, call = rpc.with_call(payload, timeout=TIMEOUT, metadata=(("test-bin", b"\x00\xff"),))
        assert response == payload
        method, received, metadata = upstream.calls.get(timeout=TIMEOUT)
        assert (method, received, metadata["test-bin"]) == (TRANSFER, payload, b"\x00\xff")
        assert dict(call.initial_metadata())["upstream-header"] == "present"
        assert dict(call.trailing_metadata())["upstream-trailer"] == "present"

        with pytest.raises(grpc.RpcError) as failure:
            rpc(b"error", timeout=TIMEOUT)
        assert failure.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        assert failure.value.details() == "replica is not ready"
        assert dict(failure.value.initial_metadata())["upstream-header"] == "present"
        assert dict(failure.value.trailing_metadata())["upstream-trailer"] == "present"


def test_peer_proxy_holds_one_match_and_keeps_consensus_and_recovery_live(upstream):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        rpc = channel.unary_unary(TRANSFER)
        with proxy.hold(TRANSFER, matches=lambda request: request == b"selected-shard") as gate:
            assert rpc(b"other-shard", timeout=TIMEOUT) == b"other-shard"
            assert upstream.calls.get(timeout=TIMEOUT)[1] == b"other-shard"

            held = rpc.future(b"selected-shard", timeout=TIMEOUT)
            assert gate.wait_for_request(TIMEOUT) == b"selected-shard"
            assert_no_calls(upstream)
            assert not held.done()

            assert channel.unary_unary(RAFT)(b"consensus", timeout=TIMEOUT) == b"consensus"
            assert upstream.calls.get(timeout=TIMEOUT)[:2] == (RAFT, b"consensus")
            assert rpc(b"selected-shard", timeout=TIMEOUT) == b"selected-shard"
            assert upstream.calls.get(timeout=TIMEOUT)[1] == b"selected-shard"
            assert not held.done()

            gate.release()
            assert held.result(TIMEOUT) == b"selected-shard"
            assert upstream.calls.get(timeout=TIMEOUT)[1] == b"selected-shard"


@pytest.mark.parametrize("expire", [False, True], ids=["cancel", "deadline"])
def test_peer_proxy_does_not_forward_cancelled_held_requests(upstream, expire):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        rpc = channel.unary_unary(TRANSFER)
        with proxy.hold(TRANSFER) as gate:
            held = rpc.future(b"cancel-me", timeout=1 if expire else TIMEOUT)
            gate.wait_for_request(TIMEOUT)
            if expire:
                with pytest.raises(grpc.RpcError) as failure:
                    held.result(TIMEOUT)
                assert failure.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
            else:
                assert held.cancel()
            assert gate.cancelled.wait(TIMEOUT)
            gate.release()
            assert rpc(b"after-cancel", timeout=TIMEOUT) == b"after-cancel"
            assert upstream.calls.get(timeout=TIMEOUT)[1] == b"after-cancel"
            assert_no_calls(upstream)


@pytest.mark.parametrize("expire", [False, True], ids=["cancel", "deadline"])
def test_peer_proxy_cancels_upstream_work(upstream, expire):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        call = channel.unary_unary(TRANSFER).future(b"block-upstream", timeout=1 if expire else TIMEOUT)
        assert upstream.blocked.wait(TIMEOUT)
        if expire:
            with pytest.raises(grpc.RpcError) as failure:
                call.result(TIMEOUT)
            assert failure.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
        else:
            assert call.cancel()
        assert upstream.cancelled.wait(TIMEOUT)


def test_peer_proxy_shutdown_cancels_held_requests(upstream):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        with proxy.hold(TRANSFER) as gate:
            held = channel.unary_unary(TRANSFER).future(b"held")
            gate.wait_for_request(TIMEOUT)
            proxy.close()
            with pytest.raises(grpc.RpcError):
                held.result(TIMEOUT)
            assert gate.cancelled.wait(TIMEOUT)
            assert not proxy._thread.is_alive()
            assert_no_calls(upstream)


def test_peer_proxy_removes_unused_gate_after_test_error(upstream):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        with pytest.raises(ValueError, match="test failed"):
            with proxy.hold(TRANSFER):
                raise ValueError("test failed")
        assert channel.unary_unary(TRANSFER)(b"not-held", timeout=TIMEOUT) == b"not-held"


def test_peer_proxy_reports_which_request_did_not_arrive(upstream):
    with PeerProxy(upstream.address) as proxy:
        with proxy.hold(TRANSFER) as gate:
            with pytest.raises(TimeoutError, match="No request reached the gate for " + TRANSFER):
                gate.wait_for_request(timeout=0)


def test_peer_proxy_releases_held_request_after_test_error(upstream):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        with pytest.raises(ValueError, match="test failed"):
            with proxy.hold(TRANSFER) as gate:
                held = channel.unary_unary(TRANSFER).future(b"held", timeout=TIMEOUT)
                gate.wait_for_request(TIMEOUT)
                raise ValueError("test failed")
        assert held.result(TIMEOUT) == b"held"


def test_peer_proxies_have_independent_gates(upstream):
    with PeerProxy(upstream.address) as first, PeerProxy(upstream.address) as second:
        assert first.port != second.port
        with grpc.insecure_channel(first.address) as a, grpc.insecure_channel(second.address) as b:
            with first.hold(TRANSFER) as first_gate, second.hold(TRANSFER) as second_gate:
                first_call = a.unary_unary(TRANSFER).future(b"first", timeout=TIMEOUT)
                second_call = b.unary_unary(TRANSFER).future(b"second", timeout=TIMEOUT)
                assert first_gate.wait_for_request(TIMEOUT) == b"first"
                assert second_gate.wait_for_request(TIMEOUT) == b"second"
                assert_no_calls(upstream)
                first_gate.release()
                assert first_call.result(TIMEOUT) == b"first"
                assert not second_call.done()
                second_gate.release()
                assert second_call.result(TIMEOUT) == b"second"


def test_peer_proxy_reports_port_conflict(upstream):
    with socket.socket() as occupied:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen()
        with pytest.raises(RuntimeError, match="Failed to bind"):
            PeerProxy(upstream.address, port=occupied.getsockname()[1])


def test_peer_proxy_wait_for_peer_connection_has_a_deadline():
    # A listening TCP socket is not enough: the upstream must speak gRPC.
    with socket.socket() as upstream:
        upstream.bind(("127.0.0.1", 0))
        upstream.listen()
        with PeerProxy(f"127.0.0.1:{upstream.getsockname()[1]}") as proxy:
            with pytest.raises(TimeoutError, match=f"gRPC connection to {proxy._target} within 0 seconds"):
                proxy.wait_for_peer_connection(timeout=0)
