"""Exercise the proxy over real sockets without requiring a Qdrant binary."""

import socket
from concurrent.futures import CancelledError, ThreadPoolExecutor
from queue import Empty, Queue
from threading import Event
from types import SimpleNamespace

import grpc
import pytest

from .peer_proxy import PeerProxy, RequestGate


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
        with proxy.hold_rpc(TRANSFER, matches=lambda request: request == b"selected-shard") as gate:
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


def test_peer_proxy_blocks_all_calls_to_one_method(upstream):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        raft = channel.unary_unary(RAFT)
        with proxy.block_rpc(RAFT, matches=lambda request: request != b"survivor"):
            for request in (b"first", b"retry"):
                with pytest.raises(grpc.RpcError) as failure:
                    raft(request, timeout=TIMEOUT)
                assert failure.value.code() == grpc.StatusCode.UNAVAILABLE
            assert_no_calls(upstream)
            assert raft(b"survivor", timeout=TIMEOUT) == b"survivor"
            assert upstream.calls.get(timeout=TIMEOUT)[:2] == (RAFT, b"survivor")
            assert channel.unary_unary(TRANSFER)(b"transfer", timeout=TIMEOUT) == b"transfer"
            assert upstream.calls.get(timeout=TIMEOUT)[:2] == (TRANSFER, b"transfer")
        assert raft(b"resumed", timeout=TIMEOUT) == b"resumed"
        assert upstream.calls.get(timeout=TIMEOUT)[:2] == (RAFT, b"resumed")


def test_peer_proxy_holds_response_after_upstream_handled_request(upstream):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        rpc = channel.unary_unary(RAFT)
        with proxy.hold_rpc_response(RAFT, matches=lambda request: request == b"selected") as gate:
            held = rpc.future(b"selected", timeout=TIMEOUT)
            assert gate.wait_for_request(TIMEOUT) == b"selected"
            assert upstream.calls.get(timeout=TIMEOUT)[1] == b"selected"
            assert not held.done()

            assert rpc(b"other", timeout=TIMEOUT) == b"other"
            assert rpc(b"selected", timeout=TIMEOUT) == b"selected"
            assert not held.done()
            gate.release()
            assert held.result(TIMEOUT) == b"selected"
            assert dict(held.trailing_metadata())["upstream-trailer"] == "present"


def test_peer_proxy_cancels_held_response(upstream):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        proxy.wait_for_peer_connection(timeout=TIMEOUT)
        grpc.channel_ready_future(channel).result(timeout=TIMEOUT)
        rpc = channel.unary_unary(RAFT)
        with proxy.hold_rpc_response(RAFT) as gate:
            held = rpc.future(b"selected")
            gate.wait_for_request(TIMEOUT)
            assert upstream.calls.get(timeout=TIMEOUT)[1] == b"selected"
            assert held.cancel()
            assert gate.cancelled.wait(TIMEOUT)
            gate.release()
            assert rpc(b"after", timeout=TIMEOUT) == b"after"
            assert upstream.calls.get(timeout=TIMEOUT)[1] == b"after"
            assert_no_calls(upstream)


def test_peer_proxy_response_gate_reports_upstream_error(upstream):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        with proxy.hold_rpc_response(RAFT) as gate:
            call = channel.unary_unary(RAFT).future(b"error", timeout=TIMEOUT)
            with pytest.raises(grpc.RpcError) as failure:
                call.result(TIMEOUT)
            with pytest.raises(grpc.RpcError) as gate_failure:
                gate.wait_for_request(TIMEOUT)
            assert gate_failure.value.code() == failure.value.code() == grpc.StatusCode.FAILED_PRECONDITION
            assert gate_failure.value.details() == failure.value.details() == "replica is not ready"
            assert not gate.cancelled.is_set()


@pytest.mark.parametrize("stop", ["cancel", "shutdown"])
def test_peer_proxy_response_gate_cancels_before_upstream_response(upstream, stop):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        proxy.wait_for_peer_connection(timeout=TIMEOUT)
        grpc.channel_ready_future(channel).result(timeout=TIMEOUT)
        with proxy.hold_rpc_response(RAFT) as gate:
            call = channel.unary_unary(RAFT).future(b"block-upstream")
            assert upstream.blocked.wait(TIMEOUT)
            if stop == "cancel":
                assert call.cancel()
            else:
                proxy.close()
            assert gate.cancelled.wait(TIMEOUT)
            with pytest.raises((CancelledError, grpc.RpcError)) as failure:
                gate.wait_for_request(TIMEOUT)
            if isinstance(failure.value, grpc.RpcError):
                assert failure.value.code() == grpc.StatusCode.CANCELLED
            assert upstream.cancelled.wait(TIMEOUT)


def test_peer_proxy_response_gate_waits_for_upstream_response(upstream):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        rpc = channel.unary_unary(RAFT)
        with proxy.hold_rpc_response(RAFT) as gate:
            held = rpc.future(b"block-upstream", timeout=TIMEOUT)
            assert upstream.blocked.wait(TIMEOUT)
            # The upstream is blocked, so the response gate must not have notified yet.
            with pytest.raises(TimeoutError):
                gate.wait_for_request(timeout=0)
            upstream.cancelled.set()
            assert gate.wait_for_request(TIMEOUT) == b"block-upstream"
            assert not held.done()
            gate.release()
            assert held.result(TIMEOUT) == b"block-upstream"


def test_peer_proxy_does_not_forward_cancelled_held_requests(upstream):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        rpc = channel.unary_unary(TRANSFER)
        with proxy.hold_rpc(TRANSFER) as gate:
            held = rpc.future(b"cancel-me")
            gate.wait_for_request(TIMEOUT)
            assert held.cancel()
            assert gate.cancelled.wait(TIMEOUT)
            gate.release()
            assert rpc(b"after-cancel", timeout=TIMEOUT) == b"after-cancel"
            assert upstream.calls.get(timeout=TIMEOUT)[1] == b"after-cancel"
            assert_no_calls(upstream)


def test_peer_proxy_cancels_upstream_work(upstream):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        call = channel.unary_unary(TRANSFER).future(b"block-upstream")
        assert upstream.blocked.wait(TIMEOUT)
        assert call.cancel()
        assert upstream.cancelled.wait(TIMEOUT)


@pytest.mark.parametrize("phase", ["request", "upstream", "response"])
def test_peer_proxy_propagates_rpc_deadline(upstream, phase):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        proxy.wait_for_peer_connection(timeout=TIMEOUT)
        grpc.channel_ready_future(channel).result(timeout=TIMEOUT)
        rpc = channel.unary_unary(RAFT)
        hold = proxy.hold_rpc if phase == "request" else proxy.hold_rpc_response
        with hold(RAFT) as gate:
            request = b"block-upstream" if phase == "upstream" else b"selected"
            # The deadline starts at dispatch, so slow CI can expire it before the
            # intended phase. The cancellation tests above control that ordering.
            call = rpc.future(request, timeout=1)
            if phase == "upstream":
                assert upstream.blocked.wait(TIMEOUT)
            else:
                assert gate.wait_for_request(TIMEOUT) == request
            if phase != "request":
                assert upstream.calls.get(timeout=TIMEOUT)[1] == request
            with pytest.raises(grpc.RpcError) as failure:
                call.result(TIMEOUT)
            assert failure.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
            assert gate.cancelled.wait(TIMEOUT)
            if phase == "upstream":
                # Either the server task or its upstream call can observe the deadline first.
                with pytest.raises((CancelledError, grpc.RpcError)) as failure:
                    gate.wait_for_request(TIMEOUT)
                if isinstance(failure.value, grpc.RpcError):
                    assert failure.value.code() in (grpc.StatusCode.CANCELLED, grpc.StatusCode.DEADLINE_EXCEEDED)
                assert upstream.cancelled.wait(TIMEOUT)
            gate.release()
            assert rpc(b"after", timeout=TIMEOUT) == b"after"
            assert upstream.calls.get(timeout=TIMEOUT)[1] == b"after"
            assert_no_calls(upstream)


def test_peer_proxy_shutdown_cancels_held_requests(upstream):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        with proxy.hold_rpc(TRANSFER) as gate:
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
            with proxy.hold_rpc(TRANSFER):
                raise ValueError("test failed")
        assert channel.unary_unary(TRANSFER)(b"not-held", timeout=TIMEOUT) == b"not-held"


def test_peer_proxy_reports_which_request_did_not_arrive(upstream):
    with PeerProxy(upstream.address) as proxy:
        with proxy.hold_rpc(TRANSFER) as gate:
            with pytest.raises(TimeoutError, match="No request reached the gate for " + TRANSFER):
                gate.wait_for_request(timeout=0)


def test_peer_proxy_releases_held_request_after_test_error(upstream):
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        with pytest.raises(ValueError, match="test failed"):
            with proxy.hold_rpc(TRANSFER) as gate:
                held = channel.unary_unary(TRANSFER).future(b"held", timeout=TIMEOUT)
                gate.wait_for_request(TIMEOUT)
                raise ValueError("test failed")
        assert held.result(TIMEOUT) == b"held"


def test_peer_proxies_have_independent_gates(upstream):
    with PeerProxy(upstream.address) as first, PeerProxy(upstream.address) as second:
        assert first.port != second.port
        with grpc.insecure_channel(first.address) as a, grpc.insecure_channel(second.address) as b:
            with first.hold_rpc(TRANSFER) as first_gate, second.hold_rpc(TRANSFER) as second_gate:
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
            with pytest.raises(TimeoutError, match=f"gRPC connection to {proxy._target} within 1 seconds"):
                proxy.wait_for_peer_connection(timeout=1)


def test_peer_proxy_shutdown_cancels_rpc_and_http_gates_together(upstream):
    source_uri = "http://127.0.0.1:1"
    snapshot_url = source_uri + "/collections/test/shards/0/snapshot"
    with PeerProxy(upstream.address) as proxy, grpc.insecure_channel(proxy.address) as channel:
        with proxy.hold_rpc(TRANSFER) as rpc_gate, proxy.hold_snapshot_download(source_uri, "test", 0) as http_gate:
            assert isinstance(rpc_gate, RequestGate)
            assert isinstance(http_gate, RequestGate)
            held_rpc = channel.unary_unary(TRANSFER).future(b"held", timeout=TIMEOUT)
            assert rpc_gate.wait_for_request(TIMEOUT) == b"held"
            with socket.create_connection(("127.0.0.1", proxy.http_port), timeout=TIMEOUT) as caller:
                caller.sendall(f"GET {snapshot_url} HTTP/1.1\r\nHost: ignored\r\n\r\n".encode())
                assert http_gate.wait_for_request(TIMEOUT) == snapshot_url
                assert not held_rpc.done()
                proxy.close()
                assert caller.recv(1) == b""
            with pytest.raises(grpc.RpcError):
                held_rpc.result(TIMEOUT)
            assert rpc_gate.cancelled.is_set()
            assert http_gate.cancelled.is_set()
            assert_no_calls(upstream)
