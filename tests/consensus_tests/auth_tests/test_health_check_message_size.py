"""Health check endpoints skip authentication, so they must bound decoded message size.

Without a limit, an unauthenticated client can send a small gzip message that
decompresses to gigabytes and gets the process OOM-killed. The server limits
decoded health check messages to 4 KiB, on both the public and internal port.

The requests are sent as raw bytes so the tests control the exact payload.
"""

import gzip

import grpc
import pytest
from consensus_tests.utils import kill_all_processes, start_cluster

from .utils import ALT_SECRET, API_KEY_HEADERS, READ_ONLY_API_KEY, SECRET

# Matches `HEALTH_CHECK_MAX_DECODING_MESSAGE_SIZE` in `src/tonic/mod.rs`
LIMIT = 4 * 1024

QDRANT_HEALTH_CHECK = "/qdrant.Qdrant/HealthCheck"
GRPC_HEALTH_CHECK = "/grpc.health.v1.Health/Check"

TIMEOUT = 30


@pytest.fixture(scope="module")
def health_check_cluster(tmp_path_factory: pytest.TempPathFactory):
    tmp_path = tmp_path_factory.mktemp("health_check_message_size")

    # Set API keys, health checks must still be reachable without credentials
    peer_api_uris, _, bootstrap_uri = start_cluster(
        tmp_path,
        num_peers=1,
        extra_env={
            "QDRANT__SERVICE__API_KEY": SECRET,
            "QDRANT__SERVICE__ALT_API_KEY": ALT_SECRET,
            "QDRANT__SERVICE__READ_ONLY_API_KEY": READ_ONLY_API_KEY,
        },
        headers=API_KEY_HEADERS,
    )

    # Peer ports are allocated as: p2p = base, gRPC = base + 1, REST = base + 2
    http_port = int(peer_api_uris[0].rsplit(":", 1)[1])
    grpc_target = f"127.0.0.1:{http_port - 1}"
    p2p_target = bootstrap_uri.removeprefix("http://")

    try:
        yield grpc_target, p2p_target
    finally:
        kill_all_processes()


# (port, method) pairs that host an unauthenticated health check
ENDPOINTS = [
    pytest.param("public", QDRANT_HEALTH_CHECK, id="public-qdrant"),
    pytest.param("public", GRPC_HEALTH_CHECK, id="public-grpc-health"),
    pytest.param("internal", QDRANT_HEALTH_CHECK, id="internal-qdrant"),
]


def _target(cluster, port: str) -> str:
    grpc_target, p2p_target = cluster
    return {"public": grpc_target, "internal": p2p_target}[port]


def _string_field_1(size: int) -> bytes:
    """Protobuf message with field 1 set to a string of `size` bytes, roughly `size` bytes long.

    For `grpc.health.v1.HealthCheckRequest` this is the `service` field. For
    `qdrant.HealthCheckRequest`, which is empty, it is an unknown field that is skipped.
    """
    length = bytearray()
    n = size
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            length.append(byte | 0x80)
        else:
            length.append(byte)
            break
    return b"\x0a" + bytes(length) + b"a" * size


def _call(target: str, method: str, payload: bytes, compression=None):
    with grpc.insecure_channel(target) as channel:
        # No serializers: send and receive raw bytes
        rpc = channel.unary_unary(method)
        return rpc(payload, timeout=TIMEOUT, compression=compression)


@pytest.mark.parametrize("port, method", ENDPOINTS)
@pytest.mark.parametrize(
    "compression",
    [
        pytest.param(None, id="plain"),
        pytest.param(grpc.Compression.Gzip, id="gzip"),
    ],
)
def test_health_check_accepts_normal_requests(health_check_cluster, port, method, compression):
    target = _target(health_check_cluster, port)

    # Empty request, as sent by real clients and probes
    _call(target, method, b"", compression)

    # A request just below the limit is still accepted
    _call(target, method, _string_field_1(LIMIT - 16), compression)


@pytest.mark.parametrize("port, method", ENDPOINTS)
def test_health_check_rejects_oversized_message(health_check_cluster, port, method):
    target = _target(health_check_cluster, port)

    # Declared frame length exceeds the limit, rejected before reading the body
    with pytest.raises(grpc.RpcError) as exc:
        _call(target, method, _string_field_1(LIMIT * 4))
    assert exc.value.code() == grpc.StatusCode.OUT_OF_RANGE, exc.value.details()


@pytest.mark.parametrize("port, method", ENDPOINTS)
def test_health_check_rejects_gzip_bomb(health_check_cluster, port, method):
    target = _target(health_check_cluster, port)

    # 2 MiB decompressed, about 2 KiB compressed: below the limit on the wire,
    # so only the limit on the decompressed size catches it. Deflate expands at
    # most ~1000x, so a bigger bomb would be rejected on its wire size instead.
    payload = _string_field_1(2 * 1024 * 1024)
    assert len(gzip.compress(payload)) < LIMIT

    with pytest.raises(grpc.RpcError) as exc:
        _call(target, method, payload, grpc.Compression.Gzip)
    assert exc.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED, exc.value.details()

    # Server survived and still answers health checks
    _call(target, method, b"")
