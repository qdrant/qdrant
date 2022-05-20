import os
import requests
import socket
from contextlib import closing


def get_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def get_env(p2p_port: int, http_port: int) -> dict[str, str]:
    env = os.environ.copy()
    env["QDRANT__CLUSTER__ENABLED"] = "true"
    env["QDRANT__CLUSTER__P2P__PORT"] = str(p2p_port)
    env["QDRANT__SERVICE__HTTP_PORT"] = str(http_port)
    return env


def get_uri(port: int) -> str:
    return f"http://127.0.0.1:{port}"


def assert_http_ok(response: requests.Response):
    if response.status_code != 200:
        raise Exception(
            f"Http request failed with status {response.status_code} and contents: {response.json()}")
