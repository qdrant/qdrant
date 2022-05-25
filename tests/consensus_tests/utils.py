import os
import shutil
from subprocess import Popen
import time
from typing import Tuple
import requests
import socket
from contextlib import closing
from . import conftest
from pathlib import Path


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


def assert_project_root():
    directory_path = os.getcwd()
    folder_name = os.path.basename(directory_path)
    assert folder_name == "qdrant"


def get_qdrant_exec() -> str:
    directory_path = os.getcwd()
    qdrant_exec = directory_path + "/target/debug/qdrant"
    return qdrant_exec


# Starts a peer and returns its api_uri
def start_peer(peer_dir: Path, log_file: str, bootstrap_uri: str) -> str:
    p2p_port = get_port()
    http_port = get_port()
    env = get_env(p2p_port, http_port)
    log_file = open(log_file, "w")
    conftest.processes.append(
        Popen([get_qdrant_exec(), "--bootstrap", bootstrap_uri], env=env, cwd=peer_dir, stderr=log_file))
    time.sleep(3)
    return get_uri(http_port)


# Starts a peer and returns its api_uri and p2p_uri
def start_first_peer(peer_dir: Path, log_file: str) -> Tuple[str, str]:
    p2p_port = get_port()
    http_port = get_port()
    env = get_env(p2p_port, http_port)
    log_file = open(log_file, "w")
    bootstrap_uri = get_uri(p2p_port)
    conftest.processes.append(
        Popen([get_qdrant_exec(), "--uri", bootstrap_uri], env=env, cwd=peer_dir, stderr=log_file))
    time.sleep(5)
    return (get_uri(http_port), bootstrap_uri)


def make_peer_folders(base_path: Path, n_peers: int) -> list[Path]:
    peer_dirs = []
    for i in range(n_peers):
        peer_dir = base_path / f"peer{i}"
        peer_dir.mkdir()
        peer_dirs.append(peer_dir)
        shutil.copytree("config", peer_dir / "config")
    return peer_dirs
