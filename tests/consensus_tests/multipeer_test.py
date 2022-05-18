import os
import pathlib
import shutil
import time

import requests
from . import conftest
from subprocess import Popen

N_PEERS = 5


def get_http_port(peer_index: int) -> int:
    return 6330 + peer_index*10


def get_p2p_port(peer_index: int) -> int:
    return 6330 + peer_index*10 + 1


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


def test_multipeer_deployment(tmp_path: pathlib.Path):
    # Ensure current path is project root
    directory_path = os.getcwd()
    folder_name = os.path.basename(directory_path)
    assert folder_name == "qdrant"

    qdrant_exec = directory_path + "/target/debug/qdrant"

    # Make peer folders
    peer_dirs = []
    for i in range(N_PEERS):
        peer_dir = tmp_path / f"peer{i}"
        peer_dir.mkdir()
        peer_dirs.append(peer_dir)
        shutil.copytree("config", peer_dir / "config")

    # Gathers REST API uris
    peer_api_uris = []

    # Start bootstrap
    p2p_port = get_p2p_port(0)
    http_port = get_http_port(0)
    env = get_env(p2p_port, http_port)
    bootstrap_uri = get_uri(p2p_port)
    peer_api_uris.append(get_uri(http_port))
    log_file = open("peer0.log", "w")
    conftest.processes.append(
        Popen([qdrant_exec, "--uri", bootstrap_uri], env=env, cwd=peer_dirs[0], stderr=log_file))
    time.sleep(5)

    # Start other peers
    for i in range(1, len(peer_dirs)):
        p2p_port = get_p2p_port(i)
        http_port = get_http_port(i)
        env = get_env(p2p_port, http_port)
        peer_api_uris.append(get_uri(http_port))
        log_file = open(f"peer{i}.log", "w")
        conftest.processes.append(
            Popen([qdrant_exec, "--bootstrap", bootstrap_uri], env=env, cwd=peer_dirs[i], stderr=log_file))
        time.sleep(3)

    # Wait
    time.sleep(3)

    # Check that there are no collections on all peers
    for uri in peer_api_uris:
        r = requests.get(f"{uri}/collections")
        assert_http_ok(r)
        assert len(r.json()["result"]["collections"]) == 0

    # Create collection
    r = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection", json={
            "vector_size": 4,
            "distance": "Dot"
        })
    assert_http_ok(r)

    time.sleep(5)

    # Check that it exists on all peers
    for uri in peer_api_uris:
        r = requests.get(f"{uri}/collections")
        assert_http_ok(r)
        assert r.json()[
            "result"]["collections"][0]["name"] == "test_collection"
