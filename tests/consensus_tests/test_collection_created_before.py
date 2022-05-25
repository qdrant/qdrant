import os
import pathlib
import shutil
import time

import requests

from .utils import *
from . import conftest
from subprocess import Popen

N_PEERS = 5
N_SHARDS = 6

def test_collection_before_peers_added(tmp_path: pathlib.Path):
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
    p2p_port = get_port()
    http_port = get_port()
    env = get_env(p2p_port, http_port)
    bootstrap_uri = get_uri(p2p_port)
    peer_api_uris.append(get_uri(http_port))
    log_file = open("peer_1_0.log", "w")
    conftest.processes.append(
        Popen([qdrant_exec, "--uri", bootstrap_uri], env=env, cwd=peer_dirs[0], stderr=log_file))
    time.sleep(5)

    # Create collection
    r = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection", json={
            "vector_size": 4,
            "distance": "Dot",
            "shard_number": N_SHARDS
        })
    assert_http_ok(r)

    time.sleep(1)

    # Start other peers
    for i in range(1, len(peer_dirs)):
        p2p_port = get_port()
        http_port = get_port()
        env = get_env(p2p_port, http_port)
        peer_api_uris.append(get_uri(http_port))
        log_file = open(f"peer_1_{i}.log", "w")
        conftest.processes.append(
            Popen([qdrant_exec, "--bootstrap", bootstrap_uri], env=env, cwd=peer_dirs[i], stderr=log_file))
        time.sleep(3)

    # Wait
    time.sleep(3)

    # Check that it exists on all peers
    for uri in peer_api_uris:
        r = requests.get(f"{uri}/collections")
        assert_http_ok(r)
        assert r.json()[
            "result"]["collections"][0]["name"] == "test_collection"
