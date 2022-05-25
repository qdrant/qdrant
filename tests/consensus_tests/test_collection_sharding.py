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

def test_collection_sharding(tmp_path: pathlib.Path):
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
    log_file = open("peer_0_0.log", "w")
    conftest.processes.append(
        Popen([qdrant_exec, "--uri", bootstrap_uri], env=env, cwd=peer_dirs[0], stderr=log_file))
    time.sleep(5)

    # Start other peers
    for i in range(1, len(peer_dirs)):
        p2p_port = get_port()
        http_port = get_port()
        env = get_env(p2p_port, http_port)
        peer_api_uris.append(get_uri(http_port))
        log_file = open(f"peer_0_{i}.log", "w")
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

    # Create collection in first peer
    r = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection", json={
            "vector_size": 4,
            "distance": "Dot",
            "shard_number": N_SHARDS,
        })
    assert_http_ok(r)

    time.sleep(5)

    # Check that it exists on all peers
    for uri in peer_api_uris:
        r = requests.get(f"{uri}/collections")
        assert_http_ok(r)
        assert r.json()[
            "result"]["collections"][0]["name"] == "test_collection"

    # Create points in first peer's collection
    r = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection/points?wait=true", json={
            "points": [
                {
                    "id": 1,
                    "vector": [0.05, 0.61, 0.76, 0.74],
                    "payload": {
                      "city": "Berlin",
                      "country": "Germany" ,
                      "count": 1000000,
                      "square": 12.5,
                      "coords": { "lat": 1.0, "lon": 2.0 }
                    }
                },
                {"id": 2, "vector": [0.19, 0.81, 0.75, 0.11], "payload": {"city": ["Berlin", "London"]}},
                {"id": 3, "vector": [0.36, 0.55, 0.47, 0.94], "payload": {"city": ["Berlin", "Moscow"]}},
                {"id": 4, "vector": [0.18, 0.01, 0.85, 0.80], "payload": {"city": ["London", "Moscow"]}},
                {"id": 5, "vector": [0.24, 0.18, 0.22, 0.44], "payload": {"count": [0]}},
                {"id": 6, "vector": [0.35, 0.08, 0.11, 0.44]}
            ]
        })
    assert_http_ok(r)

    # Check that 'search' returns the same results on all peers
    for uri in peer_api_uris:
         r = requests.post(
             f"{uri}/collections/test_collection/points/search", json={
                 "vector": [0.2, 0.1, 0.9, 0.7],
                 "top": 3,
             }
         )
         assert_http_ok(r)
         assert r.json()["result"][0]["id"] == 4
         assert r.json()["result"][1]["id"] == 1
         assert r.json()["result"][2]["id"] == 3