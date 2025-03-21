import io
import pathlib
import shutil
from time import sleep
from typing import Any

import requests
from consensus_tests.fixtures import create_collection, drop_collection
from .utils import *

N_PEERS = 3
N_REPLICA = 1
N_SHARDS = 1
CONSENSUS_WAIT_SECONDS = 0.5


def test_consensus_compaction(tmp_path: pathlib.Path):
    """
    Basic consensus compaction test.
    Does a few consensus operations on a cluster. Then adds a new peer and
    asserts it boots with correct consensus state.
    """

    assert_project_root()

    env = {
        # Aggressively compact consensus WAL
        "QDRANT__CLUSTER__CONSENSUS__COMPACT_WAL_ENTRIES": "1",
    }

    # Start cluster
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, port_seed=10000, extra_env=env)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA)
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)

    # Add cluster metadata
    put_metadata_key(peer_api_uris, 'my_metadata', 'My value!')

    # Repeatedly drop, re-create collection to accumulate Raft log entries
    for i in range(0, 5):
        print(f"recreating collection {i}")
        # Drop test_collection
        drop_collection(peer_api_uris[0], "test_collection", timeout=5)
        # Re-create test_collection
        create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA, timeout=3)
        res = requests.get(f"{peer_api_uris[1]}/collections")
        print(res.json())

    # Wait for collection to be ready on all peers
    for peer_uri in peer_api_uris:
        wait_for_all_replicas_active(peer_uri, "test_collection")

    # Add extra node
    # Due to aggressive consensus WAL compaction, the peer has to join by consensus snapshot
    peer_dirs.append(make_peer_folder(tmp_path, N_PEERS))
    new_url = start_peer(peer_dirs[-1], "peer_3_extra.log", bootstrap_uri, port=21000, extra_env=env)
    peer_api_uris.append(new_url)
    wait_all_peers_up([new_url])

    # Ensure cluster metadata is consistent on all peers
    # Failed before <https://github.com/qdrant/qdrant/pull/6014>
    get_metadata_key(peer_api_uris, 'my_metadata', 'My value!')


def test_consensus_compaction_shard_keys(tmp_path: pathlib.Path):
    """
    Basic consensus compaction test using user-defined shard keys.
    Does a few consensus operations on a cluster. Then adds a new peer and
    asserts it boots with correct consensus state.

    Tests:
    - <https://github.com/qdrant/qdrant/pull/6209>
    - <https://github.com/qdrant/qdrant/pull/6212>
    """

    assert_project_root()

    # Have shard keys with different types to properly test them in consensus snapshots
    SHARD_KEYS = {
        1: "some_key",
        2: "1",
        3: 2,
    }

    env = {
        # Aggressively compact consensus WAL
        "QDRANT__CLUSTER__CONSENSUS__COMPACT_WAL_ENTRIES": "1",
    }

    # Start cluster
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, port_seed=10000, extra_env=env)

    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA, sharding_method="custom")
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)

    # Create shard keys
    for _, shard_key in SHARD_KEYS.items():
        create_shard_key(shard_key, peer_api_uris[0], collection="test_collection")

    # Validate shard keys on all peers
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)
    validate_shard_keys(peer_api_uris, SHARD_KEYS)

    # Repeatedly drop, re-create other collection to accumulate Raft log entries
    for i in range(0, 5):
        print(f"create temporary collection {i}")
        # Create test_collection
        create_collection(peer_api_uris[0], "tmp_collection", shard_number=N_SHARDS, replication_factor=N_REPLICA, timeout=3)
        # Drop test_collection
        drop_collection(peer_api_uris[0], "tmp_collection", timeout=5)

    # Wait for collection to be ready on all peers
    for peer_uri in peer_api_uris:
        wait_for_all_replicas_active(peer_uri, "test_collection")

    # Add extra node
    # Due to aggressive consensus WAL compaction, the peer has to join by consensus snapshot
    peer_dirs.append(make_peer_folder(tmp_path, N_PEERS))
    new_url = start_peer(peer_dirs[-1], "peer_3_extra.log", bootstrap_uri, port=21000, extra_env=env)
    peer_api_uris.append(new_url)
    wait_all_peers_up([new_url])

    # Validate shard keys on all peers
    # Failed before where the numeric ID would become a string, fixed in:
    # - <https://github.com/qdrant/qdrant/pull/6209>
    # - <https://github.com/qdrant/qdrant/pull/6212>
    validate_shard_keys(peer_api_uris, SHARD_KEYS)


def put_metadata_key(peer_uris: list[str], key: str, value: Any):
    resp = requests.put(f"{peer_uris[0]}/cluster/metadata/keys/{key}", json=value)
    assert_http_ok(resp)
    sleep(CONSENSUS_WAIT_SECONDS)
    get_metadata_key(peer_uris, key, value)


def get_metadata_key(peer_uris: list[str], key: str, expected_value: Any):
    for peer_uri in peer_uris:
        resp = requests.get(f"{peer_uri}/cluster/metadata/keys/{key}")
        assert_http_ok(resp)
        assert resp.json()['result'] == expected_value


def validate_shard_keys(peer_uris: list[str], shard_keys: dict[int, Any]):
    for peer_uri in peer_uris:
        info = get_collection_cluster_info(peer_uri, "test_collection")
        replicas = info['local_shards'] + info['remote_shards']

        assert len(replicas) == (len(shard_keys) * N_SHARDS), "incorrect number of replicas"
        for replica_info in replicas:
            assert shard_keys[replica_info['shard_id']] == replica_info['shard_key'], "shard key does not match"
