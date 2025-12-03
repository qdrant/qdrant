import io
import pathlib
import shutil
from time import sleep
from typing import Any

import requests

from .fixtures import *
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

    # Wait for collection to be ready on the new peer
    wait_all_peers_up([new_url])
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=[new_url])

    # Ensure cluster metadata is consistent on all peers
    # Failed before <https://github.com/qdrant/qdrant/pull/6014>
    assert_consistent_metadata_key(peer_api_uris, 'my_metadata', 'My value!')


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
    wait_for_shard_keys(peer_api_uris, "test_collection", SHARD_KEYS)

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

    # New node might need time to catch up with consensus state
    # So we await for the collection to be ready on all peers
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)

    # Validate shard keys on all peers
    # Failed before where the numeric ID would become a string, fixed in:
    # - <https://github.com/qdrant/qdrant/pull/6209>
    # - <https://github.com/qdrant/qdrant/pull/6212>
    wait_for_shard_keys(peer_api_uris, "test_collection", SHARD_KEYS)

@pytest.mark.parametrize(
    "replication_factor",
    [1, 2]
)
def test_consensus_snapshot_create_collection(tmp_path: pathlib.Path, replication_factor: int):
    assert_project_root()

    N_PEERS = 3

    env = {
        "QDRANT__LOG_LEVEL": "debug",
        # Aggressively compact consensus WAL
        "QDRANT__CLUSTER__CONSENSUS__COMPACT_WAL_ENTRIES": "1",
    }

    # Start cluster
    peers, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, extra_env=env)

    # Get last peer ID
    last_peer_id = get_cluster_info(peers[-1])['peer_id']

    # Kill last peer
    processes.pop().kill()

    # Bootstrap collection
    create_collection(peers[0], shard_number=3, replication_factor=replication_factor)
    wait_collection_on_all_peers("test_collection", peers[:N_PEERS - 1], 10)

    upsert_random_points(peers[0], 1000, fail_on_error=False)

    # Restart last peer
    peers[-1] = start_peer(peer_dirs[-1], "peer_2_restarted.log", bootstrap_uri)
    wait_for_peer_online(peers[-1])

    # Wait for last peer recovery
    wait_collection_exists_and_active_on_all_peers("test_collection", peers, 10)


def put_metadata_key(peer_uris: list[str], key: str, value: Any):
    resp = requests.put(f"{peer_uris[0]}/cluster/metadata/keys/{key}?wait=true", json=value)
    assert_http_ok(resp)
    sleep(CONSENSUS_WAIT_SECONDS)
    assert_consistent_metadata_key(peer_uris, key, value)


def assert_consistent_metadata_key(peer_uris: list[str], key: str, expected_value: Any):
    for peer_id, peer_uri in enumerate(peer_uris):
        resp = requests.get(f"{peer_uri}/cluster/metadata/keys/{key}")
        assert_http_ok(resp)
        peer_key_value = resp.json()['result']
        assert peer_key_value == expected_value, f"incorrect metadata key value for peers[{peer_id}] '{peer_uri}' - expected '{expected_value}' but got '{peer_key_value}'"


def check_shard_keys(peer_uris: list[str], collection_name: str, shard_keys: dict[int, Any]):
        for peer_uri in peer_uris:
            print(f"Checking shard keys on {peer_uri} for collection {collection_name}")
            info = get_collection_cluster_info(peer_uri, collection_name)
            replicas = info['local_shards'] + info['remote_shards']

            if len(replicas) != (len(shard_keys) * N_SHARDS):
                return False

            for replica_info in replicas:
                if shard_keys[replica_info['shard_id']] != replica_info['shard_key']:
                    return False

        return True


def wait_for_shard_keys(peer_uris: List[str], collection_name: str, shard_keys: dict[int, Any]):
    try:
        wait_for(check_shard_keys, peer_uris=peer_uris, collection_name=collection_name, shard_keys=shard_keys)
    except Exception as e:
        print_collection_cluster_info(peer_uris, collection_name)
        raise e
