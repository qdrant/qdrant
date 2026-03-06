import pathlib

from .fixtures import create_collection
from .utils import *
from .assertions import assert_http_ok

N_PEERS = 2
N_SHARDS = 2
N_REPLICA = 1


def test_shard_transfer_rate_limiting(tmp_path: pathlib.Path):
    peer_api_uris, peer_dirs, _ = start_cluster(tmp_path, N_PEERS)

    # Check that there are no collections on all peers
    for uri in peer_api_uris:
        r = requests.get(f"{uri}/collections")
        assert_http_ok(r)
        assert len(r.json()["result"]["collections"]) == 0

    # Create collection in first peer
    create_collection(peer_api_uris[0], shard_number=N_SHARDS, replication_factor=N_REPLICA)

    # Check that it exists on all peers
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)

    # Check collection's cluster info
    collection_cluster_info = get_collection_cluster_info(peer_api_uris[0], "test_collection")
    assert collection_cluster_info["shard_count"] == N_SHARDS

    # Create points in first peer's collection
    r = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection/points?wait=true", json={
            "points": [
                {
                    "id": 1,
                    "vector": [0.05, 0.61, 0.76, 0.74],
                    "payload": {
                        "city": "Berlin",
                        "country": "Germany",
                        "count": 1000000,
                        "square": 12.5,
                        "coords": {"lat": 1.0, "lon": 2.0}
                    }
                },
                {"id": 2, "vector": [0.19, 0.81, 0.75, 0.11],
                 "payload": {"city": ["Berlin", "London"]}},
                {"id": 3, "vector": [0.36, 0.55, 0.47, 0.94],
                 "payload": {"city": ["Berlin", "Moscow"]}},
                {"id": 4, "vector": [0.18, 0.01, 0.85, 0.80],
                 "payload": {"city": ["London", "Moscow"]}},
                {"id": 5, "vector": [0.24, 0.18, 0.22, 0.44],
                 "payload": {"count": [0]}},
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

    # Extract current collection cluster info
    collection_cluster_info = get_collection_cluster_info(peer_api_uris[0], "test_collection")
    target_peer_id = collection_cluster_info["remote_shards"][0]["peer_id"]
    source_uri = peer_api_uris[0]
    target_uri = peer_api_uris[1]

    target_collection_cluster_info = get_collection_cluster_info(target_uri, "test_collection")
    target_before_local_shard_count = len(target_collection_cluster_info["local_shards"])

    before_local_shard_count = len(collection_cluster_info["local_shards"])
    shard_id = collection_cluster_info["local_shards"][0]["shard_id"]
    source_peer_id = collection_cluster_info["peer_id"]

    # Set rate limiting
    r = requests.patch(
        f"{source_uri}/collections/test_collection", json={
            "strict_mode_config": {
                "enabled": True,
                "write_rate_limit": 1,
                "read_rate_limit": 1,
            }
        })
    assert_http_ok(r)

    # Exhaust write rate limit on target shard
    rate_limited = False
    for _ in range(100):
        r = requests.put(
            f"{target_uri}/collections/test_collection/points?wait=true", json={
                "points": [
                    {
                        "id": 1,
                        "vector": [0.05, 0.61, 0.76, 0.74],
                        "payload": {
                            "city": "Berlin",
                            "country": "Germany",
                            "count": 1000000,
                            "square": 12.5,
                            "coords": {"lat": 1.0, "lon": 2.0}
                        }
                    },
                ]
            })
        if not r.ok:
            rate_limited = True
            assert r.status_code == 429, f"Failure body {r.json()}"
            assert "Rate limiting exceeded: Write rate limit exceeded" in r.json()['status']['error']
            break

    assert rate_limited, "Expected write rate limit to be reached"

    # Move shard `shard_id` to peer `target_peer_id` without write budget
    move_shard(source_uri, "test_collection", shard_id, source_peer_id, target_peer_id)

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(source_uri, "test_collection", 0)

    # Check the number of local shards goes down by 1
    assert check_collection_local_shards_count(source_uri, "test_collection", before_local_shard_count - 1)
    assert check_collection_local_shards_count(target_uri, "test_collection", target_before_local_shard_count + 1)

    # Disable read limits to check data consistency
    r = requests.patch(
        f"{source_uri}/collections/test_collection", json={
            "strict_mode_config": {
                "enabled": False,
            }
        })
    assert_http_ok(r)

    # Wait for propagation of disable command
    wait_for_strict_mode_disabled(peer_api_uris[1], "test_collection")

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

    # Re enable rate limiting
    r = requests.patch(
        f"{source_uri}/collections/test_collection", json={
            "strict_mode_config": {
                "enabled": True,
                "write_rate_limit": 1,
                "read_rate_limit": 1,
            }
        })
    assert_http_ok(r)

    # Wait for propagation of disable command
    wait_for_strict_mode_enabled(peer_api_uris[1], "test_collection")

    # Replicate shards back to the source peer
    r = requests.post(
        f"{source_uri}/collections/test_collection/cluster", json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": target_peer_id,
                "to_peer_id": source_peer_id
            }
        })
    assert_http_ok(r)

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(source_uri, "test_collection", 0)

    # Check that the number of local shard goes back to the original value
    assert check_collection_local_shards_count(source_uri, "test_collection", before_local_shard_count)
    assert check_collection_local_shards_count(target_uri, "test_collection", target_before_local_shard_count + 1)

    # Disable read limits to check data consistency
    r = requests.patch(
        f"{source_uri}/collections/test_collection", json={
            "strict_mode_config": {
                "enabled": False,
            }
        })
    assert_http_ok(r)

    # Wait for propagation of disable command
    wait_for_strict_mode_disabled(peer_api_uris[1], "test_collection")

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

    # Re enable rate limiting
    r = requests.patch(
        f"{source_uri}/collections/test_collection", json={
            "strict_mode_config": {
                "enabled": True,
                "write_rate_limit": 1,
                "read_rate_limit": 1,
            }
        })
    assert_http_ok(r)

    # Wait for propagation of enable command
    wait_for_strict_mode_enabled(peer_api_uris[1], "test_collection")

    # Perform a replication for the second time with the target node active
    r = requests.post(
        f"{source_uri}/collections/test_collection/cluster", json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": target_peer_id,
                "to_peer_id": source_peer_id
            }
        })
    assert_http_ok(r)

    # Wait for end of shard transfer
    wait_for_collection_shard_transfers_count(source_uri, "test_collection", 0)

    # Check that the number of local shard is still the same
    assert check_collection_local_shards_count(source_uri, "test_collection", before_local_shard_count)
    assert check_collection_local_shards_count(target_uri, "test_collection", target_before_local_shard_count + 1)

    # Disable read limits to check data consistency
    r = requests.patch(
        f"{source_uri}/collections/test_collection", json={
            "strict_mode_config": {
                "enabled": False,
            }
        })
    assert_http_ok(r)

    # Wait for propagation of disable command
    wait_for_strict_mode_disabled(peer_api_uris[1], "test_collection")

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


