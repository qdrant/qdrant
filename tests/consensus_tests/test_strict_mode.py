import logging
import pathlib

from .fixtures import create_collection, upsert_random_points, upsert_points, random_dense_vector, set_strict_mode, delete_points_by_filter, set_payload_by_filter
from .utils import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

N_PEERS = 2
N_SHARDS = 1
N_REPLICAS = 1
COLLECTION_NAME = "test_collection_strict_mode"


def test_vector_storage_strict_mode_upsert(tmp_path: pathlib.Path):
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, 4)

    strict_mode = {
        "enabled": True,
        "max_collection_vector_size_bytes": 11600,
    }
    create_collection(peer_urls[0], collection=COLLECTION_NAME, shard_number=4, replication_factor=N_REPLICAS, strict_mode=strict_mode)

    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)

    # Insert points into leader
    for i in range(10):
        upsert_random_points(peer_urls[0], 100, collection_name=COLLECTION_NAME, offset=i*100)

    # Check that each node blocks new points now
    for peer_url in peer_urls:
        for _ in range(32):
            point = {"id": 1001, "payload": {}, "vector": random_dense_vector()}
            res = upsert_points(peer_url, [point], collection_name=COLLECTION_NAME)
            if not res.ok:
                assert "Max vector storage size" in res.json()['status']['error']
                return

    raise AssertionError("Should have blocked upsert but didn't")


def test_vector_storage_strict_mode_upsert_no_local_shard(tmp_path: pathlib.Path):
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_urls[0], collection=COLLECTION_NAME, shard_number=1, replication_factor=N_REPLICAS, sharding_method="custom")

    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)

    collection_info = get_cluster_info(peer_urls[0])
    non_leader = 0
    for peer_id, peer_info in collection_info['peers'].items():
        peer_id = int(peer_id)
        if peer_id != int(collection_info['peer_id']):
            non_leader = peer_id
            break

    create_shard_key("non_leader", peer_urls[0], collection=COLLECTION_NAME, placement=[non_leader])

    for _ in range(32):
        point = {"id": 1, "payload": {}, "vector": random_dense_vector()}
        upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME, shard_key="non_leader").raise_for_status()

    set_strict_mode(peer_urls[0], COLLECTION_NAME, {
        "enabled": True,
        "max_collection_vector_size_bytes": 33,
    })

    wait_for_strict_mode_enabled(peer_urls[1], COLLECTION_NAME)

    for _ in range(32):
        point = {"id": 2, "payload": {}, "vector": random_dense_vector()}
        upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME, shard_key="non_leader").raise_for_status()

    for _ in range(32):
        point = {"id": 3, "payload": {}, "vector": random_dense_vector()}
        res = upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME, shard_key="non_leader")
        if not res.ok:
            assert "Max vector storage size" in res.json()['status']['error']
            assert not res.ok
            return

    raise AssertionError("Should have blocked upsert but didn't")


def test_vector_storage_strict_mode_upsert_local_shard(tmp_path: pathlib.Path):
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_urls[0], collection=COLLECTION_NAME, shard_number=N_SHARDS, replication_factor=N_REPLICAS)

    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)

    for _ in range(32):
        point = {"id": 1, "payload": {}, "vector": random_dense_vector()}
        upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME).raise_for_status()

    set_strict_mode(peer_urls[0], COLLECTION_NAME, {
        "enabled": True,
        "max_collection_vector_size_bytes": 33,
    })

    wait_for_strict_mode_enabled(peer_urls[1], COLLECTION_NAME)

    for _ in range(32):
        point = {"id": 2, "payload": {}, "vector": random_dense_vector()}
        upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME).raise_for_status()

    for _ in range(32):
        point = {"id": 3, "payload": {}, "vector": random_dense_vector()}
        res = upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME)
        if not res.ok:
            assert "Max vector storage size" in res.json()['status']['error']
            assert not res.ok
            return

    raise AssertionError("Should have blocked upsert but didn't")



def test_payload_strict_mode_upsert(tmp_path: pathlib.Path):
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, 4)

    strict_mode = {
        "enabled": True,
        "max_collection_payload_size_bytes": 8000,
    }
    create_collection(peer_urls[0], collection=COLLECTION_NAME, shard_number=4, replication_factor=N_REPLICAS, strict_mode=strict_mode)

    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)

    # Insert points into leader
    for i in range(10):
        upsert_random_points(peer_urls[0], 100, collection_name=COLLECTION_NAME, offset=i*100)

    # Check that each node blocks new points now
    for peer_url in peer_urls:
        for _ in range(32):
            point = {"id": 1001, "payload": {"city": "Berlin"}, "vector": random_dense_vector()}
            res = upsert_points(peer_url, [point], collection_name=COLLECTION_NAME)
            if not res.ok:
                assert "Max payload storage size" in res.json()['status']['error']
                return

    raise AssertionError("Should have blocked upsert but didn't")



def test_payload_strict_mode_upsert_no_local_shard(tmp_path: pathlib.Path):
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_urls[0], collection=COLLECTION_NAME, shard_number=1, replication_factor=N_REPLICAS, sharding_method="custom")

    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)

    collection_info = get_cluster_info(peer_urls[0])
    non_leader = 0
    for peer_id, peer_info in collection_info['peers'].items():
        peer_id = int(peer_id)
        if peer_id != int(collection_info['peer_id']):
            non_leader = peer_id
            break

    create_shard_key("non_leader", peer_urls[0], collection=COLLECTION_NAME, placement=[non_leader])

    payload = {"country": "Germany", "city": "Berlin"}

    # Use unique point IDs across all phases. The mmap (gridstore) payload
    # storage size estimate is bitmask-based: overwriting the same id keeps
    # old blocks allocated until a periodic flush reclaims them, which makes
    # a same-id test race the flush worker. Unique ids keep every block live,
    # so the post-flush size still reflects all inserted points.
    for i in range(32):
        point = {"id": i, "payload": payload, "vector": random_dense_vector()}
        upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME, shard_key="non_leader").raise_for_status()

    set_strict_mode(peer_urls[0], COLLECTION_NAME, {
        "enabled": True,
        "max_collection_payload_size_bytes": 10_000,
    })

    wait_for_strict_mode_enabled(peer_urls[1], COLLECTION_NAME)

    for i in range(32, 64):
        point = {"id": i, "payload": payload, "vector": random_dense_vector()}
        upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME, shard_key="non_leader").raise_for_status()

    for i in range(64, 96):
        point = {"id": i, "payload": payload, "vector": random_dense_vector()}
        res = upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME, shard_key="non_leader")
        if not res.ok:
            assert "Max payload storage size" in res.json()['status']['error']
            assert not res.ok
            return

    raise AssertionError("Should have blocked upsert but didn't")



def test_write_rate_limiting_across_node(tmp_path: pathlib.Path):
    # 2 peers with a single shard without replica to make sure that one of the node is empty
    n_peers = 2
    n_shard = 1
    n_replica = 1
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, n_peers)

    create_collection(peer_urls[0], collection=COLLECTION_NAME, shard_number=n_shard, replication_factor=n_replica)

    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)

    empty_peer = None
    for peer_url in peer_urls:
        if 0 == get_collection_local_shards_count(peer_url, COLLECTION_NAME):
            empty_peer = peer_url
            break

    # Make sure that one of the node is empty
    assert empty_peer is not None

    # No rate limiting until we enable it
    for _ in range(32):
        point = {"id": 1, "vector": random_dense_vector()}
        upsert_points(empty_peer, [point], collection_name=COLLECTION_NAME).raise_for_status()

    # Enable rate limiting
    set_strict_mode(peer_urls[0], COLLECTION_NAME, {
        "enabled": True,
        "write_rate_limit": 60,
    })

    wait_for_strict_mode_enabled(peer_urls[1], COLLECTION_NAME)

    # Rate limiting should be triggered, although we are sending requests to the empty node.
    # This proves that the rate limiting error's `retry-after` field is propagated across the cluster from the node that triggered it.
    for _ in range(120):
        point = {"id": 1, "vector": random_dense_vector()}
        response = upsert_points(empty_peer, [point], collection_name=COLLECTION_NAME)

        if not response.ok:
            print(response.json())
            assert response.status_code == 429
            assert "Rate limiting exceeded: Write rate limit exceeded" in response.json()['status']['error']
            assert response.headers['Retry-After'] is not None
            # need to wait about a second for one out of 100 tokens to be replenished
            assert 1 <= int(response.headers['Retry-After']) <= 5
            return

    raise AssertionError("rate limiter was never triggered")


def test_max_update_by_filter_limit(tmp_path: pathlib.Path):
    # Single peer / single shard so the filter resolution is fully local.
    n_peers = 1
    n_shard = 1
    n_replica = 1
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, n_peers)

    create_collection(
        peer_urls[0],
        collection=COLLECTION_NAME,
        shard_number=n_shard,
        replication_factor=n_replica,
    )

    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=peer_urls
    )

    # Upsert points that all share the same payload value, so a single filter
    # matches every one of them.
    for i in range(20):
        point = {
            "id": i,
            "vector": random_dense_vector(),
            "payload": {"city": "Berlin"},
        }
        upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME).raise_for_status()

    berlin_filter = {
        "must": [{"key": "city", "match": {"value": "Berlin"}}],
    }

    # Enable the limit: at most 10 points per update-by-filter operation.
    set_strict_mode(
        peer_urls[0],
        COLLECTION_NAME,
        {
            "enabled": True,
            "max_update_by_filter_limit": 10,
        },
    )
    wait_for_strict_mode_enabled(peer_urls[0], COLLECTION_NAME)

    # A delete by filter matching all 20 points exceeds the limit.
    res = delete_points_by_filter(peer_urls[0], berlin_filter, collection_name=COLLECTION_NAME)
    assert not res.ok
    assert "exceeding the configured limit of 10" in res.json()["status"]["error"]

    # A set-payload by filter matching all 20 points is also rejected.
    res = set_payload_by_filter(
        peer_urls[0],
        {"visited": True},
        berlin_filter,
        collection_name=COLLECTION_NAME,
    )
    assert not res.ok
    assert "exceeding the configured limit of 10" in res.json()["status"]["error"]

    # An explicit delete by ids is not filter-resolving, so it is allowed
    # regardless of the limit.
    r = requests.post(
        f"{peer_urls[0]}/collections/{COLLECTION_NAME}/points/delete?wait=true",
        json={"points": list(range(20))},
    )
    assert_http_ok(r)

    # With strict mode disabled, the same delete by filter that was rejected
    # above (20 points > limit 10) is now allowed, proving the limit is tied to
    # the enabled flag. Re-upsert the points first, since the id-based delete
    # above removed them.
    for i in range(20):
        point = {
            "id": i,
            "vector": random_dense_vector(),
            "payload": {"city": "Berlin"},
        }
        upsert_points(peer_urls[0], [point], collection_name=COLLECTION_NAME).raise_for_status()

    set_strict_mode(peer_urls[0], COLLECTION_NAME, {"enabled": False})
    wait_for_strict_mode_disabled(peer_urls[0], COLLECTION_NAME)

    res = delete_points_by_filter(
        peer_urls[0], berlin_filter, collection_name=COLLECTION_NAME
    )
    assert_http_ok(res)
