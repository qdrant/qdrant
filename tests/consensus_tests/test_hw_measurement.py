import logging
import pathlib

from .fixtures import create_collection, get_telemetry_hw_info, update_points_payload, \
    update_points_vector, upsert_random_points
from .utils import *
from math import ceil

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

N_PEERS = 4
N_SHARDS = N_PEERS
N_REPLICAS = 1
COLLECTION_NAME = "test_collection_hw_counting"


def test_no_local_shard(tmp_path: pathlib.Path):
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, 2)

    create_collection(peer_urls[0], collection=COLLECTION_NAME, shard_number=1, replication_factor=N_REPLICAS, sharding_method="custom", sparse_vectors=False)

    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)

    collection_info = get_cluster_info(peer_urls[0])
    targeted_node_id = 0  # Non leader node
    for peer_id, peer_info in collection_info['peers'].items():
        peer_id = int(peer_id)
        if peer_id != int(collection_info['peer_id']):
            targeted_node_id = peer_id
            break

    targeted_node_url = peer_urls[1]
    create_shard_key("target_node", peer_urls[0], collection=COLLECTION_NAME, placement=[targeted_node_id])

    hw_before = get_telemetry_hw_info(targeted_node_url, COLLECTION_NAME)
    if hw_before is not None:
        assert hw_before['payload_io_write'] == 0
        assert hw_before['vector_io_write'] == 0

    # Targeted non-leader node has captured all measurements
    upsert_random_points(peer_urls[0], 1000, collection_name=COLLECTION_NAME, shard_key="target_node", with_sparse_vector=False)
    hw = get_telemetry_hw_info(targeted_node_url, COLLECTION_NAME)
    assert_with_upper_bound_error(hw['payload_io_write'], 1000 * 20, upper_bound_error_percent=0.2) # 1k points times ~5 bytes avg payload size
    assert_with_upper_bound_error(hw['vector_io_write'], 1000 * 4 * 4,upper_bound_error_percent=0.2)  # 1k vectors of dim 4 where each dim is 4 bytes

    # Leader still has no measurements
    leader_hw = get_telemetry_hw_info(peer_urls[0], COLLECTION_NAME)
    if hw_before is not None:
        assert leader_hw['payload_io_write'] == 0
        assert leader_hw['vector_io_write'] == 0


def test_measuring_hw_for_updates(tmp_path: pathlib.Path):
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS)
    create_collection(peer_urls[0], collection=COLLECTION_NAME, shard_number=N_SHARDS, replication_factor=N_REPLICAS, sparse_vectors=False)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)
    upsert_random_points(peer_urls[0], 200, collection_name=COLLECTION_NAME, with_sparse_vector=False)

    peer_hw_infos = [get_telemetry_hw_info(x, COLLECTION_NAME) for x in peer_urls]

    # Check initial insertion IO measurements are reported in telemetry
    for i in peer_hw_infos:
        assert i["payload_io_write"] > 0
        assert i["vector_io_write"] > 0

    # Upsert ~20 vectors into each shard
    upsert_random_points(peer_urls[0], N_SHARDS * 20, collection_name=COLLECTION_NAME, with_sparse_vector=False)

    total_vector_io_write = 0
    # Check upsert
    for peer_idx in range(N_PEERS):
        peer_url = peer_urls[peer_idx]
        peer_hw = get_telemetry_hw_info(peer_url, COLLECTION_NAME)

        approx_points_on_node = 20 - 5

        # Assert that each nodes telemetry has been increased by some bytes
        expected_payload_delta = (approx_points_on_node * 5)  # ~20 (15) vectors times avg. 5 bytes payload
        assert abs(peer_hw["payload_io_write"] - peer_hw_infos[peer_idx]["payload_io_write"]) >= expected_payload_delta

        expected_vector_delta = (approx_points_on_node * 4 * 4)  # ~20 (15) vectors times avg. 4 dim 4 bytes vector. They might not be qually distributed so we only check for 15 vectors here.
        vector_size = abs(peer_hw["vector_io_write"] - peer_hw_infos[peer_idx]["vector_io_write"])
        total_vector_io_write += vector_size
        assert vector_size >= expected_vector_delta

    # Ensure that all vectors have been taken accounted for
    assert total_vector_io_write >= N_PEERS * 20 * 4 * 4

    peer_hw_infos = [get_telemetry_hw_info(x, COLLECTION_NAME) for x in peer_urls]

    total_payload_io_write_old = sum([x["payload_io_write"] for x in peer_hw_infos])
    total_vector_io_write_old = sum([x["vector_io_write"] for x in peer_hw_infos])

    # Update 20 points
    update_payload_hw_data = update_points_payload(peer_urls[0], collection_name=COLLECTION_NAME, points=[x for x in range(N_PEERS*20)])["usage"]
    update_vectors_hw_data = update_points_vector(peer_urls[0], collection_name=COLLECTION_NAME, points=[x for x in range(N_PEERS*20)])["usage"]

    total_payload_io_write = 0
    total_vector_io_write = 0

    # Check payload update
    for peer_idx in range(N_PEERS):
        peer_url = peer_urls[peer_idx]
        peer_hw = get_telemetry_hw_info(peer_url, COLLECTION_NAME)

        total_payload_io_write += peer_hw["payload_io_write"]
        total_vector_io_write += peer_hw["vector_io_write"]

        approx_points_on_node = 20 - 5

        # Assert that each nodes telemetry has been increased by some bytes
        assert abs(peer_hw["payload_io_write"] - peer_hw_infos[peer_idx]["payload_io_write"]) >= approx_points_on_node * 5
        assert abs(peer_hw["vector_io_write"] - peer_hw_infos[peer_idx]["vector_io_write"]) >= (approx_points_on_node * 4 * 4)

    # Check that API response hardware data is equal to the data reported in telemetry!
    assert update_payload_hw_data['payload_io_write'] == total_payload_io_write - total_payload_io_write_old
    assert update_vectors_hw_data['vector_io_write'] == total_vector_io_write - total_vector_io_write_old


def test_measuring_hw_for_updates_without_waiting(tmp_path: pathlib.Path):
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS)
    create_collection(peer_urls[0], collection=COLLECTION_NAME, shard_number=N_SHARDS, replication_factor=N_REPLICAS, sparse_vectors=False)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)

    check_collection_points_count(peer_urls[0], COLLECTION_NAME, 0)

    upsert_vectors = 50
    total_vectors = upsert_vectors * N_PEERS  # 200 vectors

    # Upsert 200 points without waiting
    upsert_random_points(peer_urls[0], total_vectors, collection_name=COLLECTION_NAME, wait="false", with_sparse_vector=False)

    wait_collection_points_count(peer_urls[0], COLLECTION_NAME, total_vectors)

    total_vectors_metrics = 0

    # Check metrics getting collected on each node, despite `wait=false`.
    for peer_idx in range(N_PEERS):
        peer_url = peer_urls[peer_idx]
        peer_hw = get_telemetry_hw_info(peer_url, COLLECTION_NAME)

        approx_points_on_node = upsert_vectors - 5

        vector_writes = peer_hw["vector_io_write"]
        total_vectors_metrics += vector_writes

        # Assert that each nodes telemetry has been increased by some bytes
        assert peer_hw["payload_io_write"] >= upsert_vectors * 5  # 50 vectors on this node with payload of ~5 bytes
        assert_with_upper_bound_error(vector_writes, approx_points_on_node * 4 * 4,upper_bound_error_percent=0.25)  # ~50 (45) vectors on this node with 4 dim and 4 bytes

    # Ensure all vectors have been accounted for
    assert_with_upper_bound_error(total_vectors_metrics, total_vectors * 4 * 4)

    # TODO: also test vector updates when implemented

def assert_with_upper_bound_error(inp: int, min_value: int, upper_bound_error_percent: float = 0.05):
    """Asserts `inp` being equal to `min_value` with a max upperbound error given in percent."""
    if inp < min_value:
        assert False, f"Assertion {inp} >= {min_value} failed"

    upper_bound = ceil(float(min_value) + float(min_value) * upper_bound_error_percent)

    if inp > upper_bound:
        assert False, f"Assertion {inp} < {upper_bound} (upperbound) failed. Allowed error = {upper_bound_error_percent}"


@pytest.mark.parametrize("test_item", [
    {"path":"query",
     "json": {"query": 123, "limit": 10, "with_payload": True}
     },
    {"path":"scroll",
     "json": {"limit": 10, "with_payload": True}
     }], ids=["query", "scroll"])
def test_payload_io_read_is_within_limit(tmp_path: pathlib.Path, test_item):
    assert_project_root()
    # Start cluster
    env = {
        "QDRANT__SERVICE__HARDWARE_REPORTING" : "true"
    }
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS, extra_env=env)
    # Create collection with 50 segments and 1000 points.
    # Each point has a payload of roughly 22B.
    # With a request's limit of 10 points, the payload_io_read should be within 300B
    # Before when reading payloads in every segment it would read 11 kilobytes
    # See: <https://github.com/qdrant/qdrant/pull/6279>
    create_collection(peer_api_uris[0], default_segment_number=50, on_disk_payload=True)
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)
    upsert_random_points(peer_api_uris[0], 1000)
    wait_collection_green(peer_api_uris[0], "test_collection")

    # Send request and check payload_io_read is within limit
    res = requests.post(
        f"{peer_api_uris[0]}/collections/test_collection/points/{test_item.get('path')}",
        json=test_item.get('json')
    )
    print(res.json().get('usage'))
    payload_io_read = res.json().get('usage', {}).get('payload_io_read')
    assert payload_io_read is not None, "payload_io_read is not found in usage"
    assert payload_io_read < 300, f"payload_io_read={payload_io_read} is not within limit"