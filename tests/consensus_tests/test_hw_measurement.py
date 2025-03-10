import logging
import pathlib

from .fixtures import create_collection, upsert_random_points, get_telemetry_hw_info, update_points_payload, \
    update_points_vector
from .utils import *
from math import ceil

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

N_PEERS = 4
N_SHARDS = N_PEERS
N_REPLICAS = 1
COLLECTION_NAME = "test_collection_hw_counting"


def test_measuring_hw_for_updates(tmp_path: pathlib.Path):
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS)
    create_collection(peer_urls[0], collection=COLLECTION_NAME, shard_number=N_SHARDS, replication_factor=N_REPLICAS)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)
    upsert_random_points(peer_urls[0], 200, collection_name=COLLECTION_NAME)

    peer_hw_infos = [get_telemetry_hw_info(x, COLLECTION_NAME) for x in peer_urls]

    # Check initial insertion IO measurements are reported in telemetry
    for i in peer_hw_infos:
        assert i["payload_io_write"] > 0
        assert i["vector_io_write"] > 0

    # Upsert ~20 vectors into each shard
    upsert_random_points(peer_urls[0], N_SHARDS * 20, collection_name=COLLECTION_NAME)

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
    create_collection(peer_urls[0], collection=COLLECTION_NAME, shard_number=N_SHARDS, replication_factor=N_REPLICAS)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)

    check_collection_points_count(peer_urls[0], COLLECTION_NAME, 0)

    upsert_vectors = 50
    total_vectors = upsert_vectors * N_PEERS  # 200 vectors

    # Upsert 200 points without waiting
    upsert_random_points(peer_urls[0], total_vectors, collection_name=COLLECTION_NAME, wait="false")

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
        assert False, f"Assertion {inp} being below upperbound error of {upper_bound_error_percent}(={upper_bound}) failed."
