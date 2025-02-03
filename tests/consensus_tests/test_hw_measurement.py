import logging
import pathlib

from .fixtures import create_collection, upsert_random_points, get_telemetry_hw_info, update_points_payload
from .utils import *

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
        # TODO: Add tests for vector writes

    # Upsert ~20 vectors into each shard
    upsert_random_points(peer_urls[0], N_PEERS * 20, collection_name=COLLECTION_NAME)

    # Check upsert
    for peer_idx in range(N_PEERS):
        peer_url = peer_urls[peer_idx]
        peer_hw = get_telemetry_hw_info(peer_url, COLLECTION_NAME)

        # Assert that each nodes telemetry has been increased by some bytes
        expected_delta = (19 * 5)  # ~20 vectors times avg. 5 bytes payload
        assert abs(peer_hw["payload_io_write"] - peer_hw_infos[peer_idx]["payload_io_write"]) >= expected_delta
        # TODO: also test for written vectors when implemented!

    peer_hw_infos = [get_telemetry_hw_info(x, COLLECTION_NAME) for x in peer_urls]

    total_payload_io_write_old = sum([x["payload_io_write"] for x in peer_hw_infos])

    # Update 20 points
    update_hw_data = update_points_payload(peer_urls[0], collection_name=COLLECTION_NAME, points=[x for x in range(N_PEERS*20)])["usage"]

    total_payload_io_write = 0

    # Check payload update
    for peer_idx in range(N_PEERS):
        peer_url = peer_urls[peer_idx]
        peer_hw = get_telemetry_hw_info(peer_url, COLLECTION_NAME)

        total_payload_io_write += peer_hw["payload_io_write"]

        # Assert that each nodes telemetry has been increased by some bytes
        assert abs(peer_hw["payload_io_write"] - peer_hw_infos[peer_idx]["payload_io_write"]) >= (19 * 5)

    # Check that API response hardware data is equal to the data reported in telemetry!
    assert update_hw_data['payload_io_write'] == total_payload_io_write - total_payload_io_write_old

    # TODO: also test vector updates when implemented
