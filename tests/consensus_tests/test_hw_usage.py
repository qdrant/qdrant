import pathlib

from consensus_tests.fixtures import create_collection, upsert_random_points
from .utils import *

N_PEERS = 3


@pytest.mark.parametrize("test_item", [
    {"path":"query",
     "json": {"query": 123, "limit": 10, "with_payload": True,}
     },
    {"path":"scroll",
     "json": {"limit": 10, "with_payload": True,}
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
