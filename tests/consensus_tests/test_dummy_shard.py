import pathlib

from requests import get, post, put, delete
from .fixtures import create_collection, upsert_random_points
from .utils import *

N_PEERS = 3
COLLECTION_NAME = "test_collection"


def test_dummy_shard_all_reads_and_writes_succeed(tmp_path: pathlib.Path):
    peer_url = start_cluster_with_corrupted_node(N_PEERS, 2, 1, tmp_path)
    read_requests(peer_url, 200)
    write_requests(peer_url, 200, 200)
    collection_snapshot_and_collection_delete(peer_url, check_failure=False)

def test_dummy_shard_all_reads_fail(tmp_path: pathlib.Path):
    peer_url = start_cluster_with_corrupted_node(N_PEERS, 1, 1, tmp_path)
    read_requests(peer_url, 500)
    collection_snapshot_and_collection_delete(peer_url)

# When first "write" request fails, it marks shard as "dead".
# `write_consistency_factor` always "capped" at the number of "alive" shards.
#
# So, when the shard is marked as "dead", `write_consistency_factor`, effectively,
# becomes 2 instead of 3, and so the following requests start to succeed...
# until the shard switches to the "partial" state. 🙈
#
# Even though we add some special handling for the `DummyShard`, the node still "flickers"
# into "partial" state and then back to "dead" state, and so it's kinda hard
# to run this test reliably. :/
@pytest.mark.skip(reason="hard to test reliably")
def test_dummy_shard_only_first_write_fails(tmp_path: pathlib.Path):
    peer_url = start_cluster_with_corrupted_node(1, N_PEERS, N_PEERS, tmp_path)
    write_requests(peer_url, 500, 200)


def start_cluster_with_corrupted_node(
    shard_number, replication_factor, write_consistency_factor, tmp_path):

    assert_project_root()

    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS)

    create_collection(
        peer_urls[0],
        shard_number=shard_number,
        replication_factor=replication_factor,
        write_consistency_factor=write_consistency_factor,
    )

    wait_collection_exists_and_active_on_all_peers(
        collection_name="test_collection",
        peer_api_uris=peer_urls,
    )

    upsert_random_points(peer_urls[0], 100)

    # Kill the last peer
    processes.pop().kill()

    # Find a local shard inside the collection
    collection_path = Path(peer_dirs[-1])/"storage"/"collections"/COLLECTION_NAME

    segments_path = next(filter(
        lambda segments: segments.exists(),
        map(lambda shard: shard/"segments", collection_path.iterdir()),
    ))

    # Find a segment inside a local shard
    segment_path = next(filter(lambda path: path.is_dir(), segments_path.iterdir()))

    # Corrupt `segment.json` file inside a segment (to trigger collection load failure)
    segment_json_path = segment_path/"segment.json"

    with open(segment_json_path, "a") as segment_json_file:
        segment_json_file.write("borked")

    # Restart the peer
    peer_url = start_peer(peer_dirs[-1], "peer_0_restarted.log", bootstrap_url, extra_env={
        "QDRANT__STORAGE__HANDLE_COLLECTION_LOAD_ERRORS": "true"
    })

    wait_for_peer_online(peer_url)

    return peer_url

def read_requests(peer_url, expected_status):
    # Collection info
    resp = requests.get(base_url(peer_url))
    assert_http_response(resp, expected_status, "GET", f"collections/{COLLECTION_NAME}")

    TESTS = [
        (get, "points/1"),

        (post, "points", {
            "ids": [1, 2, 3],
        }),

        # TODO: Empty payload is *required* for `points/scroll`! :/
        (post, "points/scroll", {}),

        # TODO: Empty payload is *required* for `points/count`! :/
        (post, "points/count", {}),

        (post, "points/search", {
            "vector": [.1, .1, .1, .1],
            "limit": 10,
        }),

        (post, "points/search/batch", {
            "searches": [
                { "vector": [.1, .1, .1, .1], "limit": 10 },
                { "vector": [.2, .2, .2, .2], "limit": 10 },
            ]
        }),

        (post, "points/recommend", {
            "positive": [1, 2, 3],
            "limit": 10,
        }),

        (post, "points/recommend/batch", {
            "searches": [
                { "positive": [1, 2, 3], "limit": 10 },
                { "positive": [2, 3, 4], "limit": 10 },
            ]
        }),
    ]

    execute_requests(peer_url, expected_status, TESTS)

def write_requests(peer_url, first_request_expected_status, following_requests_expected_status):
    TESTS = [
        (put, "points?wait=true", {
            "points": [
                { "id": 6942, "payload": { "what": "ever" }, "vector": [.6, .9, .4, .2] },
            ]
        }),

        (put, "points?wait=true", {
            "batch": {
                "ids": [4269],
                "payloads": [{ "ever": "what" }],
                "vectors": [[.4, .2, .6, .9]],
            }
        }),

        (put, "points/payload?wait=true", {
            "points": [1, 2, 3],
            "payload": { "what": "ever" },
        }),

        (post, "points/payload?wait=true", {
            "points": [1, 2, 3],
            "payload": { "ever": "what" },
        }),

        (post, "points/payload/delete?wait=true", {
            "points": [1, 2, 3],
            "keys": ["city", "what"],
        }),

        (post, "points/payload/clear?wait=true", {
            "points": [1, 2, 3],
        }),

        (post, "points/delete?wait=true", {
            "points": [1, 2, 3],
        }),

        (put, "index", {
            "field_name": "city",
            "field_schema": "keyword",
        }),

        (delete, "index/city"),
    ]

    execute_requests(peer_url, first_request_expected_status, TESTS[:1])
    execute_requests(peer_url, following_requests_expected_status, TESTS[1:])

def collection_snapshot_and_collection_delete(peer_url, check_failure=True):
    if check_failure:
        # Create collection snapshot.
        # Expect that snapshot creation fails unless it was not recovered from another replica
        resp = requests.post(f"{base_url(peer_url)}/snapshots")
        assert_http_response(resp, 500, "POST", "snapshots")

    # Delete collection. We expect this request to succeed in all cluster configurations.
    resp = requests.delete(base_url(peer_url))
    assert_http_response(resp, 200, "DELETE", f"collections/{COLLECTION_NAME}")


def base_url(peer_url):
    return f"{peer_url}/collections/{COLLECTION_NAME}"

def execute_requests(peer_url, expected_status, tests):
    for method, url, *payload in tests:
        resp = method(
            f"{base_url(peer_url)}/{url}",
            json=payload[0] if payload else None,
        )

        assert_http_response(resp, expected_status, method.__name__.upper(), url)

def assert_http_response(resp, expected_status, method, url):
    assert expected_status == resp.status_code, \
        f"`{method} {url}` "\
        f"returned an unexpected response (expected {expected_status}, received {resp.status_code}): "\
        f"{resp.json()}"
