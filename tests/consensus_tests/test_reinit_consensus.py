import pathlib
from operator import itemgetter
from random import randrange
from time import sleep

from .fixtures import create_collection, drop_collection
from .utils import *

N_PEERS = 2
N_SHARDS = 2
N_REPLICAS = 1
N_COLLECTION_LOOPS = 3
COLLECTION_NAME = "test_collection"
POINTS_JSON = {
    "points": [
        {"id": 1, "vector": [0.05, 0.61, 0.76, 0.74], "payload": {"city": "Berlin"}},
        {"id": 2, "vector": [0.19, 0.81, 0.75, 0.11], "payload": {"city": "London"}},
        {"id": 3, "vector": [0.36, 0.55, 0.47, 0.94], "payload": {"city": "Paris"}},
        {"id": 4, "vector": [0.18, 0.01, 0.85, 0.80], "payload": {"city": "Malaga"}},
        {"id": 5, "vector": [0.24, 0.18, 0.22, 0.44], "payload": {"city": "New York"}},
        {"id": 6, "vector": [0.35, 0.08, 0.11, 0.44], "payload": {"city": "Munich"}},
        {"id": 7, "vector": [0.45, 0.07, 0.21, 0.04], "payload": {"city": "Madrid"}},
        {"id": 8, "vector": [0.75, 0.18, 0.91, 0.48], "payload": {"city": "Prague"}},
        {"id": 9, "vector": [0.30, 0.01, 0.10, 0.12], "payload": {"city": "Bratislava"}},
        {"id": 10, "vector": [0.95, 0.8, 0.17, 0.19], "payload": {"city": "Tokyo"}},
    ]
}


def get_collection_points(uri):
    req_json = {
        "limit": len(POINTS_JSON["points"]),
        "with_payload": True,
        "with_vector": True
    }
    res = requests.post(
        f"{uri}/collections/{COLLECTION_NAME}/points/scroll", json=req_json
    )
    assert_http_ok(res)
    return res.json()["result"]["points"]


def compare_points(uri):
    original_points = POINTS_JSON["points"]
    fetched_points = get_collection_points(uri)
    original_points, fetched_points = [sorted(l, key=itemgetter('id')) for l in (original_points, fetched_points)]
    pairs = zip(original_points, fetched_points)
    diff = [(x, y) for x, y in pairs if x != y]
    assert len(diff) == 0, f'Original and final points are not equal, diff: "{diff}"'


def test_reinit_consensus(tmp_path: pathlib.Path):
    assert_project_root()
    peer_urls, peer_dirs, bootstrap_url = start_cluster(tmp_path, N_PEERS)

    create_collection(peer_urls[0], shard_number=N_SHARDS, replication_factor=N_REPLICAS)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)

    for i in range(N_COLLECTION_LOOPS):
        drop_collection(peer_urls[randrange(N_PEERS)], collection=COLLECTION_NAME)

        create_collection(peer_urls[randrange(N_PEERS)], shard_number=N_SHARDS, replication_factor=N_REPLICAS)
        wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls)

        r_batch = requests.put(
            f"{peer_urls[randrange(N_PEERS)]}/collections/{COLLECTION_NAME}/points?wait=true", json=POINTS_JSON)
        assert_http_ok(r_batch)

        # assert data in both shards
        for peer_api_uri in peer_urls:
            info = get_collection_cluster_info(peer_api_uri, COLLECTION_NAME)
            for shard in info["local_shards"]:
                assert shard["points_count"] > 0

    print("Stop the peers, keep data")
    for _ in range(N_PEERS):
        processes.pop().kill()

    print("Start the peers with new urls")
    peer_urls_new = []
    (bootstrap_api_uri, bootstrap_uri) = start_first_peer(peer_dirs[0], "peer_0_restarted.log", reinit=True)
    peer_urls_new.append(bootstrap_api_uri)
    leader = wait_peer_added(bootstrap_api_uri, expected_size=2)
    for i in range(1, len(peer_dirs)):
        peer_urls_new.append(start_peer(peer_dirs[i], f"peer_{i}_restarted.log", bootstrap_uri, reinit=True))
    wait_for_uniform_cluster_status(peer_urls_new, leader)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls_new)

    compare_points(peer_urls_new[0])

    print("Add one more peer")
    peer_dir_additional = make_peer_folder(tmp_path, N_PEERS)
    peer_dirs.append(peer_dir_additional)
    peer_url_additional = start_peer(peer_dir_additional, f"peer_{N_PEERS}_additional.log", bootstrap_uri)
    wait_peer_added(bootstrap_api_uri, expected_size=3)
    peer_urls_new.append(peer_url_additional)
    wait_for_uniform_cluster_status(peer_urls_new, leader)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION_NAME, peer_api_uris=peer_urls_new)
    compare_points(peer_urls_new[1])
