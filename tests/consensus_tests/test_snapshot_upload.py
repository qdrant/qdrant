import pathlib

import requests
from .fixtures import create_collection, upsert_random_points, random_dense_vector, search, random_sparse_vector
from .utils import *
from .assertions import assert_http_ok
import time

N_PEERS = 3
N_SHARDS = 3
COLLECTION_NAME = "test_collection"


def create_snapshot(peer_api_uri):
    r = requests.post(
        f"{peer_api_uri}/collections/{COLLECTION_NAME}/snapshots")
    assert_http_ok(r)
    return r.json()["result"]["name"]


def get_peer_id(peer_api_uri):
    r = requests.get(f"{peer_api_uri}/cluster")
    assert_http_ok(r)
    return r.json()["result"]["peer_id"]


def get_local_shards(peer_api_uri):
    r = requests.get(f"{peer_api_uri}/collections/{COLLECTION_NAME}/cluster")
    assert_http_ok(r)
    return r.json()["result"]["local_shards"]


def get_remote_shards(peer_api_uri):
    r = requests.get(f"{peer_api_uri}/collections/{COLLECTION_NAME}/cluster")
    assert_http_ok(r)
    return r.json()["result"]["remote_shards"]


def upload_snapshot(peer_api_uri, snapshot_path):
    with open(snapshot_path, "rb") as f:
        print(f"uploading {snapshot_path} to {peer_api_uri}")
        snapshot_file = {"snapshot": f}
        r = requests.post(
            f"{peer_api_uri}/collections/{COLLECTION_NAME}/snapshots/upload",
            files=snapshot_file,
        )
        assert_http_ok(r)
        return r.json()["result"]


def test_upload_snapshot_1(tmp_path: pathlib.Path):
    recover_from_uploaded_snapshot(tmp_path, 1)


def test_upload_snapshot_2(tmp_path: pathlib.Path):
    recover_from_uploaded_snapshot(tmp_path, 2)


def recover_from_uploaded_snapshot(tmp_path: pathlib.Path, n_replicas):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    create_collection(
        peer_api_uris[0], shard_number=N_SHARDS, replication_factor=n_replicas
    )
    wait_collection_exists_and_active_on_all_peers(
        collection_name="test_collection", peer_api_uris=peer_api_uris
    )
    upsert_random_points(peer_api_uris[0], 100)

    query_city = "London"

    dense_query_vector = random_dense_vector()
    dense_search_result = search(peer_api_uris[0], dense_query_vector, query_city)
    assert len(dense_search_result) > 0

    sparse_query_vector = {"name": "sparse-text", "vector": random_sparse_vector()}
    sparse_search_result = search(peer_api_uris[0], sparse_query_vector, query_city)
    assert len(sparse_search_result) > 0

    snapshot_name = create_snapshot(peer_api_uris[-1])
    assert snapshot_name is not None

    # move file
    snapshot_path = Path(peer_dirs[-1]) / \
        "snapshots" / COLLECTION_NAME / snapshot_name
    assert snapshot_path.exists()

    process_peer_id = get_peer_id(peer_api_uris[-1])
    local_shards = get_local_shards(peer_api_uris[-1])

    # Kill last peer
    p = processes.pop()
    p.kill()

    # Remove last peer from cluster
    res = requests.delete(
        f"{peer_api_uris[0]}/cluster/peer/{process_peer_id}?force=true"
    )
    assert_http_ok(res)

    new_peer_dir = make_peer_folder(tmp_path, N_PEERS + 1)
    new_url = start_peer(
        new_peer_dir, f"peer_snapshot_{N_PEERS + 1}.log", bootstrap_uri
    )

    # Wait node is up and synced
    while True:
        try:
            res = requests.get(f"{new_url}/collections")
        except requests.exceptions.ConnectionError:
            time.sleep(1)
            continue
        if not res.ok:
            time.sleep(1)  # Wait to node is up
            continue
        collections = set(
            collection["name"] for collection in res.json()["result"]["collections"]
        )
        if COLLECTION_NAME not in collections:
            time.sleep(1)  # Wait to sync with consensus
            continue
        break

    # Recover snapshot
    # All nodes share the same snapshot directory, so it is fine to use any

    print(f"Recovering snapshot {snapshot_path} on {new_url}")
    upload_snapshot(new_url, snapshot_path)

    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME, peer_api_uris=[new_url]
    )

    new_local_shards = get_local_shards(new_url)

    new_local_shards = sorted(new_local_shards, key=lambda x: x["shard_id"])
    local_shards = sorted(local_shards, key=lambda x: x["shard_id"])

    assert len(new_local_shards) == len(local_shards)
    for i in range(len(new_local_shards)):
        assert new_local_shards[i] == local_shards[i]

    # check that the dense vectors are still the same
    new_dense_search_result = search(new_url, dense_query_vector, query_city)
    assert len(new_dense_search_result) == len(dense_search_result)
    for i in range(len(new_dense_search_result)):
        assert new_dense_search_result[i] == dense_search_result[i]

    # check that the sparse vectors are still the same
    new_sparse_search_result = search(new_url, sparse_query_vector, query_city)
    assert len(new_sparse_search_result) == len(sparse_search_result)
    for i in range(len(new_sparse_search_result)):
        # skip score check because it is not deterministic
        new_sparse_search_result[i].score = sparse_search_result[i].score
        assert new_sparse_search_result[i] == sparse_search_result[i]

    peer_0_remote_shards_new = get_remote_shards(peer_api_uris[0])
    for shard in peer_0_remote_shards_new:
        print("remote shard", shard)
        assert shard["state"] == "Active"
    assert len(peer_0_remote_shards_new) == 2 * n_replicas
