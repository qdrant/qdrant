import pathlib

from .utils import *

N_PEERS = 5
N_SHARDS = 4
N_REPLICA = 2


def test_collection_sharding(tmp_path: pathlib.Path):
    assert_project_root()
    peer_dirs = make_peer_folders(tmp_path, N_PEERS)

    # Gathers REST API uris
    peer_api_uris = []

    # Start bootstrap
    (bootstrap_api_uri, bootstrap_uri) = start_first_peer(
        peer_dirs[0], "peer_0_0.log")
    peer_api_uris.append(bootstrap_api_uri)

    # Wait for leader
    leader = wait_peer_added(bootstrap_api_uri)

    # Start other peers
    for i in range(1, len(peer_dirs)):
        peer_api_uris.append(start_peer(
            peer_dirs[i], f"peer_0_{i}.log", bootstrap_uri))

    # Wait for cluster
    wait_for_uniform_cluster_status(peer_api_uris, leader)

    # Check that there are no collections on all peers
    for uri in peer_api_uris:
        r = requests.get(f"{uri}/collections")
        assert_http_ok(r)
        assert len(r.json()["result"]["collections"]) == 0

    # Create collection in first peer
    r = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection", json={
            "vectors": {
                "size": 4,
                "distance": "Dot"
            },
            "shard_number": N_SHARDS,
            "replication_factor": N_REPLICA,
        })
    assert_http_ok(r)

    # Check that it exists on all peers
    wait_for_uniform_collection_existence("test_collection", peer_api_uris)

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
