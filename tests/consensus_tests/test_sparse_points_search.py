import pathlib
from time import sleep

from .utils import *
from .assertions import assert_http_ok

N_PEERS = 5
N_SHARDS = 4
N_REPLICA = 1  # replicas introduces non-determinism in sparse scores


def test_sparse_points_search(tmp_path: pathlib.Path):
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
        r_batch = requests.get(f"{uri}/collections")
        assert_http_ok(r_batch)
        assert len(r_batch.json()["result"]["collections"]) == 0

    # Create collection in first peer
    r_batch = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection", json={
            "sparse_vectors": {
                "text": {}
            },
            "shard_number": N_SHARDS,
            "replication_factor": N_REPLICA,
        })
    assert_http_ok(r_batch)

    # Check that it exists on all peers
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)

    # Check collection's cluster info
    collection_cluster_info = get_collection_cluster_info(peer_api_uris[0], "test_collection")
    assert collection_cluster_info["shard_count"] == N_SHARDS

    # Create points in first peer's collection
    r_batch = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection/points?wait=true", json={
            "points": [
                {
                    "id": 1,
                    "vector": {
                        "text": {
                            "values": [0.05, 0.61, 0.76, 0.74],
                            "indices": [0, 1, 2, 3]
                        }
                    },
                    "payload": {
                        "city": "Berlin",
                        "country": "Germany",
                        "count": 1000000,
                        "square": 12.5,
                        "coords": {"lat": 1.0, "lon": 2.0}
                    }
                },
                {
                    "id": 2,
                    "vector": {
                        "text": {
                            "values": [0.19, 0.81, 0.75, 0.11],
                            "indices": [0, 1, 2, 3]
                        }
                    },
                    "payload": {"city": ["Berlin", "London"]}
                },
                {
                    "id": 3,
                    "vector": {
                        "text": {
                            "values": [0.36, 0.55, 0.47, 0.94],
                            "indices": [0, 1, 2, 3]
                        }
                    },
                    "payload": {"city": ["Berlin", "Moscow"]}
                },
                {
                    "id": 4,
                    "vector": {
                        "text": {
                            "values": [0.18, 0.01, 0.85, 0.80],
                            "indices": [0, 1, 2, 3]
                        }
                    },
                    "payload": {"city": ["London", "Moscow"]}
                },
                {
                    "id": 5,
                    "vector": {
                        "text": {
                            "values": [0.24, 0.18, 0.22, 0.44],
                            "indices": [0, 1, 2, 3]
                        }
                    },
                    "payload": {"count": [0]}
                },
                {
                    "id": 6,
                    "vector": {
                        "text": {
                            "values": [0.35, 0.08, 0.11, 0.44],
                            "indices": [0, 1, 2, 3]
                        }
                    }
                },
                {
                    "id": 7,
                    "vector": {
                        "text": {
                            "values": [0.45, 0.07, 0.21, 0.04],
                            "indices": [0, 1, 2, 3]
                        }
                    }
                },
                {
                    "id": 8,
                    "vector": {
                        "text": {
                            "values": [0.75, 0.18, 0.91, 0.48],
                            "indices": [0, 1, 2, 3]
                        }
                    }
                },
                {
                    "id": 9,
                    "vector": {
                        "text": {
                            "values": [0.30, 0.01, 0.1, 0.12],
                            "indices": [0, 1, 2, 3]
                        }
                    }
                },
                {
                    "id": 10,
                    "vector": {
                        "text": {
                            "values": [0.95, 0.8, 0.17, 0.19],
                            "indices": [0, 1, 2, 3]
                        }
                    }
                }
            ]
        })
    assert_http_ok(r_batch)

    # Check that 'search' & `search_batch` return the same results on all peers
    q = {
        "vector": {
            "name": "text",
            "vector": {
                "values": [0.2, 0.1, 0.9, 0.7],
                "indices": [0, 1, 2, 3]
            }
        },
        "top": 3,
        "offset": 1,
        "with_vector": True,
        "with_payload": True,
        "score_threshold": 0.1
    }

    # Capture result from first peer
    r_init_search = requests.post(
        f"{peer_api_uris[0]}/collections/test_collection/points/search", json=q
    ).json()["result"]

    for uri in peer_api_uris:
        r_search = requests.post(
            f"{uri}/collections/test_collection/points/search", json=q
        )
        assert_http_ok(r_search)

        r_batch = requests.post(
            f"{uri}/collections/test_collection/points/search/batch", json={
                "searches": [q]
            }
        )
        assert_http_ok(r_batch)

        # only one search in the batch
        assert len(r_batch.json()["result"]) == 1
        # assert same number of results
        assert len(r_search.json()["result"]) == len(r_batch.json()["result"][0])
        # assert stable across peers
        assert r_search.json()["result"] == r_init_search
        # search equivalent to single batch
        assert r_search.json()["result"] == r_batch.json()["result"][0]

    # Check that `search_batch` return the same results on all peers for duplicated searches
    for uri in peer_api_uris:
        r_batch = requests.post(
            f"{uri}/collections/test_collection/points/search/batch", json={
                "searches": [q, q, q, q]
            }
        )
        assert_http_ok(r_batch)

        # assert num searches
        assert len(r_batch.json()["result"]) == 4
        # assert the search limit
        assert len(r_batch.json()["result"][0]) == 3
        assert len(r_batch.json()["result"][1]) == 3
        assert len(r_batch.json()["result"][2]) == 3
        assert len(r_batch.json()["result"][3]) == 3

        assert r_batch.json()["result"] == [r_init_search, r_init_search, r_init_search, r_init_search]

    # Check that `search_batch` return the same results on all peers compared to multiple searches
    q1 = {
        "vector": {
            "name": "text",
            "vector": {
                "values": [0.2, 0.1, 0.9, 0.7],
                "indices": [0, 1, 2, 3]
            }
        },
        "top": 3,
        "offset": 1,
        "with_vector": True
    }
    q2 = {
        "vector": {
            "name": "text",
            "vector": {
                "values": [0.2, 0.1, 0.9, 0.7],
                "indices": [0, 1, 2, 3]
            }
        },
        "top": 5,
        "offset": 3,
        "with_payload": True,
    }
    q3 = {
        "vector": {
            "name": "text",
            "vector": {
                "values": [0.2, 0.1, 0.9, 0.7],
                "indices": [0, 1, 2, 3]
            }
        },
        "top": 10,
        "score_threshold": 1.1
    }
    for uri in peer_api_uris:
        r_batch = requests.post(
            f"{uri}/collections/test_collection/points/search/batch", json={
                "searches": [q1, q2, q3]
            }
        )
        assert_http_ok(r_batch)

        r_search_1 = requests.post(
            f"{uri}/collections/test_collection/points/search", json=q1
        )
        assert_http_ok(r_search_1)
        r_search_2 = requests.post(
            f"{uri}/collections/test_collection/points/search", json=q2
        )
        assert_http_ok(r_search_2)

        r_search_3 = requests.post(
            f"{uri}/collections/test_collection/points/search", json=q3
        )
        assert_http_ok(r_search_3)

        accumulated = [r_search_1.json()["result"], r_search_2.json()["result"], r_search_3.json()["result"]]
        assert accumulated == r_batch.json()["result"]
