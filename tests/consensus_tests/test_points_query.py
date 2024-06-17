import pathlib

from .utils import *
from .assertions import assert_http_ok

N_PEERS = 5
N_SHARDS = 4
N_REPLICA = 2


def test_points_query(tmp_path: pathlib.Path):
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
        r_two = requests.get(f"{uri}/collections")
        assert_http_ok(r_two)
        assert len(r_two.json()["result"]["collections"]) == 0

    # Create collection in first peer
    r_two = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection", json={
            "vectors": {
                "size": 4,
                "distance": "Dot"
            },
            "shard_number": N_SHARDS,
            "replication_factor": N_REPLICA,
        })
    assert_http_ok(r_two)

    # add index on count field
    r_index = requests.put(
        f"{peer_api_uris[0]}/collections/test_collection/index?wait=true", json={
            "field_name": "count",
            "field_schema": "integer"
        })
    assert_http_ok(r_index)

    # Check that it exists on all peers
    wait_collection_exists_and_active_on_all_peers(collection_name="test_collection", peer_api_uris=peer_api_uris)

    # Check collection's cluster info
    collection_cluster_info = get_collection_cluster_info(peer_api_uris[0], "test_collection")
    assert collection_cluster_info["shard_count"] == N_SHARDS

    # Create points in first peer's collection
    r_two = requests.put(
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
                        "coords": {
                            "lat": 1.0,
                            "lon": 2.0
                        }
                    }
                },
                {
                    "id": 2,
                    "vector": [0.19, 0.81, 0.75, 0.11],
                    "payload": {
                        "city": ["Berlin", "London"]
                    }
                },
                {
                    "id": 3,
                    "vector": [0.36, 0.55, 0.47, 0.94],
                    "payload": {
                        "city": ["Berlin", "Moscow"],
                        "count": 2,

                    }
                },
                {
                    "id": 4,
                    "vector": [0.18, 0.01, 0.85, 0.80],
                    "payload": {
                        "city": ["London", "Moscow"]
                    }
                },
                {
                    "id": 5,
                    "vector": [0.24, 0.18, 0.22, 0.44],
                    "payload": {
                        "count": 1,
                    }
                },
                {
                    "id": 6,
                    "vector": [0.35, 0.08, 0.11, 0.44],
                    "payload": {
                        "count": 4,
                    }
                },
                {
                    "id": 7,
                    "vector": [0.45, 0.07, 0.21, 0.04],
                    "payload": {
                        "count": 2,
                    }
                },
                {
                    "id": 8,
                    "vector": [0.75, 0.18, 0.91, 0.48]
                },
                {
                    "id": 9,
                    "vector": [0.30, 0.01, 0.1, 0.12],
                    "payload": {
                        "count": 3,
                    }
                },
                {
                    "id": 10,
                    "vector": [0.95, 0.8, 0.17, 0.19],
                    "payload": {
                        "count": 3,
                    }
                }
            ]
        })
    assert_http_ok(r_two)

    # a filter to reuse in multiple requests
    filter = {
        "must_not": [
            {
                "key": "city",
                "match": {
                    "value": "Berlin"
                }
            }
        ]
    }

    # pairs of requests that should produce the same results
    # each element is ("request path", "extract key", "request body")
    list_of_equivalences = [
        # nearest search & query with filter
        (
            ("search", None, {
                "vector": [0.2, 0.1, 0.9, 0.7],
                "limit": 5,
                "offset": 1,
                "filter": filter,
                "with_vector": True,
                "with_payload": True,
                "score_threshold": 0.5
            }),
            ("query", None,{
                "query": [0.2, 0.1, 0.9, 0.7],
                "limit": 5,
                "offset": 1,
                "filter": filter,
                "with_vector": True,
                "with_payload": True,
                "score_threshold": 0.5
            })
        ),
        # recommend & query recommend
        (
            ("recommend", None, {
                "positive": [1, 2, 3, 4],
                "negative": [3],
                "limit": 5,
            }),
            ("query", None, {
                "query":  {
                    "recommend": {
                        "positive": [1, 2, 3, 4],
                        "negative": [3],
                    }
                },
                "limit": 5,
            })
        ),
        # discover & query discover
        (
            ("discover", None, {
                "target": 2,
                "context": [{"positive": 3, "negative": 4}],
                "limit": 5,
            }),
            ("query", None, {
                "query":  {
                    "discover": {
                        "target": 2,
                        "context": [{"positive": 3, "negative": 4}],
                    }
                },
                "limit": 5,
            })
        ),
        # context & query context
        (
            ("discover", None, {
                "context": [{"positive": 2, "negative": 4}],
                "limit": 5,
            }),
            ("query", None, {
                "query":  {
                    "context": [{"positive": 2, "negative": 4}]
                },
                "limit": 5,
            })
        ),
        # request filter & source filters
        (
            ("query", None, {
                "prefetch": [
                    {
                        "query": [0.2, 0.1, 0.9, 0.7],
                        "filter": filter,
                    }
                ],
                "query": {"fusion": "rrf"},
                "limit": 5,
                "offset": 1,
                "with_vector": True,
                "with_payload": True,
                "score_threshold": 0.5
            }),
            ("query", None, {
                "prefetch": [
                    {
                        "query": [0.2, 0.1, 0.9, 0.7],
                    }
                ],
                "query": {"fusion": "rrf"},
                "limit": 5,
                "offset": 1,
                "filter": filter,
                "with_vector": True,
                "with_payload": True,
                "score_threshold": 0.5
            })
        ),
        # TODO(universal-query) uncomment when order_by aggregation is fixed
        # (
        #     # scroll
        #     ("scroll", "points.id", {
        #         "filter": filter,
        #         "limit": 5,
        #     }),
        #     ("query", "id", {
        #         "filter": filter,
        #         "limit": 5,
        #         "with_payload": True,
        #     }),
        # ),
        # (
        #     # scroll order by `asc`
        #     ("scroll", "points.id", {
        #         "filter": filter,
        #         "limit": 5,
        #         "order_by": "count",
        #         "direction": "asc",
        #     }),
        #     ("query", "id", {
        #         "filter": filter,
        #         "limit": 5,
        #         "order_by": "count",
        #         "direction": "asc",
        #     }),
        # ),
        # (
        #     # scroll order by `desc`
        #     ("scroll", "points.id", {
        #         "filter": filter,
        #         "limit": 5,
        #         "order_by": "count",
        #         "direction": "desc",
        #     }),
        #     ("query", "id", {
        #         "filter": filter,
        #         "limit": 5,
        #         "order_by": "count",
        #         "direction": "desc",
        #     }),
        # )
    ]

    # Verify that the results are the same across all peers
    for (action1, extract1, body1), (action2, extract2, body2) in list_of_equivalences:
        # Capture result from first peer
        r_init_one = requests.post(
            f"{peer_api_uris[0]}/collections/test_collection/points/{action1}",
            params={"consistency":"all"},
            json=body1
        )
        assert_http_ok(r_init_one)
        r_init_one = r_init_one.json()["result"]
        if extract1:
            r_init_one = apply_json_path(r_init_one, extract1)

        # Loop through all peers
        for uri in peer_api_uris:
            # first request
            r_one = requests.post(
                f"{uri}/collections/test_collection/points/{action1}",
                params={"consistency":"all"},
                json=body1
            )
            assert_http_ok(r_one)
            r_one = r_one.json()["result"]
            if extract1:
                r_one = apply_json_path(r_one, extract1)

            # second request
            r_two = requests.post(
                f"{uri}/collections/test_collection/points/{action2}",
                params={"consistency":"all"},
                json=body2
            )
            assert_http_ok(r_two)
            r_two = r_two.json()["result"]
            if extract2:
                r_two = apply_json_path(r_two, extract2)

            # assert same number of results
            assert len(r_one) == len(r_two), f"Different number of results for {action1} and {action2}"
            # search equivalent results
            assert set(str(d) for d in r_one) == set(str(d) for d in r_two), f"Different results for {action1} and {action2}"
            # assert stable across peers
            assert set(str(d) for d in r_one) == set(str(d) for d in r_init_one), f"Different results for {action1} and {action2}"

def apply_json_path(json_obj, json_path):
    if json_path is None:
        return json_obj
    for key in json_path.split("."):
        if isinstance(json_obj, list):
            # return [apply_json_path(item, key) for item in json_obj]
            return [item[key] for item in json_obj]
        json_obj = json_obj[key]
    return json_obj