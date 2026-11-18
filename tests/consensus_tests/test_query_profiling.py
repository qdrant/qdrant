"""Query profiling (beta): `profile=analyze|explain` and `GET /profiler/workload`.

Single node (cluster disabled) and distributed (3 peers, remote shards) setups.
"""
import pathlib
import random

import pytest
import requests
from grpc_requests import Client

from .fixtures import DENSE_VECTOR_SIZE, create_collection, upsert_random_points
from .utils import *

COLLECTION = "test_profiling"
OTHER_COLLECTION = "test_profiling_other"
PROFILING_ENV = {"QDRANT__FEATURE_FLAGS__QUERY_PROFILING": "true"}


def query_vector():
    return [random.random() for _ in range(DENSE_VECTOR_SIZE)]


def post(url, path, body, params=None):
    response = requests.post(f"{url}{path}", json=body, params=params)
    return response


def profiled(url, path, body, mode="analyze"):
    response = post(url, path, body, params={"profile": mode})
    assert response.ok, response.text
    body = response.json()
    assert "profile" in body, body
    return body["result"], body["profile"]


def all_segments(profile):
    return [segment for shard in profile["shards"] for segment in shard["segments"]]


def start_single_node(tmp_path: pathlib.Path, extra_env):
    peer_dir = make_peer_folder(tmp_path, 0)
    env = {**extra_env, "QDRANT__CLUSTER__ENABLED": "false"}
    url, _ = start_first_peer(peer_dir, "peer_0_0.log", extra_env=env)
    def ready():
        try:
            return requests.get(f"{url}/readyz").ok
        except requests.exceptions.ConnectionError:
            return False

    wait_for(ready)
    return url


def grpc_port(url: str) -> int:
    # Ports are allocated in triples: p2p, grpc, http
    return int(url.rsplit(":", 1)[1]) - 1


def setup_collection(url, collection=COLLECTION, shard_number=1, points=500):
    create_collection(url, collection=collection, shard_number=shard_number, sparse_vectors=False,
                      indexing_threshold=0)
    requests.put(f"{url}/collections/{collection}/index?wait=true",
                 json={"field_name": "city", "field_schema": "keyword"}).raise_for_status()
    upsert_random_points(url, points, collection_name=collection, with_sparse_vector=False)


def test_profile_rejected_without_feature_flag(tmp_path: pathlib.Path):
    # Debug builds enable all feature flags (config/development.yaml)
    url = start_single_node(tmp_path, {"QDRANT__FEATURE_FLAGS__ALL": "false",
                                       "QDRANT__FEATURE_FLAGS__QUERY_PROFILING": "false"})
    setup_collection(url, points=10)

    response = post(url, f"/collections/{COLLECTION}/points/query", {"query": query_vector()},
                    params={"profile": "analyze"})
    assert response.status_code == 400
    assert "query_profiling" in response.text

    # Without the parameter, nothing changes
    response = post(url, f"/collections/{COLLECTION}/points/query", {"query": query_vector()})
    assert response.ok
    assert "profile" not in response.json()

    workload = requests.get(f"{url}/profiler/workload").json()["result"]
    assert workload["enabled"] is False
    assert workload["fingerprints"] == []


def test_single_node_profile(tmp_path: pathlib.Path):
    url = start_single_node(tmp_path, PROFILING_ENV)
    setup_collection(url)
    setup_collection(url, collection=OTHER_COLLECTION, points=300)
    total_points = 500

    # Filter on an indexed key (`city`) and on an un-indexed one (`b`)
    filter_ = {"must": [{"key": "city", "match": {"any": ["London", "Paris"]}}],
               "should": [{"key": "b", "match": {"value": "x"}}, {"key": "city", "match": {"value": "London"}}]}

    # --- query, analyze
    result, profile = profiled(url, f"/collections/{COLLECTION}/points/query",
                               {"query": query_vector(), "limit": 5, "filter": filter_})
    assert len(result["points"]) > 0
    assert profile["version"] == 1
    assert profile["mode"] == "analyze"
    assert profile["total_time_ms"] > 0
    assert len(profile["shards"]) == 1
    shard = profile["shards"][0]
    assert shard["location"] == "local"
    assert shard["peer_id"] is None  # not distributed
    assert shard["network_time_ms"] is None
    segments = [s for s in all_segments(profile) if s["points"] > 0]
    assert segments
    # Only segments of this collection: no data of the other collection leaks
    assert sum(s["points"] for s in segments) == total_points
    for segment in segments:
        assert segment["operation"] == "search"
        assert segment["strategy"] is not None
        assert segment["strategy_reason"] is not None
        assert segment["filter"]["estimated_cardinality"] is not None
        assert "b" in segment["filter"]["unindexed_keys"]
        assert "city" not in segment["filter"]["unindexed_keys"]
    scores = sum(s["counters"]["vector_scores"] + s["counters"]["quantized_vector_scores"] for s in segments)
    assert scores > 0
    assert any("`b`" in note and "payload index" in note for note in profile["notes"])

    # --- query, explain: planned, not executed
    result, explain = profiled(url, f"/collections/{COLLECTION}/points/query",
                               {"query": query_vector(), "limit": 5, "filter": filter_}, mode="explain")
    assert result["points"] == []
    assert explain["mode"] == "explain"
    explain_segments = [s for s in all_segments(explain) if s["points"] > 0]
    assert [s["strategy"] for s in explain_segments] == [s["strategy"] for s in segments]
    assert all(s["counters"]["vector_scores"] == 0 for s in explain_segments)
    assert all(s["counters"]["quantized_vector_scores"] == 0 for s in explain_segments)

    # --- no profile: no profile in the response
    response = post(url, f"/collections/{COLLECTION}/points/query", {"query": query_vector(), "limit": 5})
    assert "profile" not in response.json()

    # --- other endpoints
    _, profile = profiled(url, f"/collections/{COLLECTION}/points/search", {"vector": query_vector(), "limit": 3})
    assert profile["shards"][0]["operation"] == "core_search"

    _, profile = profiled(url, f"/collections/{COLLECTION}/points/query/batch",
                          {"searches": [{"query": query_vector()}, {"query": query_vector(), "filter": filter_}]})
    assert profile["shards"][0]["operation"] == "query_batch"

    result, profile = profiled(url, f"/collections/{COLLECTION}/points/count", {"filter": filter_, "exact": True})
    read_segments = [s for s in all_segments(profile) if s["points"] > 0]
    assert all(s["operation"] == "read" for s in read_segments)
    assert sum(s["filter"]["actual_cardinality"] for s in read_segments) == result["count"]

    _, profile = profiled(url, f"/collections/{COLLECTION}/points/scroll", {"filter": filter_, "limit": 2})
    assert profile["shards"][0]["operation"] == "scroll"

    _, profile = profiled(url, f"/collections/{COLLECTION}/facet", {"key": "city"})
    assert profile["shards"][0]["operation"] == "facet"

    # Explain is not meaningful for facet, and says why
    response = post(url, f"/collections/{COLLECTION}/facet", {"key": "city"}, params={"profile": "explain"})
    assert response.status_code == 400
    assert "not supported for facet" in response.text

    # --- gRPC: same schema
    client = Client.get_by_endpoint(f"localhost:{grpc_port(url)}")
    response = client.request("qdrant.Points", "Query", {
        "collection_name": COLLECTION,
        "query": {"nearest": {"dense": {"data": query_vector()}}},
        "filter": {"must": [{"field": {"key": "city", "match": {"keywords": {"strings": ["London", "Paris"]}}}}],
                   "should": [{"field": {"key": "b", "match": {"keyword": "x"}}}]},
        "limit": 5,
        "profile": "ProfileAnalyze",
    })
    grpc_profile = response["profile"]
    assert grpc_profile["version"] == 1
    grpc_segments = [s for shard in grpc_profile["shards"] for s in shard["segments"] if int(s.get("points", 0)) > 0]
    assert sorted(s.get("segment_id", s.get("segmentId")) for s in grpc_segments) == sorted(s["segment_id"] for s in segments)
    assert any("`b`" in note for note in grpc_profile["notes"])

    response = client.request("qdrant.Points", "Query", {
        "collection_name": COLLECTION,
        "query": {"nearest": {"dense": {"data": query_vector()}}},
    })
    assert "profile" not in response

    # --- workload fingerprints: literal values stripped, calls aggregated
    for city in ["London", "Paris", "Rome"]:
        post(url, f"/collections/{COLLECTION}/points/query",
             {"query": query_vector(), "limit": 7, "filter": {"must": [{"key": "city", "match": {"value": city}}]}})
    workload = requests.get(f"{url}/profiler/workload",
                            params={"collection": COLLECTION, "sort": "calls", "limit": 100}).json()["result"]
    assert workload["enabled"] is True
    assert workload["version"] == 1
    entries = [e for e in workload["fingerprints"] if '"key": "city"' in str(e["normalized_request"]).replace("'", '"')
               and e["request_name"] == "query" and e["calls"] == 3]
    assert len(entries) == 1, workload["fingerprints"]
    entry = entries[0]
    assert entry["collection_name"] == COLLECTION
    assert entry["p95_time_ms"] >= entry["min_time_ms"]
    assert entry["mean_time_ms"] > 0
    assert entry["vector_scores"] + entry["quantized_vector_scores"] > 0
    assert "London" not in str(entry["normalized_request"])
    # Explain-mode requests are not part of the workload
    assert all(e["collection_name"] == COLLECTION for e in workload["fingerprints"])

    assert requests.delete(f"{url}/profiler/workload").ok
    assert requests.get(f"{url}/profiler/workload").json()["result"]["fingerprints"] == []


N_PEERS = 3


def test_distributed_profile(tmp_path: pathlib.Path):
    peer_urls, _, _ = start_cluster(tmp_path, N_PEERS, extra_env=PROFILING_ENV)
    setup_collection(peer_urls[0], shard_number=N_PEERS, points=600)
    wait_collection_exists_and_active_on_all_peers(collection_name=COLLECTION, peer_api_uris=peer_urls)

    cluster = get_collection_cluster_info(peer_urls[0], COLLECTION)
    local_shards = {shard["shard_id"] for shard in cluster["local_shards"]}
    remote_shards = {shard["shard_id"]: shard["peer_id"] for shard in cluster["remote_shards"]}
    assert len(remote_shards) >= 1

    filter_ = {"must": [{"key": "city", "match": {"value": "London"}}]}
    for path, body, operation in [
        ("/points/query", {"query": query_vector(), "limit": 5, "filter": filter_}, "query_batch"),
        ("/points/count", {"filter": filter_, "exact": True}, "count"),
        ("/points/scroll", {"filter": filter_, "limit": 3}, "scroll"),
        ("/facet", {"key": "city"}, "facet"),
    ]:
        _, profile = profiled(peer_urls[0], f"/collections/{COLLECTION}{path}", body)
        shards = {shard["shard_id"]: shard for shard in profile["shards"] if shard["operation"] == operation}
        assert set(shards) == local_shards | set(remote_shards), profile
        for shard_id, shard in shards.items():
            assert shard["peer_id"] is not None
            if shard_id in remote_shards:
                assert shard["location"] == "remote"
                assert shard["peer_id"] == remote_shards[shard_id]
                assert shard["network_time_ms"] is not None and shard["network_time_ms"] >= 0
                # Segment details come back from the remote peer
                assert any(s["points"] > 0 for s in shard["segments"]), shard
            else:
                assert shard["location"] == "local"
                assert shard["network_time_ms"] is None

    # Explain is forwarded: remote shards plan, but score nothing
    result, profile = profiled(peer_urls[0], f"/collections/{COLLECTION}/points/query",
                               {"query": query_vector(), "limit": 5, "filter": filter_}, mode="explain")
    assert result["points"] == []
    remote = [s for s in profile["shards"] if s["location"] == "remote"]
    assert remote
    for segment in all_segments(profile):
        assert segment["counters"]["vector_scores"] == 0
        assert segment["counters"]["quantized_vector_scores"] == 0

    # Workload is per node: the remote peers count the shard operations they executed
    for url in peer_urls[1:]:
        workload = requests.get(f"{url}/profiler/workload", params={"collection": COLLECTION}).json()["result"]
        assert workload["fingerprints"], url
