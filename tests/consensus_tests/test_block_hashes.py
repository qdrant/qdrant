import requests

from tools.block_hashes import block_hashes
from .utils import (
    every_test,  # noqa: F401: autouse fixture cleans up the test's peer processes
    processes,
    start_cluster,
    wait_collection_exists_and_active_on_all_peers,
)
from .assertions import assert_http_ok


def test_distributed_block_hashes(tmp_path):
    uris, _, _ = start_cluster(tmp_path, 3)
    name = "block_hashes"
    response = requests.put(f"{uris[0]}/collections/{name}", json={
        "vectors": {"size": 4, "distance": "Dot"},
        "shard_number": 3, "replication_factor": 1,
    })
    assert_http_ok(response)
    wait_collection_exists_and_active_on_all_peers(collection_name=name, peer_api_uris=uris)
    points = [{"id": i, "vector": [0.1] * 4, "payload": {"sync": {"fingerprint": str(i)}}} for i in range(2200)]
    response = requests.put(f"{uris[0]}/collections/{name}/points?wait=true", json={"points": points[::-1]})
    assert_http_ok(response)
    expected = block_hashes(points, "sync.fingerprint", 16)
    for uri in uris:
        response = requests.post(f"{uri}/collections/{name}/points/block-hashes", json={
            "payload_key": "sync.fingerprint", "block_count": 16,
        })
        assert_http_ok(response)
        assert response.json()["result"] == expected

    # An unavailable shard must not become a successful partial summary.
    failed_peer = processes.pop()
    failed_peer.kill()
    response = requests.post(f"{uris[0]}/collections/{name}/points/block-hashes?timeout=1", json={
        "payload_key": "sync.fingerprint", "block_count": 16,
    }, timeout=10)
    assert not response.ok
    assert response.json().get("result") is None
