import json
import uuid

import pytest
import requests

from tools.block_hashes import block_hashes, slice_point_id_hash
from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation, qdrant_host_headers
from .helpers.settings import QDRANT_HOST


@pytest.fixture(autouse=True, scope="module")
def setup(collection_name):
    drop_collection(collection_name)
    response = request_with_validation(
        api="/collections/{collection_name}", method="PUT",
        path_params={"collection_name": collection_name},
        body={"vectors": {"size": 2, "distance": "Dot"}, "shard_number": 3},
    )
    assert response.ok
    yield
    drop_collection(collection_name)


def hashes(collection_name, count=16, filter=None):
    body = {"payload_key": "sync.fingerprint", "block_count": count}
    if filter is not None:
        body["filter"] = filter
    return request_with_validation(
        api="/collections/{collection_name}/points/block-hashes", method="POST",
        path_params={"collection_name": collection_name}, body=body,
    )


def upsert(collection_name, points):
    response = request_with_validation(
        api="/collections/{collection_name}/points", method="PUT",
        path_params={"collection_name": collection_name}, query_params={"wait": "true"},
        body={"points": [dict(p, vector=[0.1, 0.2]) for p in points]},
    )
    assert response.ok


def delete(collection_name, ids):
    response = request_with_validation(
        api="/collections/{collection_name}/points/delete", method="POST",
        path_params={"collection_name": collection_name}, query_params={"wait": "true"},
        body={"points": ids},
    )
    assert response.ok


def test_reconciliation(collection_name):
    assert hashes(collection_name).json()["result"] == block_hashes([], "sync.fingerprint", 16)
    points = [
        {"id": i if i % 2 == 0 else str(uuid.UUID(int=i)),
         "payload": {"tenant": "a" if i % 3 else "b", "sync": {"fingerprint": f"é\0雪:{i}"}}}
        for i in range(2305)
    ]
    upsert(collection_name, points[::-1])
    expected = block_hashes(points, "sync.fingerprint", 16)
    assert hashes(collection_name).json()["result"] == expected
    assert hashes(collection_name).json()["result"] == expected

    scope = {"key": "tenant", "match": {"value": "a"}}
    scoped = [p for p in points if p["payload"]["tenant"] == "a"]
    assert hashes(collection_name, filter={"must": [scope]}).json()["result"] == block_hashes(scoped, "sync.fingerprint", 16)
    parent = {"slice": {"total": 16, "index": 3}}
    refined = [p for p in scoped if slice_point_id_hash(p["id"]) % 16 == 3]
    result = hashes(collection_name, 64, {"must": [scope, parent]}).json()["result"]
    assert result == block_hashes(refined, "sync.fingerprint", 64)
    assert all(b["point_count"] == 0 for b in result["blocks"] if b["block_id"] not in (3, 19, 35, 51))

    response = request_with_validation(
        api="/collections/{collection_name}/points/scroll", method="POST",
        path_params={"collection_name": collection_name},
        body={"filter": {"must": [scope, {"slice": {"total": 64, "index": 19}}]},
              "with_payload": ["sync.fingerprint"], "with_vector": False, "limit": 1000},
    )
    records = response.json()["result"]["points"]
    assert {str(p["id"]) for p in records} == {str(p["id"]) for p in refined if slice_point_id_hash(p["id"]) % 64 == 19}
    assert all("vector" not in p for p in records)

    # Swapping values must be detected even though the multiset of values is unchanged.
    changed = json.loads(json.dumps(points))
    changed[0]["payload"]["sync"], changed[1]["payload"]["sync"] = changed[1]["payload"]["sync"], changed[0]["payload"]["sync"]
    upsert(collection_name, changed[:2])
    assert hashes(collection_name).json()["result"] == block_hashes(changed, "sync.fingerprint", 16)
    assert hashes(collection_name).json()["result"] != expected
    delete(collection_name, [changed.pop()["id"]])
    assert hashes(collection_name).json()["result"] == block_hashes(changed, "sync.fingerprint", 16)
    changed.append({"id": 2**64 - 1, "payload": {"sync": {"fingerprint": "new"}}})
    upsert(collection_name, changed[-1:])
    assert hashes(collection_name).json()["result"] == block_hashes(changed, "sync.fingerprint", 16)


@pytest.mark.parametrize("value", [None, 1, 1.5, True, [], ["fp"], {}])
def test_invalid_fingerprint_fails_entire_scan(collection_name, value):
    point_id = "ffffffff-ffff-ffff-ffff-ffffffffffff"
    before = hashes(collection_name).json()["result"]
    upsert(collection_name, [{"id": point_id, "payload": {"sync": {"fingerprint": value}}}])
    try:
        response = hashes(collection_name)
        assert response.status_code == 400
        assert response.json().get("result") is None
        assert hashes(collection_name, filter={"must_not": [{"has_id": [point_id]}]}).json()["result"] == before
    finally:
        delete(collection_name, [point_id])


@pytest.mark.parametrize("changes", [
    {"block_count": 0}, {"block_count": -1}, {"block_count": 65537},
    {"payload_key": ""}, {"payload_key": "sync[]"}, {"payload_key": "sync[0]"},
    {"with_points": True}, {"filter": {"must": [{"slice": {"total": 4, "index": 4}}]}},
])
def test_request_validation(collection_name, changes):
    response = requests.post(
        f"{QDRANT_HOST}/collections/{collection_name}/points/block-hashes",
        json=dict({"payload_key": "sync.fingerprint", "block_count": 16}, **changes),
        headers=qdrant_host_headers(),
    )
    assert response.status_code in (400, 422)
    assert response.json().get("result") is None
