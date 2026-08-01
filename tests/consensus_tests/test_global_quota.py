import pathlib
from typing import Any

from consensus_tests.fixtures import create_collection, upsert_random_points

from .utils import *

N_PEERS = 3

# No real filesystem is less than 1% full, so this quota rejects every update
# that consumes disk.
UNSATISFIABLE_QUOTA = {"enabled": True, "max_disk_usage_percent": 1}


def test_global_quota(tmp_path: pathlib.Path):
    assert_project_root()

    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, N_PEERS)

    # Strict mode stays disabled for this collection throughout the test
    create_collection(peer_api_uris[0], shard_number=1, replication_factor=N_PEERS)
    wait_collection_exists_and_active_on_all_peers(
        collection_name="test_collection", peer_api_uris=peer_api_uris
    )

    # Quotas are disabled by default, so updates pass
    for peer_api_uri in peer_api_uris:
        assert get_quotas(peer_api_uri)["config"] == {"enabled": False}
    upsert_random_points(peer_api_uris[0], 10)

    # Setting the quota on one peer applies it on all of them
    set_quotas(peer_api_uris[0], UNSATISFIABLE_QUOTA)
    for peer_api_uri in peer_api_uris:
        wait_for(_quota_config_matches, peer_api_uri, UNSATISFIABLE_QUOTA)
        # Telemetry reports the quota the peer is enforcing. None of these peers
        # was started with one, so this can only come from the applied config.
        assert _telemetry_quota(peer_api_uri) == UNSATISFIABLE_QUOTA

    # Every peer now rejects disk-consuming updates, and says exactly why
    for peer_api_uri in peer_api_uris:
        error = _upsert_error(peer_api_uri)
        assert "Disk usage is at" in error, error
        assert "global quota config" in error, error

    # Deletes free disk instead of consuming it, so they stay allowed
    resp = requests.post(
        f"{peer_api_uris[0]}/collections/test_collection/points/delete?wait=true",
        json={"points": [0]},
    )
    assert_http_ok(resp)

    # The quota is persisted, so a restarted peer comes back enforcing it
    processes.pop().kill()
    restarted_api_uri = start_peer(peer_dirs[-1], "peer_restarted.log", bootstrap_uri)
    wait_for_peer_online(restarted_api_uri)
    wait_for(_quota_config_matches, restarted_api_uri, UNSATISFIABLE_QUOTA)
    assert "Disk usage is at" in _upsert_error(restarted_api_uri)

    # Disabling the quota lifts the restriction cluster-wide
    peer_api_uris = peer_api_uris[:-1] + [restarted_api_uri]
    set_quotas(peer_api_uris[0], {"enabled": False})
    for peer_api_uri in peer_api_uris:
        wait_for(_quota_config_matches, peer_api_uri, {"enabled": False})
        upsert_random_points(peer_api_uri, 1)


def get_quotas(peer_api_uri: str) -> dict[str, Any]:
    resp = requests.get(f"{peer_api_uri}/quotas")
    assert_http_ok(resp)
    return resp.json()["result"]


def set_quotas(peer_api_uri: str, config: dict[str, Any]):
    resp = requests.put(f"{peer_api_uri}/quotas?wait=true", json=config)
    assert_http_ok(resp)


def _quota_config_matches(peer_api_uri: str, expected: dict[str, Any]) -> bool:
    return get_quotas(peer_api_uri)["config"] == expected


def _telemetry_quota(peer_api_uri: str) -> dict[str, Any]:
    resp = requests.get(f"{peer_api_uri}/telemetry")
    assert_http_ok(resp)
    return resp.json()["result"]["quota"]


def _upsert_error(peer_api_uri: str) -> str:
    resp = requests.put(
        f"{peer_api_uri}/collections/test_collection/points?wait=true",
        json={"points": [{"id": 1000, "vector": [0.0] * 4}]},
    )
    assert resp.status_code == 400, resp.text
    return resp.json()["status"]["error"]
