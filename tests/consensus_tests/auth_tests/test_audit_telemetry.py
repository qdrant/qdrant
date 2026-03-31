import json
import time
from pathlib import Path

import pytest
import requests

from consensus_tests import fixtures
from consensus_tests.utils import kill_all_processes, wait_for

from .utils import (
    API_KEY_HEADERS,
    READ_ONLY_API_KEY,
    REST_URI,
    SECRET,
    encode_jwt,
    start_jwt_protected_cluster,
)

COLL_NAME = "jwt_audit_telemetry_collection"
TOKEN_COLL_R = encode_jwt(
    {"access": [{"collection": COLL_NAME, "access": "r"}]}, SECRET
)


def read_audit_methods(peer_dir: Path) -> list[str]:
    methods = []
    for log_file in sorted((peer_dir / "storage" / "audit").glob("audit.*.log")):
        for line in log_file.read_text().splitlines():
            if not line.strip():
                continue
            methods.append(json.loads(line)["method"])
    return methods


def wait_for_audit_count(peer_dir: Path, expected_count: int):
    wait_for(
        lambda: len(read_audit_methods(peer_dir)) == expected_count,
        wait_for_timeout=10,
        wait_for_interval=0.1,
    )


@pytest.fixture(scope="module")
def jwt_audit_cluster(tmp_path_factory: pytest.TempPathFactory):
    tmp_path = tmp_path_factory.mktemp("jwt_audit_cluster")
    peer_api_uris, peer_dirs, bootstrap_uri = start_jwt_protected_cluster(
        tmp_path,
        extra_env={"QDRANT__AUDIT__ENABLED": "true"},
    )

    try:
        fixtures.create_collection(
            REST_URI,
            collection=COLL_NAME,
            sharding_method="custom",
            headers=API_KEY_HEADERS,
        )
        yield peer_api_uris, peer_dirs, bootstrap_uri
    finally:
        try:
            fixtures.drop_collection(REST_URI, COLL_NAME, headers=API_KEY_HEADERS)
        finally:
            kill_all_processes()


def test_telemetry_scrapes_are_not_added_to_audit_log(jwt_audit_cluster):
    peer_api_uris, peer_dirs, _ = jwt_audit_cluster
    peer_url = peer_api_uris[0]
    peer_dir = Path(peer_dirs[0])

    methods_before = read_audit_methods(peer_dir)

    logger_response = requests.get(
        f"{peer_url}/logger",
        headers={"api-key": READ_ONLY_API_KEY},
    )
    assert logger_response.status_code == 200

    wait_for_audit_count(peer_dir, len(methods_before) + 1)
    methods_after_control = read_audit_methods(peer_dir)
    assert methods_after_control[-1] == "get_logger_config"

    telemetry_response = requests.get(
        f"{peer_url}/telemetry?details_level=1",
        headers={"api-key": READ_ONLY_API_KEY},
    )
    assert telemetry_response.status_code == 200

    metrics_response = requests.get(
        f"{peer_url}/metrics",
        headers={"api-key": READ_ONLY_API_KEY},
    )
    assert metrics_response.status_code == 200

    cluster_telemetry_response = requests.get(
        f"{peer_url}/cluster/telemetry?details_level=2",
        headers={"api-key": READ_ONLY_API_KEY},
    )
    assert cluster_telemetry_response.status_code == 200

    time.sleep(1)
    assert read_audit_methods(peer_dir) == methods_after_control


def test_telemetry_access_checks_still_apply_without_audit_log(jwt_audit_cluster):
    peer_api_uris, peer_dirs, _ = jwt_audit_cluster
    peer_url = peer_api_uris[0]
    peer_dir = Path(peer_dirs[0])
    methods_before = read_audit_methods(peer_dir)
    headers = {"authorization": f"Bearer {TOKEN_COLL_R}"}

    telemetry_response = requests.get(
        f"{peer_url}/telemetry?details_level=1",
        headers=headers,
    )
    assert telemetry_response.status_code == 200
    telemetry_result = telemetry_response.json()["result"]
    assert "requests" not in telemetry_result
    assert "memory" not in telemetry_result
    assert "cluster" not in telemetry_result

    metrics_response = requests.get(
        f"{peer_url}/metrics",
        headers=headers,
    )
    assert metrics_response.status_code == 403
    assert (
        metrics_response.json()["status"]["error"]
        == "Forbidden: Global access is required"
    )

    cluster_telemetry_response = requests.get(
        f"{peer_url}/cluster/telemetry?details_level=2",
        headers=headers,
    )
    assert cluster_telemetry_response.status_code == 200

    time.sleep(1)
    assert read_audit_methods(peer_dir) == methods_before
