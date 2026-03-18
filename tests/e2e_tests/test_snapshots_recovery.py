import pytest
import requests
import yaml
from pathlib import Path

from e2e_tests.client_utils import ClientUtils
from e2e_tests.conftest import QdrantContainerConfig

COLLECTION_NAME = "test_collection"
RECOVERED_URL = "test_collection_recovered_url"
RECOVERED_FILE = "test_collection_recovered_file"
NUM_BATCHES = 2
EXPECTED_POINTS = NUM_BATCHES * 100  # generate_points yields 100 points per batch
INTERNAL_QDRANT_URL = "http://localhost:6333"


def _s3_config() -> dict:
    return {
        'bucket': 'test-bucket',
        'region': 'us-east-1',
        'access_key': 'minioadmin',
        'secret_key': 'minioadmin',
        'endpoint_url': 'http://host.docker.internal:9000',
    }


def _create_snapshot_config(storage_method: str, tmp_path: Path) -> Path:
    """Create a Qdrant config file with the specified snapshot storage method."""
    config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    snapshots_config = config.setdefault('storage', {}).setdefault('snapshots_config', {})
    snapshots_config['snapshots_storage'] = storage_method
    if storage_method == "s3":
        snapshots_config['s3_config'] = _s3_config()

    temp_config = tmp_path / "config.yaml"
    with open(temp_config, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    return temp_config


def _skip_if_minio_unavailable():
    """Skip the test if MinIO is not reachable on the host."""
    try:
        response = requests.get("http://127.0.0.1:9000/minio/health/live", timeout=1)
        response.raise_for_status()
    except requests.exceptions.RequestException:
        pytest.skip("MinIO is not available for S3 testing")


def _verify_recovered(client: ClientUtils, collection_name: str, expected_config):
    """Assert a recovered collection has the expected point count, config, and green status."""
    resp = client.verify_collection_exists(collection_name)
    result = resp["result"]
    assert result["status"] == "green", f"Collection {collection_name} is not in green status"
    assert result.get("points_count") == EXPECTED_POINTS, (
        f"Collection {collection_name}: expected {EXPECTED_POINTS} points, got {result.get('points_count')}"
    )
    assert result.get("config") == expected_config, (
        f"Collection {collection_name}: config mismatch"
    )


class TestSnapshotsRecovery:
    """Snapshot creation, download, and recovery with local and S3 storage."""

    @pytest.mark.parametrize("storage_method", ["local", "s3"])
    def test_snapshots_recovery(self, qdrant_container_factory, storage_method, tmp_path):
        if storage_method == "s3":
            _skip_if_minio_unavailable()

        config_file = _create_snapshot_config(storage_method, tmp_path)

        container_config = QdrantContainerConfig(
            volumes={str(config_file): {'bind': '/qdrant/config/config.yaml', 'mode': 'ro'}},
            additional_params={"extra_hosts": {"host.docker.internal": "host-gateway"}},
        )
        container_info = qdrant_container_factory(container_config)

        client = ClientUtils(host=container_info.host, port=container_info.http_port)
        client.wait_for_server()

        # Seed data
        client.create_collection(COLLECTION_NAME, {"vectors": {"size": 4, "distance": "Dot"}})
        for batch in client.generate_points(NUM_BATCHES):
            client.insert_points(COLLECTION_NAME, batch, wait=True)
        original_config = client.get_collection_info_dict(COLLECTION_NAME)["result"]["config"]

        # --- Collection snapshots ---

        snapshot_name = client.create_snapshot(COLLECTION_NAME)
        assert snapshot_name, "Snapshot creation failed"

        snapshot_content, snapshot_checksum = client.download_snapshot(COLLECTION_NAME, snapshot_name)
        assert snapshot_content, "Downloaded snapshot is empty"

        # Recover from URL (server fetches from itself via internal address)
        snapshot_url = f"{INTERNAL_QDRANT_URL}/collections/{COLLECTION_NAME}/snapshots/{snapshot_name}"
        client.recover_snapshot_from_url(RECOVERED_URL, snapshot_url, snapshot_checksum)
        _verify_recovered(client, RECOVERED_URL, original_config)

        # Recover from uploaded file
        client.upload_snapshot_file(RECOVERED_FILE, snapshot_content)
        _verify_recovered(client, RECOVERED_FILE, original_config)

        # --- Shard snapshots ---

        shard_snapshot_name = client.create_shard_snapshot(COLLECTION_NAME, 0)
        assert shard_snapshot_name, "Shard snapshot creation failed"

        shard_snapshot_content = client.download_shard_snapshot(COLLECTION_NAME, 0, shard_snapshot_name)
        assert shard_snapshot_content, "Downloaded shard snapshot is empty"

        # Recover shard from URL
        shard_url = f"{INTERNAL_QDRANT_URL}/collections/{COLLECTION_NAME}/shards/0/snapshots/{shard_snapshot_name}"
        client.recover_shard_snapshot_from_url(RECOVERED_URL, 0, shard_url)

        # Recover shard from uploaded file
        client.upload_shard_snapshot_file(RECOVERED_FILE, 0, shard_snapshot_content)

        # Final verification after all recovery operations
        _verify_recovered(client, RECOVERED_URL, original_config)
        _verify_recovered(client, RECOVERED_FILE, original_config)