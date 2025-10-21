import pytest
import requests
import yaml
from pathlib import Path

from e2e_tests.client_utils import ClientUtils
from e2e_tests.conftest import QdrantContainerConfig


class TestSnapshotsRecovery:
    """Test suite for snapshot creation, download, and recovery with different storage methods."""

    @staticmethod
    def create_config_with_storage(storage_method: str, tmp_path: Path) -> Path:
        """Create a config file with the specified storage method."""
        config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Ensure snapshots_config exists
        if 'storage' not in config:
            config['storage'] = {}
        if 'snapshots_config' not in config['storage']:
            config['storage']['snapshots_config'] = {}

        # Configure storage method
        if storage_method == "s3":
            config['storage']['snapshots_config']['snapshots_storage'] = "s3"
            config['storage']['snapshots_config']['s3_config'] = {
                'bucket': 'test-bucket',
                'region': 'us-east-1',
                'access_key': 'minioadmin',
                'secret_key': 'minioadmin',
                'endpoint_url': 'http://127.0.0.1:9000'
            }
        else:
            config['storage']['snapshots_config']['snapshots_storage'] = "local"

        # Write modified config to temp file
        temp_config = tmp_path / "config.yaml"
        with open(temp_config, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)

        return temp_config

    @pytest.mark.parametrize("storage_method", ["local", "s3"])
    def test_snapshots_recovery(self, qdrant_container_factory, storage_method, tmp_path):
        """Test snapshot creation, download, and recovery with both local and S3 storage."""
        if storage_method == "s3":
            # For S3 testing, we'd need a MinIO container or mock S3
            # For now, skip if MinIO is not available
            try:
                response = requests.get("http://127.0.0.1:9000/minio/health/live", timeout=1)
                response.raise_for_status()
            except requests.exceptions.RequestException:
                pytest.skip("MinIO is not available for S3 testing")

        # Create config file with the specified storage method
        config_file = self.create_config_with_storage(storage_method, tmp_path)

        config = QdrantContainerConfig(
            network_mode="host",
            volumes={
                str(config_file): {
                    'bind': '/qdrant/config/config.yaml',
                    'mode': 'ro'
                }
            }
        )
        container_info = qdrant_container_factory(config)

        client = ClientUtils(host=container_info.host, port=container_info.http_port)
        client.wait_for_server()

        # Create collection and insert points
        collection_config = {"vectors": {"size": 4, "distance": "Dot"}}
        client.create_collection("test_collection", collection_config)
        for points_batch in client.generate_points(2):
            client.insert_points("test_collection", points_batch, wait=True)
        initial_collection_config = client.get_collection_info_dict("test_collection")["result"]["config"]

        # Create collection snapshot
        snapshot_name = client.create_snapshot("test_collection")
        assert snapshot_name, "Snapshot creation failed"

        # Download snapshot
        snapshot_content, snapshot_checksum = client.download_snapshot("test_collection", snapshot_name)
        assert len(snapshot_content) > 0, "Downloaded snapshot is empty"

        # Save snapshot to file for later use
        snapshot_file = tmp_path / "test_collection.snapshot"
        with open(snapshot_file, 'wb') as f:
            f.write(snapshot_content)

        # Test 1: Recover from URL
        snapshot_url = f"http://{client.host}:{client.port}/collections/test_collection/snapshots/{snapshot_name}"
        client.recover_snapshot_from_url("test_collection_recovered_1", snapshot_url, snapshot_checksum)

        # Verify recovered collection exists
        resp = client.verify_collection_exists("test_collection_recovered_1")
        assert resp["result"].get("points_count") == 200, "Recovered collection does not have expected number of points"
        assert resp["result"].get("config") == initial_collection_config, "Recovered collection config does not match"

        # Test 2: Upload snapshot as file
        client.upload_snapshot_file("test_collection_recovered_2", snapshot_content)

        # Verify second recovered collection exists
        client.verify_collection_exists("test_collection_recovered_2")

        # Test shard snapshots
        # Create shard snapshot
        shard_snapshot_name = client.create_shard_snapshot("test_collection", 0)
        assert shard_snapshot_name, "Shard snapshot creation failed"

        # Download shard snapshot
        shard_snapshot_content = client.download_shard_snapshot("test_collection", 0, shard_snapshot_name)
        assert len(shard_snapshot_content) > 0, "Downloaded shard snapshot is empty"

        # Save shard snapshot to file
        shard_snapshot_file = tmp_path / "test_collection_shard.snapshot"
        with open(shard_snapshot_file, 'wb') as f:
            f.write(shard_snapshot_content)

        # Test 3: Recover shard from URL
        shard_snapshot_url = f"http://{client.host}:{client.port}/collections/test_collection/shards/0/snapshots/{shard_snapshot_name}"
        client.recover_shard_snapshot_from_url("test_collection_recovered_1", 0, shard_snapshot_url)

        # Test 4: Upload shard snapshot as file
        client.upload_shard_snapshot_file("test_collection_recovered_2", 0, shard_snapshot_content)

        # Final verification - ensure all collections are accessible
        for collection_name in ["test_collection_recovered_1", "test_collection_recovered_2"]:
            resp = client.verify_collection_exists(collection_name)
            assert resp["result"]["status"] == "green", f"Collection {collection_name} is not in green status"
            assert resp["result"].get(
                "points_count") == 200, "Recovered collection does not have expected number of points"
            assert resp["result"].get(
                "config") == initial_collection_config, "Recovered collection config does not match"
