import uuid

import pytest
import requests
from pathlib import Path

from e2e_tests.conftest import QdrantContainerConfig
from e2e_tests.client_utils import ClientUtils
from e2e_tests.utils import extract_archive


class TestStorageCompatibility:
    """Test storage and snapshot compatibility with defined previous Qdrant versions."""
    
    PREV_PATCH_VERSION = "v1.15.4"
    PREV_MINOR_VERSION = "v1.14.0"  # skipping v1.14.1 as it contains a known data corruption (https://github.com/qdrant/qdrant/pull/6916)
    
    EXPECTED_COLLECTIONS = [
        "test_collection_vector_memory",
        "test_collection_vector_on_disk", 
        "test_collection_vector_on_disk_threshold",
        "test_collection_scalar_int8",
        "test_collection_product_x64",
        "test_collection_product_x32", 
        "test_collection_product_x16",
        "test_collection_product_x8",
        "test_collection_binary",
        "test_collection_mmap_field_index",
        "test_collection_vector_datatype_u8",
        "test_collection_vector_datatype_f16"
    ]
    
    @staticmethod
    def _download_compatibility_data(version: str, storage_test_dir: Path):
        """Download compatibility data for a specific version."""
        # Use test-specific filename to avoid conflicts in parallel execution
        test_id = str(uuid.uuid4())[:8]
        compatibility_file = storage_test_dir / f"compatibility_{version}_{test_id}.tar"
        
        url = f"https://storage.googleapis.com/qdrant-backward-compatibility/compatibility-{version}.tar"
        
        print(f"Downloading compatibility data for {version}...")

        try:
            with requests.get(url, stream=True, timeout=(10, 300)) as response:
                response.raise_for_status()
                with open(compatibility_file, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
        except requests.exceptions.RequestException as e:
            pytest.skip(f"Could not download compatibility data for {version}: {e}")
        
        return compatibility_file

    @staticmethod
    def _extract_storage_data(compatibility_file: Path, storage_test_dir: Path):
        """Extract storage data from compatibility archive."""
        extract_archive(compatibility_file, storage_test_dir, cleanup_archive=True)
        
        # Extract nested storage archive
        storage_archive = storage_test_dir / "storage.tar.bz2"
        if storage_archive.exists():
            extract_archive(storage_archive, storage_test_dir, cleanup_archive=True)
        
        return storage_test_dir / "storage"

    @staticmethod
    def _extract_snapshot_data(storage_test_dir: Path):
        """Extract snapshot data."""
        snapshot_gz = storage_test_dir / "full-snapshot.snapshot.gz"
        
        if snapshot_gz.exists():
            extract_archive(snapshot_gz, storage_test_dir, cleanup_archive=True)
            return storage_test_dir / "full-snapshot.snapshot"
        
        return None

    @staticmethod
    def _check_collections(host: str, port: int) -> bool:
        """Check that all collections are loaded properly."""
        client = ClientUtils(host=host, port=port)

        try:
            collections = client.list_collections_names()
        except Exception as e:
            print(f"Error listing collections: {e}")
            return False

        expected = set(TestStorageCompatibility.EXPECTED_COLLECTIONS)
        found = set(collections)
        missing = expected - found
        if missing:
            print(f"Missing expected collections: {sorted(missing)}")
            return False

        for collection in expected:
            try:
                collection_info = client.get_collection_info_dict(collection)
                if collection_info["status"] != "ok":
                    print(f"Collection {collection} returned status {collection_info['status']}")
                    return False
            except Exception as error:
                print(f"Failed to get collection info for {collection}: {error}")
                return False
        return True


    @pytest.mark.parametrize("version", [PREV_PATCH_VERSION, PREV_MINOR_VERSION])
    def test_storage_compatibility(self, docker_client, qdrant_image, temp_storage_dir, version, qdrant_container_factory):
        """Test storage compatibility with previous versions."""
        compatibility_file = self._download_compatibility_data(version, temp_storage_dir)
        storage_dir = self._extract_storage_data(compatibility_file, temp_storage_dir)

        config = QdrantContainerConfig(
            volumes={str(storage_dir): {"bind": "/qdrant/storage", "mode": "rw"}},
            exit_on_error=False
        )
        container_info = qdrant_container_factory(config)

        client = ClientUtils(host=container_info.host, port=container_info.http_port)
        if not client.wait_for_server():
            pytest.fail(f"Server failed to start for {version}")

        if not self._check_collections(container_info.host, container_info.http_port):
            pytest.fail(f"Storage compatibility failed for {version}")
    
    @pytest.mark.parametrize("version", [PREV_PATCH_VERSION, PREV_MINOR_VERSION])
    def test_snapshot_compatibility(self, docker_client, qdrant_image, temp_storage_dir, version, qdrant_container_factory):
        """Test snapshot recovery compatibility with previous versions."""
        compatibility_file = self._download_compatibility_data(version, temp_storage_dir)
        self._extract_storage_data(compatibility_file, temp_storage_dir)
        snapshot_file = self._extract_snapshot_data(temp_storage_dir)

        if not snapshot_file:
            pytest.fail(f"No snapshot file found for {version}")

        config = QdrantContainerConfig(
            volumes={str(snapshot_file): {"bind": "/qdrant/snapshot.snapshot", "mode": "ro"}},
            command=["./qdrant", "--storage-snapshot", "/qdrant/snapshot.snapshot"],
            exit_on_error=False
        )
        container_info = qdrant_container_factory(config)

        client = ClientUtils(host=container_info.host, port=container_info.http_port)
        if not client.wait_for_server():
            pytest.fail(f"Server failed to start from snapshot for {version}")

        if not self._check_collections(container_info.host, container_info.http_port):
            pytest.fail(f"Snapshot compatibility failed for {version}")
