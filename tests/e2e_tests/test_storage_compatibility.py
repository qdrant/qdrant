import uuid

import pytest
import requests
import time
import random
from pathlib import Path

from e2e_tests.conftest import _create_qdrant_container, QdrantContainerConfig, _cleanup_container
from e2e_tests.client_utils import ClientUtils
from e2e_tests.utils import extract_archive


class TestStorageCompatibility:
    """Test storage compatibility with previous Qdrant versions."""
    
    # Versions under test
    PREV_PATCH_VERSION = "v1.14.0"
    PREV_MINOR_VERSION = "v1.13.5"
    
    # Expected collections from the populate script
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
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            
            with open(compatibility_file, 'wb') as f:
                f.write(response.content)
                
        except Exception as e:
            pytest.skip(f"Could not download compatibility data for {version}: {e}")
        
        return compatibility_file

    @staticmethod
    def _extract_storage_data(compatibility_file: Path, storage_test_dir: Path):
        """Extract storage data from compatibility archive."""
        # Extract the main compatibility archive
        extract_archive(compatibility_file, storage_test_dir, cleanup_archive=True)
        
        # Extract nested storage archive if it exists
        storage_archive = storage_test_dir / "storage.tar.bz2"
        if storage_archive.exists():
            extract_archive(storage_archive, storage_test_dir, cleanup_archive=True)
        
        return storage_test_dir / "storage"

    @staticmethod
    def _extract_snapshot_data(storage_test_dir: Path):
        """Extract snapshot data."""
        snapshot_gz = storage_test_dir / "full-snapshot.snapshot.gz"
        
        if snapshot_gz.exists():
            # Use the general extract_archive utility for gzip files
            extract_archive(snapshot_gz, storage_test_dir, cleanup_archive=True)
            return storage_test_dir / "full-snapshot.snapshot"
        
        return None

    @staticmethod
    def _check_collections(host: str, port: int) -> bool:
        """Check that all collections are loaded properly."""
        try:
            client = ClientUtils(host=host, port=port)
            
            collections = client.list_collections_names()
            
            for collection in collections:
                try:
                    collection_info = client.get_collection_info_dict(collection)
                    if collection_info["status"] != "ok":
                        print(f"Storage compatibility failed for collection {collection}")
                        return False
                except Exception as e:
                    print(f"Storage compatibility failed for collection {collection}: {e}")
                    return False
            
            return True
            
        except Exception as e:
            print(f"Error checking collections: {e}")
            return False

    def _test_storage_compatibility(self, docker_client, qdrant_image, version: str, storage_test_dir: Path):
        """Test storage compatibility for a specific version."""
        print(f"Testing storage compatibility for {version}")
        
        compatibility_file = self._download_compatibility_data(version, storage_test_dir)
        storage_dir = self._extract_storage_data(compatibility_file, storage_test_dir)
        
        config = QdrantContainerConfig(
            volumes={str(storage_dir): {"bind": "/qdrant/storage", "mode": "rw"}},
            exit_on_error=False
        )
        container_info = _create_qdrant_container(docker_client, qdrant_image, config)
        
        try:
            client = ClientUtils(host=container_info.host, port=container_info.http_port)
            if not client.wait_for_server():
                pytest.fail(f"Server failed to start for {version}")
            
            if not self._check_collections(container_info.host, container_info.http_port):
                pytest.fail(f"Storage compatibility failed for {version}")
            
            print(f"Storage compatibility test passed for {version}")
            
        finally:
            _cleanup_container(container_info.container)

    def _test_snapshot_compatibility(self, docker_client, qdrant_image, version: str, storage_test_dir: Path):
        """Test snapshot recovery compatibility for a specific version."""
        print(f"Testing snapshot compatibility for {version}")
        
        snapshot_file = self._extract_snapshot_data(storage_test_dir)
        if not snapshot_file:
            print(f"No snapshot file found for {version}, skipping snapshot test")
            return
        
        config = QdrantContainerConfig(
            volumes={str(snapshot_file): {"bind": "/qdrant/snapshot.snapshot", "mode": "ro"}},
            command=["./qdrant", "--storage-snapshot", "/qdrant/snapshot.snapshot"],
            exit_on_error=False
        )
        container_info = _create_qdrant_container(docker_client, qdrant_image, config)
        
        try:
            client = ClientUtils(host=container_info.host, port=container_info.http_port)
            if not client.wait_for_server():
                pytest.fail(f"Server failed to start from snapshot for {version}")
            
            if not self._check_collections(container_info.host, container_info.http_port):
                pytest.fail(f"Snapshot compatibility failed for {version}")
            
            print(f"Snapshot compatibility test passed for {version}")
            
        finally:
            _cleanup_container(container_info.container)
            snapshot_file.unlink(missing_ok=True)
    
    @pytest.mark.parametrize("version", [PREV_PATCH_VERSION, PREV_MINOR_VERSION])
    def test_storage_compatibility(self, docker_client, qdrant_image, temp_storage_dir, version):
        """Test storage compatibility with previous versions."""
        self._test_storage_compatibility(docker_client, qdrant_image, version, temp_storage_dir)
    
    @pytest.mark.parametrize("version", [PREV_PATCH_VERSION, PREV_MINOR_VERSION])
    def test_snapshot_compatibility(self, docker_client, qdrant_image, temp_storage_dir, version):
        """Test snapshot recovery compatibility with previous versions."""
        try:
            compatibility_file = self._download_compatibility_data(version, temp_storage_dir)
            self._extract_storage_data(compatibility_file, temp_storage_dir)
        except Exception as e:
            pytest.skip(f"Failed to prepare data for snapshot test: {e}")
        
        self._test_snapshot_compatibility(docker_client, qdrant_image, version, temp_storage_dir)
    
    @pytest.mark.parametrize("storage_from_archive", ["storage.tar.xz"], indirect=True)
    def test_local_storage_compatibility(self, docker_client, qdrant_image, qdrant_container_factory, storage_from_archive):
        """Test storage compatibility using local test data - parallel safe."""
        print("Testing local storage compatibility")
        
        if not storage_from_archive.exists():
            pytest.skip("Extracted storage directory not found")
        
        # Create container with mounted storage
        config = QdrantContainerConfig(
            volumes={str(storage_from_archive): {"bind": "/qdrant/storage", "mode": "rw"}},
            exit_on_error=False
        )
        container_info = _create_qdrant_container(docker_client, qdrant_image, config)
        
        try:
            client = ClientUtils(host=container_info.host, port=container_info.http_port)
            if not client.wait_for_server():
                pytest.fail("Server failed to start with local storage")
            
            if not self._check_collections(container_info.host, container_info.http_port):
                pytest.fail("Local storage compatibility failed")
            
            print("Local storage compatibility test passed")
            
        finally:
            _cleanup_container(container_info.container)
