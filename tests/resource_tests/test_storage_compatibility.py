import pytest
import requests
import tarfile
import gzip
import shutil
import time
import random
from pathlib import Path

from resource_tests.conftest import _create_qdrant_container, QdrantContainerConfig, _cleanup_container
from resource_tests.utils import wait_for_server


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
        test_id = f"{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        compatibility_file = storage_test_dir / f"compatibility_{version}_{test_id}.tar"
        
        # Download compatibility archive
        url = f"https://storage.googleapis.com/qdrant-backward-compatibility/compatibility-{version}.tar"
        
        print(f"Downloading compatibility data for {version}...")
        
        # Use requests instead of wget for better control and error handling
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
        # Extract compatibility archive
        with tarfile.open(compatibility_file, 'r') as tar:
            tar.extractall(storage_test_dir)
        
        # Extract storage archive
        storage_archive = storage_test_dir / "storage.tar.bz2"
        if storage_archive.exists():
            with tarfile.open(storage_archive, 'r:bz2') as tar:
                tar.extractall(storage_test_dir)
        
        # Cleanup archives
        compatibility_file.unlink(missing_ok=True)
        storage_archive.unlink(missing_ok=True)
        
        return storage_test_dir / "storage"

    @staticmethod
    def _extract_snapshot_data(storage_test_dir: Path):
        """Extract snapshot data."""
        snapshot_gz = storage_test_dir / "full-snapshot.snapshot.gz"
        snapshot_file = storage_test_dir / "full-snapshot.snapshot"
        
        if snapshot_gz.exists():
            with gzip.open(snapshot_gz, 'rb') as gz_file:
                with open(snapshot_file, 'wb') as out_file:
                    shutil.copyfileobj(gz_file, out_file)
            
            snapshot_gz.unlink()
            return snapshot_file
        
        return None

    @staticmethod
    def _check_collections(host: str, port: int) -> bool:
        """Check that all collections are loaded properly."""
        base_url = f"http://{host}:{port}"
        
        try:
            # Get list of collections
            response = requests.get(f"{base_url}/collections")
            response.raise_for_status()
            
            collections_data = response.json()
            collections = [col["name"] for col in collections_data["result"]["collections"]]
            
            print(f"Found collections: {collections}")
            
            # Check each collection
            for collection in collections:
                info_response = requests.get(f"{base_url}/collections/{collection}")
                if info_response.status_code != 200:
                    print(f"Storage compatibility failed for collection {collection}")
                    return False
                
                print(f"Collection {collection} is valid")
            
            return True
            
        except Exception as e:
            print(f"Error checking collections: {e}")
            return False

    def _test_storage_compatibility(self, docker_client, qdrant_image, version: str, storage_test_dir: Path):
        """Test storage compatibility for a specific version."""
        print(f"Testing storage compatibility for {version}")
        
        # Download and extract compatibility data
        compatibility_file = self._download_compatibility_data(version, storage_test_dir)
        storage_dir = self._extract_storage_data(compatibility_file, storage_test_dir)
        
        # Create container with mounted storage
        config = QdrantContainerConfig(
            volumes={str(storage_dir): {"bind": "/qdrant/storage", "mode": "rw"}},
            exit_on_error=False
        )
        container_info = _create_qdrant_container(docker_client, qdrant_image, config)
        
        try:
            if not wait_for_server(container_info.host, container_info.http_port):
                pytest.fail(f"Server failed to start for {version}")
            
            if not self._check_collections(container_info.host, container_info.http_port):
                pytest.fail(f"Storage compatibility failed for {version}")
            
            print(f"Storage compatibility test passed for {version}")
            
        finally:
            _cleanup_container(container_info.container)

    def _test_snapshot_compatibility(self, docker_client, qdrant_image, version: str, storage_test_dir: Path):
        """Test snapshot recovery compatibility for a specific version."""
        print(f"Testing snapshot compatibility for {version}")
        
        # Extract snapshot data
        snapshot_file = self._extract_snapshot_data(storage_test_dir)
        if not snapshot_file:
            print(f"No snapshot file found for {version}, skipping snapshot test")
            return
        
        # Create container with snapshot recovery
        config = QdrantContainerConfig(
            volumes={str(snapshot_file): {"bind": "/qdrant/snapshot.snapshot", "mode": "ro"}},
            command=["./qdrant", "--storage-snapshot", "/qdrant/snapshot.snapshot"],
            exit_on_error=False
        )
        container_info = _create_qdrant_container(docker_client, qdrant_image, config)
        
        try:
            if not wait_for_server(container_info.host, container_info.http_port):
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
        # First run storage compatibility to extract the data
        try:
            compatibility_file = self._download_compatibility_data(version, temp_storage_dir)
            self._extract_storage_data(compatibility_file, temp_storage_dir)
        except Exception as e:
            pytest.skip(f"Failed to prepare data for snapshot test: {e}")
        
        self._test_snapshot_compatibility(docker_client, qdrant_image, version, temp_storage_dir)
    
    @pytest.mark.parametrize("storage_from_archive", ["storage.tar.xz"], indirect=True)
    def test_local_storage_compatibility(self, docker_client, qdrant_image, qdrant_container, storage_from_archive):
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
            if not wait_for_server(container_info.host, container_info.http_port):
                pytest.fail("Server failed to start with local storage")
            
            if not self._check_collections(container_info.host, container_info.http_port):
                pytest.fail("Local storage compatibility failed")
            
            print("Local storage compatibility test passed")
            
        finally:
            _cleanup_container(container_info.container)
