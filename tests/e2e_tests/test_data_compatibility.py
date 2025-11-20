import shutil

import pytest
from pathlib import Path

from e2e_tests.conftest import QdrantContainerConfig
from e2e_tests.client_utils import ClientUtils
from e2e_tests.utils import extract_archive


@pytest.mark.xdist_group("compatibility")
class TestStorageCompatibility:
    """Test storage and snapshot compatibility with defined previous Qdrant versions.

    These tests are grouped to run sequentially on the same worker using xdist_group marker.
    This prevents parallel downloads of large archives and disk saturation.
    """

    VERSIONS = [
        "v1.16.0",
        "v1.15.5",
        "v1.15.4",
        "v1.15.3",
        "v1.15.2",
        "v1.15.1",
        "v1.15.0"
    ]

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
    def _get_compatibility_file(version: str, compatibility_data_cache, temp_storage_dir: Path) -> Path:
        """Get compatibility archive from cache and copy to temp storage directory.

        Args:
            version: Version string (e.g., "v1.16.0")
            compatibility_data_cache: Cache fixture that provides downloaded archives
            temp_storage_dir: Temporary directory for this test

        Returns:
            Path to the copied compatibility archive in temp_storage_dir
        """
        cached_archive = compatibility_data_cache.get(version)

        # Copy to temp_storage_dir for extraction
        compatibility_file = temp_storage_dir / f"compatibility-{version}.tar"
        shutil.copy2(cached_archive, compatibility_file)

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


    @pytest.mark.parametrize("version", VERSIONS)
    def test_storage_compatibility(self, docker_client, qdrant_image, temp_storage_dir, version,
                                   qdrant_container_factory, compatibility_data_cache):
        """Test storage compatibility with previous versions."""
        compatibility_file = self._get_compatibility_file(version, compatibility_data_cache, temp_storage_dir)
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

    @pytest.mark.parametrize("version", VERSIONS)
    def test_snapshot_compatibility(self, docker_client, qdrant_image, temp_storage_dir, version,
                                    qdrant_container_factory, compatibility_data_cache):
        """Test snapshot recovery compatibility with previous versions."""
        compatibility_file = self._get_compatibility_file(version, compatibility_data_cache, temp_storage_dir)
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
