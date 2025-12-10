import uuid
from pathlib import Path
from typing import Optional

import pytest
import requests
from docker.errors import NotFound

from e2e_tests.conftest import QdrantContainerConfig
from e2e_tests.client_utils import ClientUtils
from e2e_tests.utils import extract_archive, remove_dir


class TestStorageCompatibility:
    """Test storage and snapshot compatibility with defined previous Qdrant versions.

    These tests use pytest-subtests to run both storage and snapshot compatibility
    tests for each version while downloading the compatibility data only once.
    This minimizes disk usage (critical for GitHub runners with limited space)
    while maintaining granular test reporting.
    """

    VERSIONS = [
        "v1.16.1",
        "v1.16.0",
        "v1.15.5",
        "v1.15.4",
        "v1.15.3",
        "v1.15.2",
        "v1.15.1",
        # "v1.15.0", the archive triggers debug_assertion for the UUID payload index https://github.com/qdrant/qdrant/pull/6916
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
    def _download_compatibility_data(version: str, storage_test_dir: Path) -> Path:
        """Download compatibility data for a specific version.

        Args:
            version: Version string (e.g., "v1.16.0")
            storage_test_dir: Directory to download the archive to

        Returns:
            Path to the downloaded compatibility archive
        """
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
    def _extract_storage_data(compatibility_file: Path, storage_test_dir: Path) -> Path:
        """Extract storage data from compatibility archive."""
        extract_archive(compatibility_file, storage_test_dir, cleanup_archive=True)

        # Extract nested storage archive
        storage_archive = storage_test_dir / "storage.tar.bz2"
        if storage_archive.exists():
            extract_archive(storage_archive, storage_test_dir, cleanup_archive=True)

        return storage_test_dir / "storage"

    @staticmethod
    def _extract_snapshot_data(storage_test_dir: Path) -> Optional[Path]:
        """Extract snapshot data."""
        snapshot_gz = storage_test_dir / "full-snapshot.snapshot.gz"

        if snapshot_gz.exists():
            extract_archive(snapshot_gz, storage_test_dir, cleanup_archive=True)
            return storage_test_dir / "full-snapshot.snapshot"

        return None

    def _check_collections(self, host: str, port: int) -> tuple[bool, str]:
        """Check that all collections are loaded properly.

        Returns:
            Tuple of (success: bool, error_message: str)
        """
        client = ClientUtils(host=host, port=port)

        try:
            collections = client.list_collections_names()
        except Exception as e:
            return False, f"Error listing collections: {e}"

        expected = set(self.EXPECTED_COLLECTIONS)
        found = set(collections)
        missing = expected - found
        if missing:
            return False, f"Missing expected collections: {sorted(missing)}"

        for collection in expected:
            try:
                collection_info = client.get_collection_info_dict(collection)
                if collection_info["status"] != "ok":
                    return False, f"Collection {collection} returned status {collection_info['status']}"
            except Exception as error:
                return False, f"Failed to get collection info for {collection}: {error}"

        return True, ""

    def _run_test(
        self,
        config: QdrantContainerConfig,
        test_name: str,
        version: str,
        qdrant_container_factory,
        cleanup_dir: Optional[Path] = None
    ) -> tuple[bool, str]:
        """Run a Qdrant container test with the given configuration.

        Args:
            config: Container configuration
            test_name: Name of the test for error messages (e.g., "storage", "snapshot")
            version: Version string for error messages
            qdrant_container_factory: Factory fixture to create containers
            cleanup_dir: Optional directory to remove after test completion

        Returns:
            Tuple of (success: bool, error_message: str)
        """
        try:
            container_info = qdrant_container_factory(config)
        except (NotFound, RuntimeError) as e:
            return False, f"Container failed to start for {version}: {e}"

        try:
            client = ClientUtils(host=container_info.host, port=container_info.http_port)
            if not client.wait_for_server():
                return False, f"Server failed to start for {test_name} test ({version})"

            success, error_msg = self._check_collections(container_info.host, container_info.http_port)
            if not success:
                return False, f"{test_name.capitalize()} compatibility failed for {version}: {error_msg}"

            return True, ""
        finally:
            try:
                container_info.container.stop()
                container_info.container.remove(force=True)
            except NotFound:
                pass
            if cleanup_dir:
                remove_dir(cleanup_dir)

    def _run_storage_test(self, storage_dir: Path, version: str, qdrant_container_factory) -> tuple[bool, str]:
        """Run storage compatibility test."""
        config = QdrantContainerConfig(
            volumes={str(storage_dir): {"bind": "/qdrant/storage", "mode": "rw"}},
            exit_on_error=False,
            remove=False,
        )
        return self._run_test(
            config, "storage", version, qdrant_container_factory, cleanup_dir=storage_dir
        )

    def _run_snapshot_test(self, snapshot_file: Optional[Path], version: str, qdrant_container_factory) -> tuple[bool, str]:
        """Run snapshot compatibility test."""
        if not snapshot_file:
            return False, f"No snapshot file found for {version}"

        config = QdrantContainerConfig(
            volumes={str(snapshot_file): {"bind": "/qdrant/snapshot.snapshot", "mode": "ro"}},
            command=["./qdrant", "--storage-snapshot", "/qdrant/snapshot.snapshot"],
            exit_on_error=False,
            remove=False,
        )
        return self._run_test(config, "snapshot", version, qdrant_container_factory)

    @pytest.mark.xdist_group("compatibility")
    @pytest.mark.parametrize("version", VERSIONS)
    def test_compatibility(self, temp_storage_dir, version, qdrant_container_factory, subtests):
        """Test both storage and snapshot compatibility for a specific version.

        This test downloads compatibility data once and runs both storage and snapshot
        subtests, minimizing disk usage while maintaining granular test reporting.

        Subtests:
            - storage: Tests that Qdrant can load storage data from the previous version
            - snapshot: Tests that Qdrant can recover from a snapshot created by the previous version
        """
        # Download and extract compatibility data once for both subtests
        compatibility_file = self._download_compatibility_data(version, temp_storage_dir)
        storage_dir = self._extract_storage_data(compatibility_file, temp_storage_dir)
        snapshot_file = self._extract_snapshot_data(temp_storage_dir)

        # Subtest 1: Storage compatibility
        with subtests.test(msg="storage", version=version):
            success, error_msg = self._run_storage_test(storage_dir, version, qdrant_container_factory)
            assert success, error_msg

        # Subtest 2: Snapshot compatibility
        with subtests.test(msg="snapshot", version=version):
            success, error_msg = self._run_snapshot_test(snapshot_file, version, qdrant_container_factory)
            assert success, error_msg