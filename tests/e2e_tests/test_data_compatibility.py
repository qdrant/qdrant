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
        "v1.17.0",
        "v1.16.3",
        "v1.16.2",
        "v1.16.1",
        "v1.16.0",
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

    DENSE_DIM = 256
    MULTI_DENSE_DIM = 128

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

    def _query_collections(self, host: str, port: int) -> tuple[bool, str]:
        """Run queries against all collections to verify data is actually accessible.

        Sends one query of each kind per collection: dense, sparse, and multivector
        search, plus scroll with filters covering every payload index type.
        """
        base_url = f"http://{host}:{port}"

        for collection in self.EXPECTED_COLLECTIONS:
            try:
                # Dense vector search
                resp = requests.post(
                    f"{base_url}/collections/{collection}/points/query",
                    json={"query": [0.1] * self.DENSE_DIM, "using": "image", "limit": 3},
                )
                if not resp.ok:
                    return False, f"Dense search failed on {collection}: {resp.status_code} {resp.text}"

                # Sparse vector search
                resp = requests.post(
                    f"{base_url}/collections/{collection}/points/query",
                    json={
                        "query": {"indices": [0, 10, 50], "values": [0.5, 0.3, 0.1]},
                        "using": "text",
                        "limit": 3,
                    },
                )
                if not resp.ok:
                    return False, f"Sparse search failed on {collection}: {resp.status_code} {resp.text}"

                # Multivector search
                resp = requests.post(
                    f"{base_url}/collections/{collection}/points/query",
                    json={
                        "query": [[0.1] * self.MULTI_DENSE_DIM, [0.2] * self.MULTI_DENSE_DIM],
                        "using": "multi-image",
                        "limit": 3,
                    },
                )
                if not resp.ok:
                    return False, f"Multivector search failed on {collection}: {resp.status_code} {resp.text}"

                # Scroll with keyword filter
                resp = requests.post(
                    f"{base_url}/collections/{collection}/points/scroll",
                    json={
                        "filter": {"must": [{"key": "keyword_field", "match": {"value": "hello"}}]},
                        "limit": 3,
                    },
                )
                if not resp.ok:
                    return False, f"Keyword filter scroll failed on {collection}: {resp.status_code} {resp.text}"

                # Scroll with float range filter
                resp = requests.post(
                    f"{base_url}/collections/{collection}/points/scroll",
                    json={
                        "filter": {"must": [{"key": "float_field", "range": {"gte": 0.0, "lte": 1.0}}]},
                        "limit": 3,
                    },
                )
                if not resp.ok:
                    return False, f"Float filter scroll failed on {collection}: {resp.status_code} {resp.text}"

                # Scroll with integer range filter
                resp = requests.post(
                    f"{base_url}/collections/{collection}/points/scroll",
                    json={
                        "filter": {"must": [{"key": "integer_field", "range": {"gte": 0, "lte": 50}}]},
                        "limit": 3,
                    },
                )
                if not resp.ok:
                    return False, f"Integer filter scroll failed on {collection}: {resp.status_code} {resp.text}"

                # Scroll with boolean filter
                resp = requests.post(
                    f"{base_url}/collections/{collection}/points/scroll",
                    json={
                        "filter": {"must": [{"key": "boolean_field", "match": {"value": True}}]},
                        "limit": 3,
                    },
                )
                if not resp.ok:
                    return False, f"Boolean filter scroll failed on {collection}: {resp.status_code} {resp.text}"

                # Scroll with geo bounding box filter
                resp = requests.post(
                    f"{base_url}/collections/{collection}/points/scroll",
                    json={
                        "filter": {"must": [{
                            "key": "geo_field",
                            "geo_bounding_box": {
                                "top_left": {"lat": 1.0, "lon": 0.0},
                                "bottom_right": {"lat": 0.0, "lon": 1.0},
                            },
                        }]},
                        "limit": 3,
                    },
                )
                if not resp.ok:
                    return False, f"Geo filter scroll failed on {collection}: {resp.status_code} {resp.text}"

                # Scroll with full-text match filter
                resp = requests.post(
                    f"{base_url}/collections/{collection}/points/scroll",
                    json={
                        "filter": {"must": [{"key": "text_field", "match": {"text": "hello"}}]},
                        "limit": 3,
                    },
                )
                if not resp.ok:
                    return False, f"Text filter scroll failed on {collection}: {resp.status_code} {resp.text}"

                # Scroll with uuid filter
                resp = requests.post(
                    f"{base_url}/collections/{collection}/points/scroll",
                    json={
                        "filter": {"must": [{"key": "uuid_field", "match": {"value": "00000000-0000-0000-0000-000000000000"}}]},
                        "limit": 3,
                    },
                )
                if not resp.ok:
                    return False, f"UUID filter scroll failed on {collection}: {resp.status_code} {resp.text}"

                # Scroll with datetime range filter
                resp = requests.post(
                    f"{base_url}/collections/{collection}/points/scroll",
                    json={
                        "filter": {"must": [{"key": "datetime_field", "range": {"gte": "2020-01-01T00:00:00Z", "lte": "2030-01-01T00:00:00Z"}}]},
                        "limit": 3,
                    },
                )
                if not resp.ok:
                    return False, f"Datetime filter scroll failed on {collection}: {resp.status_code} {resp.text}"

            except Exception as e:
                return False, f"Query failed on {collection}: {e}"

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

            success, error_msg = self._query_collections(container_info.host, container_info.http_port)
            if not success:
                return False, f"{test_name.capitalize()} query verification failed for {version}: {error_msg}"

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