import pytest
import uuid

from e2e_tests.client_utils import ClientUtils
from e2e_tests.models import QdrantContainerConfig


class TestLowRam:
    """Test Qdrant behavior under low RAM conditions and recovery mode."""

    @pytest.mark.parametrize("storage_from_archive", ["storage.tar.xz"], indirect=True)
    def test_oom_with_insufficient_memory(self, qdrant_container_factory, storage_from_archive):
        """
        Test that Qdrant OOMs when started with insufficient memory.
        """
        print("Starting container with 128MB memory limit - expecting OOM...")
        unique_suffix = str(uuid.uuid4())[:8]
        config = QdrantContainerConfig(
            name=f"qdrant-oom-{unique_suffix}",
            mem_limit="128m",
            volumes={str(storage_from_archive): {'bind': '/qdrant/storage', 'mode': 'rw'}},
            environment={"QDRANT__STORAGE__HANDLE_COLLECTION_LOAD_ERRORS": "true"},
            remove=False,
            exit_on_error=False  # Don't raise error when Qdrant fails to start
        )
        oom_container_info = qdrant_container_factory(config)

        oom_container = oom_container_info.container

        exit_code = oom_container.wait()['StatusCode']

        assert exit_code == 137, f"Expected exit code 137 (OOM), but got {exit_code}"

    @pytest.mark.parametrize("storage_from_archive", ["storage.tar.xz"], indirect=True)
    def test_recovery_mode_with_low_memory(self, qdrant_container_factory, storage_from_archive):
        """
        Test that:
        1. Qdrant can start in recovery mode with low memory limit
        2. Collection loads as a dummy shard in recovery mode
        """
        print("Starting container in recovery mode...")
        config = QdrantContainerConfig(
            mem_limit="128m",
            volumes={str(storage_from_archive): {'bind': '/qdrant/storage', 'mode': 'rw'}},
            environment={
                "QDRANT__STORAGE__HANDLE_COLLECTION_LOAD_ERRORS": "true",
                "QDRANT_ALLOW_RECOVERY_MODE": "true"
            }
        )
        recovery_container_info = qdrant_container_factory(config)

        recovery_container = recovery_container_info.container
        api_port = recovery_container_info.http_port

        client = ClientUtils(host=recovery_container_info.host, port=api_port)

        assert client.wait_for_collection_loaded("low-ram"), "Collection failed to load in ~10 seconds"

        # Check for recovery mode message in logs
        logs = recovery_container.logs().decode('utf-8')
        recovery_msg = "Qdrant is loaded in recovery mode"
        assert recovery_msg in logs, f"'{recovery_msg}' log message not found in container logs"

        # Check that collection info returns 500 (dummy shard)
        try:
            client.get_collection_info_dict("low-ram")
            assert False, "Expected collection info to fail with Out-of-Memory error"
        except Exception as e:
            error_text = str(e)
            expected_error = "Out-of-Memory"
            assert expected_error in error_text, f"'{expected_error}' not found in error: {error_text}"
