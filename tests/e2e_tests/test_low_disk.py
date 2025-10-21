import pytest
import time
import uuid
from docker.types import Mount
from typing import Literal

from e2e_tests.client_utils import ClientUtils, VECTOR_SIZE
from e2e_tests.models import QdrantContainerConfig


class TestLowDisk:
    """Test Qdrant behavior under low disk conditions."""

    @staticmethod
    def insert_points_and_search(client: ClientUtils, collection_name, points_amount):
        collection_config = {
            "vectors": {
                "size": VECTOR_SIZE,
                "distance": "Cosine"
            }
        }
        client.create_collection(collection_name, collection_config)
        for points_batch in client.generate_points(points_amount):
            client.insert_points(collection_name, points_batch)

        client.search_points(collection_name)

    @staticmethod
    def insert_points_then_index(client: ClientUtils, collection_name, points_amount):
        """
        Disable indexing, create collection, insert points up to
        a point when there is no space left on disk, then start indexing.
        Wait for indexing then send a search request.
        @param client:
        @param collection_name:
        @param points_amount:
        @return:
        """
        collection_config = {
            "vectors": {
                "size": VECTOR_SIZE,
                "distance": "Cosine"
            },
            "optimizers_config": {
                "indexing_threshold": 0,
                "default_segment_number": 2
            },
            "wal_config": {
                "wal_capacity_mb": 1
            }
        }
        client.create_collection(collection_name, collection_config)
        for points_batch in client.generate_points(points_amount):
            res = client.insert_points(collection_name, points_batch, quit_on_ood=True)
            if res == "ood":
                break

        # start indexing
        collection_params = {
            "optimizers_config": {
                "indexing_threshold": 10
            }
        }
        client.update_collection(collection_name, collection_params)
        client.wait_for_status(collection_name, "yellow")
        client.wait_for_status(collection_name, "green")
        client.search_points(collection_name)

    @pytest.mark.parametrize("test_mode", ["search", "indexing"])
    def test_low_disk_handling(self, qdrant_container_factory, test_mode: Literal["search", "indexing"]):
        """
        Test that Qdrant handles low disk conditions gracefully. Start a container with limited tmpfs mount (10MB),
        perform operations, ensure container doesn't crash.

        Args:
            test_mode: Either "search" (test during points insertion) or "indexing" (test during index building)
        """
        unique_suffix = str(uuid.uuid4())[:8]
        config = QdrantContainerConfig(
            name=f"qdrant-ood-{test_mode}-{unique_suffix}",
            mounts=[
                Mount(
                    target="/qdrant/storage",
                    source=None,
                    type="tmpfs",
                    tmpfs_size=10240000  # 10MB
                )
            ],
            remove=False  # Keep for logs
        )
        container_info = qdrant_container_factory(config)

        # Create ClientUtils instance for this container
        client = ClientUtils(host=container_info.host, port=container_info.http_port)
        collection_name = "low-disk"
        points_amount = 2000

        if test_mode == "search":
            self.insert_points_and_search(client, collection_name, points_amount)
        elif test_mode == "indexing":
            self.insert_points_then_index(client, collection_name, points_amount)

        # Give some time for logs to be written
        time.sleep(5)

        logs = container_info.container.logs().decode('utf-8')
        expected_msg = "No space left on device:"
        assert expected_msg in logs, f"'{expected_msg}' log message not found in container logs"

        print(f"{test_mode}: OK")
