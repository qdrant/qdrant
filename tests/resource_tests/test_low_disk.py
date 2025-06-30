import pytest
import time
import uuid
from docker.types import Mount
from typing import Literal

from resource_tests.utils import create_collection, generate_points, insert_points, search_point, VECTOR_SIZE, \
    update_collection, wait_for_status


class TestLowDisk:
    """Test Qdrant behavior under low disk conditions."""

    @staticmethod
    def insert_points_and_search(qdrant_host, collection_name, points_amount):
        collection_json = {
            "vectors": {
                "size": VECTOR_SIZE,
                "distance": "Cosine"
            }
        }
        create_collection(qdrant_host, collection_name, collection_json)
        for points_batch in generate_points(points_amount):
            insert_points(qdrant_host, collection_name, points_batch)

        search_point(qdrant_host, collection_name)

    @staticmethod
    def insert_points_then_index(qdrant_host, collection_name, points_amount):
        """
        Disable indexing, create collection, insert points up to
        a point when there is no space left on disk, then start indexing.
        Wait for indexing then send a search request.
        @param qdrant_host:
        @param collection_name:
        @param points_amount:
        @return:
        """
        collection_json = {
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
        create_collection(qdrant_host, collection_name, collection_json)
        for points_batch in generate_points(points_amount):
            res = insert_points(qdrant_host, collection_name, points_batch, quit_on_ood=True)
            if res == "ood":
                break

        # start indexing
        collection_json = {
            "optimizers_config": {
                "indexing_threshold": 10
            }
        }
        update_collection(qdrant_host, collection_name, collection_json)
        wait_for_status(qdrant_host, collection_name, "yellow")
        wait_for_status(qdrant_host, collection_name, "green")
        search_point(qdrant_host, collection_name)
    
    @pytest.mark.parametrize("test_mode", ["search", "indexing"])
    def test_low_disk_handling(self, qdrant_container, test_mode: Literal["search", "indexing"]):
        """
        Test that Qdrant handles low disk conditions gracefully. Start a container with limited tmpfs mount (10MB),
        perform operations, ensure container doesn't crash.
        
        Args:
            test_mode: Either "search" (test during points insertion) or "indexing" (test during index building)
        """
        unique_suffix = str(uuid.uuid4())[:8]
        container_info = qdrant_container(
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
        
        container = container_info["container"]
        qdrant_host = f"http://{container_info['host']}:{container_info['http_port']}"
        collection_name = "low-disk"
        points_amount = 2000
        
        if test_mode == "search":
            self.insert_points_and_search(qdrant_host, collection_name, points_amount)
        elif test_mode == "indexing":
            self.insert_points_then_index(qdrant_host, collection_name, points_amount)

        # Give some time for logs to be written
        time.sleep(5)

        logs = container.logs().decode('utf-8')
        expected_msg = "No space left on device:"
        assert expected_msg in logs, f"'{expected_msg}' log message not found in container logs"

        print(f"{test_mode}: OK")
