import pytest
import time
import threading

from e2e_tests.client_utils import ClientUtils, VECTOR_SIZE
from qdrant_client import models


class TestContinuousSnapshots:
    """Test continuous snapshots creation while modifying collection data."""

    @staticmethod
    def _snapshot_creation_loop(client: ClientUtils, collection_name: str, iterations: int, results: dict, stop_event: threading.Event):
        """Continuously create snapshots until stop event is set or iterations complete."""
        snapshots_created = []
        errors = []

        for i in range(iterations):
            # Check if we should stop
            if stop_event.is_set():
                print(f"Snapshot creation stopping early at iteration {i+1} (data modification completed)")
                break

            try:
                snapshot_name = client.create_snapshot(collection_name)
                snapshots_created.append(snapshot_name)
                # print(f"Snapshot {i+1}/{iterations} created: {snapshot_name}")
                # time.sleep(0.1)  # Small delay between snapshots
            except Exception as e:
                error_msg = f"Snapshot creation {i+1} failed: {str(e)}"
                print(error_msg)
                errors.append(error_msg)
                break

        results["snapshots"] = snapshots_created
        results["errors"] = errors

    @staticmethod
    def _data_modification_loop(client: ClientUtils, collection_name: str, iterations: int, results: dict, stop_event: threading.Event):
        """Continuously delete all points and insert new points for the specified number of iterations."""
        modification_count = 0
        errors = []

        for i in range(iterations):
            try:
                # Delete all existing points by their IDs
                client.client.delete(
                    collection_name=collection_name,
                    points_selector=models.PointIdsList(points=list(range(150))),
                    wait=True
                )

                # Insert 150 points in batches of 10 with integer IDs
                total_inserted = 0
                base_id = 0
                for batch_idx in range(15):  # 15 batches of 10 points = 150 points
                    points = []
                    for point_idx in range(10):
                        points.append(
                            models.PointStruct(
                                id=base_id,
                                vector=[round(float(j), 2) for j in range(VECTOR_SIZE)],
                                payload={"city": ["Berlin", "London"], "iteration": modification_count, "batch": batch_idx}
                            )
                        )
                        base_id += 1

                    client.client.upsert(
                        collection_name=collection_name,
                        points=points,
                        wait=True
                    )
                    total_inserted += len(points)

                # print(f"Iteration {i+1}/{iterations}: Inserted {total_inserted} points")
                modification_count += 1
                # time.sleep(0.05)  # Small delay between iterations
            except Exception as e:
                error_msg = f"Data modification {i+1} failed: {str(e)}"
                print(error_msg)
                errors.append(error_msg)
                break

        print(f"Done {modification_count}/{iterations} iterations.")
        # Signal snapshot thread to stop
        stop_event.set()
        results["errors"] = errors
        results["modifications_completed"] = modification_count

    def test_continuous_snapshots(self, qdrant_container_factory):
        """
        Test continuous snapshot creation while modifying collection data.
        Creates a collection and runs two concurrent loops:
        1. Creating snapshots
        2. Deleting all points and inserting 150 new points
        Both loops run for at least 100 iterations.
        """
        container_info = qdrant_container_factory()
        client = ClientUtils(host=container_info.host, port=container_info.http_port)
        assert client.wait_for_server(), "Server failed to start"

        collection_name = "continuous_snapshots_test"
        collection_config = {
            "vectors": {
                "size": VECTOR_SIZE,
                "distance": "Cosine",
            },
            "optimizers_config": {
                "default_segment_number": 2,
            },
            "write_consistency_factor": 1,
            "on_disk_payload": True
        }
        client.create_collection(collection_name, collection_config)
        print(f"Created collection: {collection_name}")

        iterations = 100

        snapshot_results = {}
        modification_results = {}

        # Create event to signal when to stop
        stop_event = threading.Event()

        snapshot_thread = threading.Thread(
            target=self._snapshot_creation_loop,
            args=(client, collection_name, iterations, snapshot_results, stop_event)
        )

        modification_thread = threading.Thread(
            target=self._data_modification_loop,
            args=(client, collection_name, iterations, modification_results, stop_event)
        )

        print(f"Starting concurrent operations with {iterations} iterations each...")
        start_time = time.time()

        snapshot_thread.start()
        modification_thread.start()
        # Wait for both threads to complete
        snapshot_thread.join()
        modification_thread.join()

        duration = time.time() - start_time
        print(f"Concurrent operations completed in {duration:.2f} seconds")

        if modification_results and modification_results.get("errors"):
            pytest.fail(f"Upsert errors occurred: {modification_results['errors']}")

        print("\nTest completed successfully - No upsert errors!")
