import pytest

from e2e_tests.client_utils import ClientUtils


class TestSnapshotsInterferenceWithConsensus:
    @pytest.mark.parametrize("qdrant_compose", [
        {"compose_file": "3-node-cluster.yaml"}
    ], indirect=True)
    def test_snapshot_does_not_block_other_operations(self, qdrant_compose):
        """Test that creating snapshots does not block other operations - https://github.com/qdrant/qdrant/issues/7489."""
        client = ClientUtils(host=qdrant_compose[0].host, port=qdrant_compose[0].http_port, timeout=10)
        client.wait_for_server()

        # Create collection and insert points
        collection_config = {"vectors": {"size": 512, "distance": "Dot"}, "shard_number": 3, "replication_factor": 1, "optimization_config": {"indexing_threshold": 0}}
        client.create_collection("test_collection", collection_config)
        for points_batch in client.generate_points(1000, vector_size=512):
            client.insert_points("test_collection", points_batch, wait=True)

        client.create_collection("small", collection_config)

        # Create snapshots very fast
        for _ in range(50):
            client.create_snapshot("test_collection", do_wait=False)

        client.delete_collection("small", timeout=2)
