import uuid
import random

from qdrant_client import models
from e2e_tests.client_utils import ClientUtils
from e2e_tests.models import QdrantContainerConfig


class TestImmutableIndex:
    """Test Qdrant immutable index functionality."""

    def test_immutable_full_text_index(self, qdrant_container_factory):
        """
        Test scenario:
        1. Create a collection with specific configuration
        2. Add multiple payload indexes
        3. Insert 100 points
        4. Verify points count matches in collection and all indexes
        """
        config = QdrantContainerConfig()
        container_info = qdrant_container_factory(config)

        client = ClientUtils(host=container_info.host, port=container_info.http_port)
        assert client.wait_for_server(), "Server failed to start"

        collection_name = "test_immutable_index"

        # 1. Create collection with specific configuration
        vector_dim = 256
        collection_config = {
            "vectors": {
                "size": vector_dim,
                "distance": "Cosine"
            },
            "on_disk_payload": False,  # True in example issue
            "optimizers_config": {
                "indexing_threshold": 1,
            }
        }
        client.create_collection(collection_name, collection_config)

        # 2. Create payload indexes using ClientUtils method
        client.create_payload_index(
            collection_name=collection_name,
            field_name="chunk_id",
            field_schema=models.PayloadSchemaType.UUID
        )

        client.create_payload_index(
            collection_name=collection_name,
            field_name="text",
            field_schema=models.TextIndexParams(
                type=models.TextIndexType.TEXT,
                tokenizer=models.TokenizerType.WORD,
                min_token_len=2,
                max_token_len=15,
                lowercase=True
            )
        )

        # 3. Insert 100 points
        points = []
        vectors_count = 100
        for i in range(vectors_count):
            point = models.PointStruct(
                id=i,
                vector=[round(random.uniform(0, 1), 2) for _ in range(vector_dim)],
                payload={
                    "chunk_id": str(uuid.uuid4()),
                    "library_id": str(uuid.uuid4()),
                    "folder_id": str(uuid.uuid4()),
                    "text": f"This is test text number {i} with some random words for indexing",
                    "media_id": str(uuid.uuid4())
                }
            )
            points.append(point)

        client.client.upsert(
            collection_name=collection_name,
            points=points,
            wait=True
        )

        status_result = client.wait_for_status(collection_name, "green")
        assert status_result == "ok", f"Collection did not reach green status within timeout"

        # 4. Verify points count using collection metadata
        collection_data = client.get_collection_info_dict(collection_name)

        collection_points_count = collection_data["result"]["points_count"]
        assert collection_points_count == vectors_count, f"Expected {vectors_count} points in collection, got {collection_points_count}"

        # Check payload schema exists and has expected number of points
        payload_schema = collection_data["result"].get("payload_schema", {})
        expected_fields = ["chunk_id", "text"]
        for field in expected_fields:
            assert field in payload_schema, f"Expected field '{field}' not found in payload schema"

            field_info = payload_schema[field]
            # Check that each indexed field has the expected number of points
            # The points count in the payload schema should equal the collection points count
            assert field_info.points == vectors_count, f"Expected {vectors_count} points in index '{field}', got {field_info.points}"
