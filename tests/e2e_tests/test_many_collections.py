import pytest
from qdrant_client import models
from tqdm import tqdm

from e2e_tests.client_utils import ClientUtils


class TestManyCollections:
    @pytest.mark.longrunning
    @pytest.mark.parametrize("qdrant_compose", [
        {"compose_file": "3-node-cluster.yaml"}
    ], indirect=True)
    def test_many_collections_do_not_cause_panic(self, qdrant_compose):
        """Test if creating a lot of collections causes panic."""
        client = ClientUtils(host=qdrant_compose[0].host, port=qdrant_compose[0].http_port, timeout=10)
        client.wait_for_server()

        for i in tqdm(range(800), desc="Creating collections"):
            collection_config = {
                "vectors": {"size": 128, "distance": "Dot", "on_disk": True},
                "shard_number": 3,
                "replication_factor": 2,
                "optimization_config": {"indexing_threshold": 0},
                "hnsw_config": {"on_disk": True},
                "on_disk_payload": True
            }
            collection_name = f"collection_{i}"
            client.create_collection(collection_name, collection_config)

            client.create_payload_index(
                collection_name=collection_name,
                field_name="keyword_field",
                field_schema=models.PayloadSchemaType.KEYWORD
            )
            client.create_payload_index(
                collection_name=collection_name,
                field_name="integer_field",
                field_schema=models.PayloadSchemaType.INTEGER
            )
            client.create_payload_index(
                collection_name=collection_name,
                field_name="float_field",
                field_schema=models.PayloadSchemaType.FLOAT
            )
            client.create_payload_index(
                collection_name=collection_name,
                field_name="timestamp_field",
                field_schema=models.PayloadSchemaType.DATETIME
            )
            client.create_payload_index(
                collection_name=collection_name,
                field_name="chunk_id",
                field_schema=models.PayloadSchemaType.UUID
            )
            client.create_payload_index(
                collection_name=collection_name,
                field_name="geo",
                field_schema=models.PayloadSchemaType.GEO
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
            payloads = {
                "keyword": "keyword_field",
                "integer": "integer_field",
                "float": "float_field",
                "timestamp": "timestamp_field",
                "uuid": "chunk_id",
                "geo": "geo",
                "text": "text"
            }

            for points_batch in client.generate_points_with_payload(10, vector_size=128, payloads_map=payloads):
                client.insert_points(collection_name, points_batch, wait=True)

        client.get_collection_info_dict("collection_0")