import hashlib
import io
import random
import requests
import time
import uuid
from typing import Optional, Dict, Any, List, Generator, Tuple

from qdrant_client import QdrantClient, models

VECTOR_SIZE = 4


class ClientUtils:
    """Utility class for Qdrant operations using qdrant-client."""

    def __init__(self, host: str = "localhost", port: int = 6333, timeout: int = 60):
        """Initialize the ClientUtils with a QdrantClient instance."""
        self.host = host
        self.port = port
        self.timeout = timeout
        self.client = QdrantClient(host=host, port=port, timeout=timeout)

    def wait_for_server(self, timeout: int = 30) -> bool:
        """Wait for Qdrant server to be ready."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                info = self.client.get_collections()
                if info is not None:
                    print("Server ready to serve traffic")
                    return True
            except Exception:
                pass

            print("Waiting for server to start...")
            time.sleep(1)
        return False

    def wait_for_collection_loaded(self, collection_name: str, timeout: int = 10) -> bool:
        """Wait for a specific collection to be loaded."""
        for _ in range(timeout):
            try:
                collections_response = self.client.get_collections()
                if collections_response and collections_response.collections:
                    collection_names = [col.name for col in collections_response.collections]
                    if collection_name in collection_names:
                        return True
            except Exception:
                pass
            time.sleep(1)
        return False

    def list_collections_names(self) -> List[str]:
        """List all collections names."""
        try:
            collections_response = self.client.get_collections()
            if collections_response and collections_response.collections:
                return [col.name for col in collections_response.collections]
            return []
        except Exception as e:
            raise Exception(f"Failed to list collections: {e}") from e

    def get_collection_info_dict(self, collection_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific collection."""
        try:
            collection_info = self.client.get_collection(collection_name)
            return {
                "result": {
                    "status": collection_info.status.value if hasattr(collection_info.status, 'value') else str(collection_info.status),
                    "vectors_count": collection_info.vectors_count,
                    "points_count": collection_info.points_count,
                    "config": collection_info.config,
                    "indexed_vectors_count": collection_info.indexed_vectors_count,
                    "payload_schema": collection_info.payload_schema
                },
                "status": "ok"
            }
        except Exception as e:
            raise Exception(f"Failed to get collection info for '{collection_name}': {e}") from e

    @staticmethod
    def generate_points(amount: int, vector_size: int = VECTOR_SIZE) -> Generator[Dict[str, List[models.PointStruct]], None, None]:
        """Generate batches of points for insertion."""
        for _ in range(amount):
            points = []
            for _ in range(100):
                points.append(
                    models.PointStruct(
                        id=str(uuid.uuid4()),
                        vector=[round(random.uniform(0, 1), 2) for _ in range(vector_size)],
                        payload={"city": ["Berlin", "London"]}
                    )
                )
            yield {"points": points}

    def create_collection(self, collection_name: str, collection_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a collection with the given configuration."""
        vectors_config = collection_config.get("vectors", {})

        # Handle vector configuration
        if isinstance(vectors_config, dict) and "size" in vectors_config:
            vector_params = models.VectorParams(
                size=vectors_config["size"],
                distance=models.Distance(vectors_config.get("distance", "Cosine"))
            )
        else:
            vector_params = models.VectorParams(size=VECTOR_SIZE, distance=models.Distance.COSINE)

        # Create collection
        result = self.client.create_collection(
            collection_name=collection_name,
            vectors_config=vector_params,
            shard_number=collection_config.get("shard_number"),
            replication_factor=collection_config.get("replication_factor"),
            write_consistency_factor=collection_config.get("write_consistency_factor"),
            on_disk_payload=collection_config.get("on_disk_payload"),
            hnsw_config=collection_config.get("hnsw_config"),
            optimizers_config=collection_config.get("optimizers_config"),
            wal_config=collection_config.get("wal_config"),
            quantization_config=collection_config.get("quantization_config"),
            init_from=collection_config.get("init_from"),
            timeout=self.timeout
        )

        return {"result": result, "status": "ok"}

    def update_collection(self, collection_name: str, collection_params: Dict[str, Any]) -> None:
        """Update collection parameters."""
        try:
            self.client.update_collection(
                collection_name=collection_name,
                optimizers_config=collection_params.get("optimizers_config"),
                collection_params=collection_params.get("collection_params"),
                vectors_config=collection_params.get("vectors_config"),
                hnsw_config=collection_params.get("hnsw_config"),
                quantization_config=collection_params.get("quantization_config"),
                timeout=self.timeout
            )
        except Exception as e:
            print(f"Collection patching failed with error: {e}")
            raise RuntimeError(f"Collection patching failed: {e}") from e

    def wait_for_status(self, collection_name: str, status: str) -> str:
        """Wait for collection to reach the specified status."""
        for _ in range(30):
            try:
                collection_info = self.client.get_collection(collection_name)
                curr_status = collection_info.status.value if hasattr(collection_info.status, 'value') else str(collection_info.status)

                if curr_status.lower() == status.lower():
                    print(f"Status {status}: OK")
                    return "ok"

                time.sleep(1)
                print(f"Wait for status {status}")
            except Exception as e:
                print(f"Collection info fetching failed with error: {e}")
                raise RuntimeError(f"Collection info fetching failed: {e}") from e

        print(f"After 30s status is not {status}. Stop waiting.")
        return "timeout"

    def insert_points(self, collection_name: str, batch_data: Dict[str, Any], quit_on_ood: bool = False, wait: bool = True) -> Optional[str]:
        """Insert points into the collection."""
        try:
            # Convert dict format to PointStruct if needed
            points = batch_data.get("points", [])
            if points and isinstance(points[0], dict):
                points = [
                    models.PointStruct(
                        id=p["id"],
                        vector=p["vector"],
                        payload=p.get("payload")
                    ) for p in points
                ]

            self.client.upsert(
                collection_name=collection_name,
                points=points,
                wait=wait
            )

        except Exception as e:
            expected_error_message = "No space left on device"
            if expected_error_message in str(e):
                if quit_on_ood:
                    return "ood"
            else:
                print(f"Points insertions failed with error: {e}")
                raise RuntimeError(f"Points insertion failed: {e}") from e

    def search_points(self, collection_name: str) -> Dict[str, Any]:
        """Search for points in the collection using the modern query_points API."""
        try:
            results = self.client.query_points(
                collection_name=collection_name,
                query=[round(random.uniform(0, 1), 2) for _ in range(VECTOR_SIZE)],
                limit=10,
                query_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="city",
                            match=models.MatchValue(value="Berlin")
                        )
                    ]
                )
            )

            # Convert results to expected format
            return {
                "result": [
                    {
                        "id": hit.id,
                        "score": hit.score,
                        "payload": hit.payload,
                        "vector": hit.vector
                    } for hit in results.points
                ],
                "status": "ok"
            }

        except Exception as e:
            raise RuntimeError("Search failed") from e

    def create_snapshot(self, collection_name: str = "test_collection") -> Optional[str]:
        """Create a snapshot of the collection."""
        snapshot_info = self.client.create_snapshot(collection_name=collection_name)
        return snapshot_info.name if snapshot_info else None

    def download_snapshot(self, collection_name: str, snapshot_name: str) -> Tuple[bytes, str]:
        """Download a snapshot and return its content and checksum."""
        # Note: qdrant-client doesn't have a direct method to download snapshot content
        # This would need to be implemented using the REST API directly
        snapshot_url = f"http://{self.host}:{self.port}/collections/{collection_name}/snapshots/{snapshot_name}"
        response = requests.get(snapshot_url)
        response.raise_for_status()

        content = response.content
        checksum = hashlib.sha256(content).hexdigest()
        return content, checksum

    def recover_snapshot_from_url(self, collection_name: str, snapshot_url: str, checksum: Optional[str] = None) -> Dict[str, Any]:
        """Recover a collection from a snapshot URL."""
        # Note: qdrant-client doesn't have a direct method for URL recovery
        # This would need to be implemented using the REST API directly
        body = {
            "location": snapshot_url,
            "wait": "true"
        }
        if checksum:
            body["checksum"] = checksum

        response = requests.put(
            f"http://{self.host}:{self.port}/collections/{collection_name}/snapshots/recover",
            json=body
        )
        if not response.ok:
            print(f"Recovery failed with status {response.status_code}: {response.text}")
        response.raise_for_status()
        return response.json()

    def upload_snapshot_file(self, collection_name: str, snapshot_content: bytes) -> Dict[str, Any]:
        """Upload a snapshot file directly."""
        # Note: qdrant-client doesn't have a direct method for snapshot upload
        # This would need to be implemented using the REST API directly
        files = {
            'snapshot': ('snapshot.tar', io.BytesIO(snapshot_content), 'application/octet-stream')
        }

        response = requests.post(
            f"http://{self.host}:{self.port}/collections/{collection_name}/snapshots/upload",
            files=files
        )
        response.raise_for_status()
        return response.json()

    def create_shard_snapshot(self, collection_name: str, shard_id: int = 0) -> str:
        """Create a snapshot of a specific shard."""
        snapshot_info = self.client.create_shard_snapshot(
            collection_name=collection_name,
            shard_id=shard_id
        )
        return snapshot_info.name

    def download_shard_snapshot(self, collection_name: str, shard_id: int, snapshot_name: str) -> bytes:
        """Download a shard snapshot and return its content."""
        # Note: qdrant-client doesn't have a direct method to download shard snapshot content
        # This would need to be implemented using the REST API directly
        snapshot_url = f"http://{self.host}:{self.port}/collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}"
        response = requests.get(snapshot_url)
        response.raise_for_status()
        return response.content

    def recover_shard_snapshot_from_url(self, collection_name: str, shard_id: int, snapshot_url: str) -> Dict[str, Any]:
        """Recover a shard from a snapshot URL."""
        result = self.client.recover_shard_snapshot(
            collection_name=collection_name,
            shard_id=shard_id,
            location=snapshot_url,
            wait=True
        )
        return {"result": result, "status": "ok"}

    def upload_shard_snapshot_file(self, collection_name: str, shard_id: int, snapshot_content: bytes) -> Dict[str, Any]:
        """Upload a shard snapshot file directly."""
        # Note: qdrant-client doesn't have a direct method for shard snapshot upload
        # This would need to be implemented using the REST API directly
        files = {
            'snapshot': ('shard_snapshot.tar', io.BytesIO(snapshot_content), 'application/octet-stream')
        }

        response = requests.post(
            f"http://{self.host}:{self.port}/collections/{collection_name}/shards/{shard_id}/snapshots/upload",
            files=files
        )
        response.raise_for_status()
        return response.json()

    def verify_collection_exists(self, collection_name: str) -> Dict[str, Any]:
        """Verify that a collection exists and is accessible."""
        collection_info = self.client.get_collection(collection_name)
        return {
            "result": {
                "status": collection_info.status,
                "points_count": collection_info.points_count,
                "config": collection_info.config
            },
            "status": "ok"
        }

    def create_payload_index(self, collection_name: str, field_name: str, 
                            field_schema: Any, wait: bool = True) -> bool:
        """
        Create a payload index for a field in the collection.

        Args:
            collection_name: Name of the collection
            field_name: Name of the field to index
            field_schema: Schema for the field - can be:
                - models.PayloadSchemaType.UUID for UUID fields
                - models.PayloadSchemaType.KEYWORD for keyword fields
                - models.TextIndexParams for text fields
                - models.KeywordIndexParams for keyword fields with options
            wait: Whether to wait for the operation to complete

        Returns:
            True if the index was created successfully
        """
        try:
            self.client.create_payload_index(
                collection_name=collection_name,
                field_name=field_name,
                field_schema=field_schema,
                wait=wait
            )
            return True
        except Exception as e:
            raise Exception(f"Failed to create payload index for field '{field_name}': {e}") from e
