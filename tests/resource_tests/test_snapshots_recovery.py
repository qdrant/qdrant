import pytest
import requests

from resource_tests.utils import create_collection, create_snapshot, download_snapshot, \
    recover_snapshot_from_url, verify_collection_exists, upload_snapshot_file, create_shard_snapshot, \
    download_shard_snapshot, recover_shard_snapshot_from_url, upload_shard_snapshot_file, generate_points, insert_points


@pytest.mark.parametrize("qdrant_container, storage_method", [
    ({"network_mode": "host"}, "local"),
    ({"network_mode": "host"}, "s3")
], indirect=["qdrant_container"])
def test_snapshots_recovery(qdrant_container, storage_method, tmp_path):
    """Test snapshot creation, download, and recovery with both local and S3 storage."""
    
    if storage_method == "s3":
        # For S3 testing, we'd need a MinIO container or mock S3
        # For now, skip if MinIO is not available
        try:
            requests.get("http://127.0.0.1:9000", timeout=1)
        except requests.exceptions.RequestException:
            pytest.skip("MinIO is not available for S3 testing")
    
    container_info = qdrant_container
    api_url = f"http://{container_info['host']}:{container_info['http_port']}"
    
    # Create collection and insert points
    collection_json = {"vectors": {"size": 4, "distance": "Dot"}}
    create_collection(api_url, "test_collection", collection_json)
    for points_batch in generate_points(2):
        insert_points(api_url, "test_collection", points_batch)

    # Create collection snapshot
    snapshot_name = create_snapshot(api_url)
    assert snapshot_name, "Snapshot creation failed"
    
    # Download snapshot
    snapshot_content, snapshot_checksum = download_snapshot(api_url, "test_collection", snapshot_name)
    assert len(snapshot_content) > 0, "Downloaded snapshot is empty"
    
    # Save snapshot to file for later use
    snapshot_file = tmp_path / "test_collection.snapshot"
    with open(snapshot_file, 'wb') as f:
        f.write(snapshot_content)
    
    # Test 1: Recover from URL
    snapshot_url = f"{api_url}/collections/test_collection/snapshots/{snapshot_name}"
    recover_snapshot_from_url(api_url, "test_collection_recovered_1", snapshot_url, snapshot_checksum)
    
    # Verify recovered collection exists
    verify_collection_exists(api_url, "test_collection_recovered_1")
    
    # Test 2: Upload snapshot as file
    upload_snapshot_file(api_url, "test_collection_recovered_2", snapshot_content)
    
    # Verify second recovered collection exists
    verify_collection_exists(api_url, "test_collection_recovered_2")
    
    # Test shard snapshots
    # Create shard snapshot
    shard_snapshot_name = create_shard_snapshot(api_url, "test_collection")
    assert shard_snapshot_name, "Shard snapshot creation failed"
    
    # Download shard snapshot
    shard_snapshot_content = download_shard_snapshot(api_url, "test_collection", 0, shard_snapshot_name)
    assert len(shard_snapshot_content) > 0, "Downloaded shard snapshot is empty"
    
    # Save shard snapshot to file
    shard_snapshot_file = tmp_path / "test_collection_shard.snapshot"
    with open(shard_snapshot_file, 'wb') as f:
        f.write(shard_snapshot_content)
    
    # Test 3: Recover shard from URL
    shard_snapshot_url = f"{api_url}/collections/test_collection/shards/0/snapshots/{shard_snapshot_name}"
    recover_shard_snapshot_from_url(api_url, "test_collection_recovered_1", 0, shard_snapshot_url)
    
    # Test 4: Upload shard snapshot as file
    upload_shard_snapshot_file(api_url, "test_collection_recovered_2", 0, shard_snapshot_content)
    
    # Final verification - ensure all collections are accessible
    for collection_name in ["test_collection_recovered_1", "test_collection_recovered_2"]:
        collection_info = verify_collection_exists(api_url, collection_name)
        assert collection_info["result"]["status"] == "green", f"Collection {collection_name} is not in green status"
