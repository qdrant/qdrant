# This script downloads a partial snapshot using a predefined manifest.
import requests

manifest = {
    "65ac6276-8cca-4f5c-b767-9722190cee8b": {
        "segment_id": "65ac6276-8cca-4f5c-b767-9722190cee8b",
        "segment_version": 0,
        "file_versions": {
            "payload_storage/config.json": 0,
            "vector_storage/vectors/status.dat": None,
            "vector_storage/deleted/status.dat": None,
            "vector_storage/deleted/flags_a.dat": None,
            "payload_storage/gaps.dat": None,
            "vector_storage/vectors/config.json": 0,
            "payload_storage/page_0.dat": None,
            "payload_storage/bitmask.dat": None,
            "payload_index/config.json": None,
            "payload_storage/tracker.dat": None
        }
    },
    "4ea958d8-0b64-4312-9a53-0cd857e93535": {
        "segment_id": "4ea958d8-0b64-4312-9a53-0cd857e93535",
        "segment_version": 254,
        "file_versions": {
            "payload_storage/gaps.dat": None,
            "vector_storage/deleted/status.dat": None,
            "vector_storage/vectors/status.dat": None,
            "vector_storage/deleted/flags_a.dat": None,
            "payload_storage/config.json": 254,
            "vector_storage/vectors/config.json": 254,
            "vector_index/graph.bin": 254,
            "vector_index/links.bin": 254,
            "vector_index/hnsw_config.json": 254,
            "payload_index/config.json": None,
            "mutable_id_tracker.mappings": None,
            "mutable_id_tracker.versions": None,
            "vector_storage/vectors/chunk_0.mmap": None,
            "payload_storage/tracker.dat": None,
            "payload_storage/bitmask.dat": None,
            "payload_storage/page_0.dat": None
        }
    }
}

save_to = "partial.snapshot"
collection_name = "test"
shard_id = 0

url = f"http://localhost:6333/collections/{collection_name}/shards/{shard_id}/snapshot/partial/create"

response = requests.post(url, json=manifest, stream=True)
with response as r:
    r.raise_for_status()
    with open(save_to, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
