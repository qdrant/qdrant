import os
import requests
import shutil

from common import DATA_DIRECTORY

from qdrant_edge import *

SNAPSHOT_URL = 'https://storage.googleapis.com/qdrant-benchmark-snapshots/test-shard.snapshot'


# Download the snapshot file into data directory
def download_snapshot(url: str, dest_folder: str):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    local_filename = os.path.join(dest_folder, url.split('/')[-1])
    if os.path.exists(local_filename):
        print(f"Snapshot already exists at: {local_filename}")
        return local_filename

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename

snapshot_path = download_snapshot(SNAPSHOT_URL, DATA_DIRECTORY)
print(f"Snapshot downloaded to: {snapshot_path}")

recovered_path = os.path.join(DATA_DIRECTORY, "restored_shard")
print(f"Restoring shard from snapshot to: {recovered_path}")
if os.path.exists(recovered_path):
    print("Removing existing recovered shard directory...")
    shutil.rmtree(recovered_path)

Shard.unpack_snapshot(snapshot_path, recovered_path)

shard = Shard(recovered_path, None)

points = shard.retrieve(point_ids=[1, 2, 3], with_vector=False, with_payload=True)

for point in points:
    print(point)

print("Manifest of restored shard:", shard.snapshot_manifest())