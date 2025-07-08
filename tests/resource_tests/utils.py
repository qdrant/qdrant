import hashlib
import io
import random
import time
import uuid

import requests

VECTOR_SIZE = 4


def wait_for_qdrant_ready(port: int = 6333, timeout: int = 30) -> bool:
    """Wait for Qdrant service to be ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"http://localhost:{port}/readyz")
            if response.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
    return False


def wait_for_server(host: str, port: int, timeout: int = 30):
    """Wait for Qdrant server to be ready."""
    base_url = f"http://{host}:{port}"

    for _ in range(timeout):
        try:
            response = requests.get(f"{base_url}/readyz", timeout=1)
            if response.status_code == 200:
                print("Server ready to serve traffic")
                return True
        except requests.RequestException:
            pass

        print("Waiting for server to start...")
        time.sleep(1)

    return False


def wait_for_collection_loaded(collection_name: str, port: int, timeout: int = 10) -> bool:
    """Wait for a specific collection to be loaded."""
    for _ in range(timeout):
        try:
            response = requests.get(f"http://localhost:{port}/collections")
            if response.status_code == 200:
                collections = response.json().get('result', {}).get('collections', [])
                if any(col.get('name') == collection_name for col in collections):
                    return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
    return False


def generate_points(amount: int):
    for _ in range(amount):
        result_item = {"points": []}
        for _ in range(100):
            result_item["points"].append(
                {
                    "id": str(uuid.uuid4()),
                    "vector": [round(random.uniform(0, 1), 2) for _ in range(VECTOR_SIZE)],
                    "payload": {"city": ["Berlin", "London"]}
                }
            )
        yield result_item


def create_collection(api_url, collection_name, collection_json):
    response = requests.put(
        f"{api_url}/collections/{collection_name}?timeout=60",
        json=collection_json
    )
    response.raise_for_status()
    return response.json()


def update_collection(api_url, collection_name, collection_json: dict):
    resp = requests.patch(
        f"{api_url}/collections/{collection_name}?timeout=60", json=collection_json)
    if resp.status_code != 200:
        print(f"Collection patching failed with response body:\n{resp.json()}")
        exit(-1)


def wait_for_status(api_url, collection_name, status: str):
    curr_status = ""
    for i in range(30):
        resp = requests.get(
            f"{api_url}/collections/{collection_name}")
        curr_status = resp.json()["result"]["status"]
        if resp.status_code != 200:
            print(f"Collection info fetching failed with response body:\n{resp.json()}")
            exit(-1)
        if curr_status == status:
            print(f"Status {status}: OK")
            return "ok"
        time.sleep(1)
        print(f"Wait for status {status}")
    print(f"After 30s status is not {status}, found: {curr_status}. Stop waiting.")


def insert_points(api_url, collection_name, batch_json, quit_on_ood: bool = False):
    resp = requests.put(
        f"{api_url}/collections/{collection_name}/points?wait=true", json=batch_json
    )
    expected_error_message = "No space left on device"
    if resp.status_code != 200:
        if resp.status_code == 500 and expected_error_message in resp.text:
            if quit_on_ood:
                return "ood"
            requests.put(f"{api_url}/collections/{collection_name}/points?wait=true", json=batch_json)
        else:
            error_response = resp.json()
            print(f"Points insertions failed with response body:\n{error_response}")
            exit(-2)


def search_point(api_url, collection_name):
    query = {
        "vector": [round(random.uniform(0, 1), 2) for _ in range(VECTOR_SIZE)],
        "top": 10,
        "filters": {
            "city": {
                "values": ["Berlin"],
                "excludes": False
            }
        }
    }
    resp = requests.post(
        f"{api_url}/collections/{collection_name}/points/search", json=query
    )

    if resp.status_code != 200:
        print("Search failed")
        exit(-3)
    return resp.json()


def create_snapshot(api_url, collection_name="test_collection"):
    """Create a snapshot of the collection."""
    response = requests.post(
        f"{api_url}/collections/{collection_name}/snapshots",
        json={}
    )
    response.raise_for_status()
    return response.json()["result"]["name"]


def download_snapshot(api_url, collection_name, snapshot_name):
    """Download a snapshot and return its content and checksum."""
    snapshot_url = f"{api_url}/collections/{collection_name}/snapshots/{snapshot_name}"
    response = requests.get(snapshot_url)
    response.raise_for_status()

    content = response.content
    checksum = hashlib.sha256(content).hexdigest()
    return content, checksum


def recover_snapshot_from_url(api_url, collection_name, snapshot_url, checksum=None):
    """Recover a collection from a snapshot URL."""
    body = {
        "location": snapshot_url,
        "wait": "true"
    }
    if checksum:
        body["checksum"] = checksum

    response = requests.put(
        f"{api_url}/collections/{collection_name}/snapshots/recover",
        json=body
    )
    if not response.ok:
        print(f"Recovery failed with status {response.status_code}: {response.text}")
    response.raise_for_status()
    return response.json()


def upload_snapshot_file(api_url, collection_name, snapshot_content):
    """Upload a snapshot file directly."""
    files = {
        'snapshot': ('snapshot.tar', io.BytesIO(snapshot_content), 'application/octet-stream')
    }

    response = requests.post(
        f"{api_url}/collections/{collection_name}/snapshots/upload",
        files=files
    )
    response.raise_for_status()
    return response.json()


def create_shard_snapshot(api_url, collection_name, shard_id=0):
    """Create a snapshot of a specific shard."""
    response = requests.post(
        f"{api_url}/collections/{collection_name}/shards/{shard_id}/snapshots",
        json={}
    )
    response.raise_for_status()
    return response.json()["result"]["name"]


def download_shard_snapshot(api_url, collection_name, shard_id, snapshot_name):
    """Download a shard snapshot and return its content."""
    snapshot_url = f"{api_url}/collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}"
    response = requests.get(snapshot_url)
    response.raise_for_status()
    return response.content


def recover_shard_snapshot_from_url(api_url, collection_name, shard_id, snapshot_url):
    """Recover a shard from a snapshot URL."""
    response = requests.put(
        f"{api_url}/collections/{collection_name}/shards/{shard_id}/snapshots/recover",
        json={
            "location": snapshot_url,
            "wait": "true"
        }
    )
    response.raise_for_status()
    return response.json()


def upload_shard_snapshot_file(api_url, collection_name, shard_id, snapshot_content):
    """Upload a shard snapshot file directly."""
    files = {
        'snapshot': ('shard_snapshot.tar', io.BytesIO(snapshot_content), 'application/octet-stream')
    }

    response = requests.post(
        f"{api_url}/collections/{collection_name}/shards/{shard_id}/snapshots/upload",
        files=files
    )
    response.raise_for_status()
    return response.json()


def verify_collection_exists(api_url, collection_name):
    """Verify that a collection exists and is accessible."""
    response = requests.get(f"{api_url}/collections/{collection_name}")
    response.raise_for_status()
    return response.json()
