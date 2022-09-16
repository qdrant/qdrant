import argparse
import requests
import time


parser = argparse.ArgumentParser("Create test collection")
parser.add_argument("collection_name")
parser.add_argument("ports", type=int, nargs="+")
args = parser.parse_args()

# Create collection
r = requests.put(
    f"http://127.0.0.1:{args.ports[0]}/collections/{args.collection_name}?timeout=60", json={
        "vectors": {
            "size": 4,
            "distance": "Dot"
        },
        "shard_number": 6
    })
assert r.status_code == 200, r.text

# Wait
time.sleep(5)

# Check that it exists on all peers
for port in args.ports:
    r = requests.get(f"http://127.0.0.1:{port}/collections")
    assert r.status_code == 200
    collections = r.json()["result"]["collections"]
    assert any(collection["name"] ==
               args.collection_name for collection in collections)
