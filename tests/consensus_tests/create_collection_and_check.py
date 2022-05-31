import argparse
import requests
import time


parser = argparse.ArgumentParser("Create test collection")
parser.add_argument("collection_name")
parser.add_argument("ports", type=int, nargs="+")
args = parser.parse_args()

# Create collection
r = requests.put(
    f"http://127.0.0.1:{args.ports[0]}/collections/{args.collection_name}", json={
        "vector_size": 4,
        "distance": "Dot",
        "shard_number": 4
    })
assert r.status_code == 200

# Wait
time.sleep(5)

# Check that it exists on all peers
for port in args.ports:
    r = requests.get(f"http://127.0.0.1:{port}/collections")
    assert r.status_code == 200
    assert r.json()[
        "result"]["collections"][0]["name"] == args.collection_name
