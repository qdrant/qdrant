import argparse
import requests
import time

from assertions import assert_http_ok

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
assert_http_ok(r)

# Wait
time.sleep(5)

MAX_WAIT = 30
# Check that it exists on all peers
while True:
    exists = True
    for port in args.ports:
        r = requests.get(f"http://127.0.0.1:{port}/collections")
        assert_http_ok(r)
        collections = r.json()["result"]["collections"]
        exists &= any(collection["name"] == args.collection_name for collection in collections)
    if exists:
        break
    else:
        # Wait until collection is created on all peers
        # Consensus guarantees that collection will appear on majority of peers, but not on all of them
        # So we need to wait a bit extra time
        time.sleep(1)
        MAX_WAIT -= 1
    if MAX_WAIT <= 0:
        raise Exception("Collection was not created on all peers in time")
