import argparse
import requests
import time

from assertions import assert_http_ok

parser = argparse.ArgumentParser("Move all shards to first node and detach the last one")
parser.add_argument("collection_name")
parser.add_argument("ports", type=int, nargs="+")
args = parser.parse_args()


# Check collection cluster distribution
r = requests.get(f"http://127.0.0.1:{args.ports[0]}/collections/{args.collection_name}/cluster")
assert_http_ok(r)

"""
Example output:
{
    "result": {
        "peer_id": 1995564390118081002,
        "shard_count": 3,
        "local_shards": [
            {
                "shard_id": 0,
                "points_count": 1
            }
        ],
        "remote_shards": [
            {
                "shard_id": 1,
                "peer_id": 3084118168649519856
            },
            {
                "shard_id": 2,
                "peer_id": 10993045052641065997
            }
        ],
        "shard_transfers": []
    },
    "status": "ok",
    "time": 0.000013974
}
"""

collection_cluster_status = r.json()
from_peer = collection_cluster_status["result"]['peer_id']
to_peer = collection_cluster_status["result"]["remote_shards"][0]["peer_id"]


for local_shards in collection_cluster_status["result"]["local_shards"]:
    shard_id = local_shards["shard_id"]
    r = requests.post(f"http://127.0.0.1:{args.ports[0]}/collections/{args.collection_name}/cluster?timeout=60", json={
        "move_shard": {
            "from_peer_id": from_peer,
            "shard_id": shard_id,
            "to_peer_id": to_peer
        }
    })

max_wait = 60

# Check that the shard is moved
while max_wait > 0:
    r = requests.get(f"http://127.0.0.1:{args.ports[0]}/collections/{args.collection_name}/cluster")
    assert_http_ok(r)
    collection_cluster_status = r.json()

    if len(collection_cluster_status["result"]["local_shards"]) == 0:
        break

    max_wait -= 1
    time.sleep(1)


# Disconnect peer from the cluster
r = requests.delete(f"http://127.0.0.1:{args.ports[0]}/cluster/peer/{from_peer}?timeout=60")
assert_http_ok(r)

