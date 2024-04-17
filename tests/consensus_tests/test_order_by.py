from .assertions import assert_http_ok
from .fixtures import create_collection
from .utils import start_cluster, every_test

import pytest
import requests

COLL_NAME = "test_collection"


def test_order_by_from_remote_shard(tmp_path, every_test):
    peer_api_uris, peer_dirs, bootstrap_uri = start_cluster(tmp_path, num_peers=2, port_seed=10000)

    uri = peer_api_uris[0]

    create_collection(uri, collection=COLL_NAME, shard_number=2, replication_factor=1)

    requests.put(
        f"{uri}/collections/{COLL_NAME}/index",
        json={"field_name": "index", "field_schema": "integer"},
    ).raise_for_status()

    res = requests.put(
        f"{uri}/collections/{COLL_NAME}/points",
        params={"wait": "true", },
        json={
            "points": [
                {
                    "id": 0,
                    "payload": {
                        "index": 0,
                        "timestamp": "2024-04-12T08:40:06.189301",
                        "text": "Italy: tomatoes, olive oil, pasta.",
                        "title": "food",
                    },
                    "vector": {},
                }
            ]
        },
    )
    assert_http_ok(res)

    requests.put(
        f"{uri}/collections/{COLL_NAME}/points",
        params={"wait": "true", },
        json={
            "batch": {
                "ids": [11, 12, 13, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "payloads": [
                    {
                        "timestamp": "2024-04-12T08:40:20.015587",
                        "text": "Japan: Polite, punctual, public transport reliance",
                        "index": 11,
                        "title": "travel",
                    },
                    {
                        "timestamp": "2024-04-12T08:40:20.290047",
                        "text": "Italy: Relaxed, historical tours, slow-paced",
                        "index": 12,
                        "title": "travel",
                    },
                    {
                        "timestamp": "2024-04-12T08:40:20.562687",
                        "text": "USA: Road trips, diverse landscapes.",
                        "index": 13,
                        "title": "travel",
                    },
                    {
                        "timestamp": "2024-04-12T08:40:09.482208",
                        "text": "Japan: seafood-centric with artistic presentation.",
                        "index": 1,
                        "title": "food",
                    },
                    {
                        "timestamp": "2024-04-12T08:40:09.744967",
                        "text": "Mexico: beans, chili peppers, and meat.",
                        "index": 2,
                        "title": "food",
                    },
                    {
                        "timestamp": "2024-04-12T08:40:10.020073",
                        "text": "India: diverse vegetarian and meat dishes.",
                        "index": 3,
                        "title": "food",
                    },
                    {
                        "timestamp": "2024-04-12T08:40:10.285342",
                        "text": "France: Gourmet dining, focuses on cheese, wine",
                        "index": 4,
                        "title": "food",
                    },
                    {
                        "timestamp": "2024-04-12T08:40:10.883714",
                        "text": "China: diverse cuisines, emphasizes balance, uses rice",
                        "index": 5,
                        "title": "food",
                    },
                    {
                        "timestamp": "2024-04-12T08:40:13.166867",
                        "text": "Thailand: five flavors, spicy, sweet, salty, sour.",
                        "index": 6,
                        "title": "food",
                    },
                    {
                        "timestamp": "2024-04-12T08:40:15.436143",
                        "text": "USA: Melting pot, diverse, large portions, fast food.",
                        "index": 7,
                        "title": "food",
                    },
                    {
                        "timestamp": "2024-04-12T08:40:15.697163",
                        "text": "Brazil: barbecue heavy",
                        "index": 8,
                        "title": "food",
                    },
                    {
                        "timestamp": "2024-04-12T08:40:15.963504",
                        "text": "Greece: Olive oil, feta, yogurt, seafood",
                        "index": 9,
                        "title": "food",
                    },
                    {
                        "timestamp": "2024-04-12T08:40:16.738832",
                        "text": "Ethiopia: bread with spicy stews and vegetables",
                        "index": 10,
                        "title": "food",
                    },
                ],
                "vectors": {},
            }
        },
    ).raise_for_status()

    res = requests.post(
        f"{uri}/collections/{COLL_NAME}/points/scroll", json={"limit": 10, "order_by": "index"}
    )
    
    res.raise_for_status()
    
    for point in res.json()['result']["points"]:
        assert point["payload"] is not None

    ids = [point["payload"]["index"] for point in res.json()['result']["points"]]
    
    assert ids == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
