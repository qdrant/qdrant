import argparse
import random
import uuid
from time import sleep

import requests


VECTOR_SIZE = 4


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


def create_collection(qdrant_host, collection_name, collection_json):
    resp = requests.put(
        f"{qdrant_host}/collections/{collection_name}?timeout=60", json=collection_json)
    if resp.status_code != 200:
        print(f"Collection creation failed with response body:\n{resp.json()}")
        exit(-1)


def update_collection(qdrant_host, collection_name, collection_json: dict):
    resp = requests.patch(
        f"{qdrant_host}/collections/{collection_name}?timeout=60", json=collection_json)
    if resp.status_code != 200:
        print(f"Collection patching failed with response body:\n{resp.json()}")
        exit(-1)


def wait_for_status(qdrant_host, collection_name, status: str):
    curr_status = ""
    for i in range(30):
        resp = requests.get(
            f"{qdrant_host}/collections/{collection_name}")
        curr_status = resp.json()["result"]["status"]
        if resp.status_code != 200:
            print(f"Collection info fetching failed with response body:\n{resp.json()}")
            exit(-1)
        if curr_status == status:
            print(f"Status {status}: OK")
            return "ok"
        sleep(1)
        print(f"Wait for status {status}")
    print(f"After 30s status is not {status}, found: {curr_status}. Stop waiting.")


def insert_points(qdrant_host, collection_name, batch_json, quit_on_ood: bool = False):
    resp = requests.put(
        f"{qdrant_host}/collections/{collection_name}/points?wait=true", json=batch_json
    )
    expected_error_message = "No space left on device"
    if resp.status_code != 200:
        if resp.status_code == 500 and expected_error_message in resp.text:
            if quit_on_ood:
                return "ood"
            requests.put(f"{qdrant_host}/collections/{collection_name}/points?wait=true", json=batch_json)
        else:
            error_response = resp.json()
            print(f"Points insertions failed with response body:\n{error_response}")
            exit(-2)


def search_point(qdrant_host, collection_name):
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
        f"{qdrant_host}/collections/{collection_name}/points/search", json=query
    )

    if resp.status_code != 200:
        print("Search failed")
        exit(-3)
    return resp.json()


def insert_points_and_search(qdrant_host, collection_name, points_amount):
    collection_json = {
        "vectors": {
            "size": VECTOR_SIZE,
            "distance": "Cosine"
        }
    }
    create_collection(qdrant_host, collection_name, collection_json)
    for points_batch in generate_points(points_amount):
        insert_points(qdrant_host, collection_name, points_batch)

    search_point(qdrant_host, collection_name)


def insert_points_then_index(qdrant_host, collection_name, points_amount):
    """
    Disable indexing, create collection, insert points up to
    a point when there is no space left on disk, then start indexing.
    Wait for indexing then send a search request.
    @param qdrant_host:
    @param collection_name:
    @param points_amount:
    @return:
    """
    collection_json = {
        "vectors": {
            "size": VECTOR_SIZE,
            "distance": "Cosine"
        },
        "optimizers_config": {
            "indexing_threshold": 0,
            "default_segment_number": 2
        },
        "wal_config": {
            "wal_capacity_mb": 1
        }
    }
    create_collection(qdrant_host, collection_name, collection_json)
    for points_batch in generate_points(points_amount):
        res = insert_points(qdrant_host, collection_name, points_batch, quit_on_ood=True)
        if res == "ood":
            break

    # start indexing
    collection_json = {
        "optimizers_config": {
            "indexing_threshold": 10
        }
    }
    update_collection(qdrant_host, collection_name, collection_json)
    wait_for_status(qdrant_host, collection_name, "yellow")
    wait_for_status(qdrant_host, collection_name, "green")
    search_point(qdrant_host, collection_name)


def main():
    parser = argparse.ArgumentParser("Create all required test items")
    parser.add_argument("test_item")
    parser.add_argument("collection_name")
    parser.add_argument("points_amount", type=int)
    parser.add_argument("ports", type=int, nargs="+")
    args = parser.parse_args()

    qdrant_host = f"http://127.0.0.1:{args.ports[0]}"
    if args.test_item == "search":
        print("run search test")
        insert_points_and_search(qdrant_host, args.collection_name, args.points_amount)
    elif args.test_item == "indexing":
        print("run indexing test")
        insert_points_then_index(qdrant_host, args.collection_name, args.points_amount)
    else:
        raise ValueError("Invalid test_item value, allowed: search|indexing")
    print("SUCCESS")


if __name__ == "__main__":
    main()
