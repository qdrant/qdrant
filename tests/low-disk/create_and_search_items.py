import argparse
import random
import uuid

import requests


def generate_points(amount: int):
    for _ in range(amount):
        result_item = {"points": []}
        for _ in range(100):
            result_item["points"].append(
                {
                    "id": str(uuid.uuid4()),
                    "vector": [round(random.uniform(0, 1), 2) for _ in range(4)],
                    "payload": {"city": ["Berlin", "London"]}
                }
            )
        yield result_item


def create_collection(qdrant_host, collection_name):
    resp = requests.put(
        f"{qdrant_host}/collections/{collection_name}?timeout=60", json={
            "vectors": {
                "size": 4,
                "distance": "Cosine"
            }
        })
    if resp.status_code != 200:
        print(f"Collection creation failed with response body:\n{resp.json()}")
        exit(-1)


def insert_points(qdrant_host, collection_name, batch_json):
    resp = requests.put(
        f"{qdrant_host}/collections/{collection_name}/points?wait=true", json=batch_json
    )
    EXPECTED_ERROR_MESSAGE = "No space left on device"
    if resp.status_code != 200:
        if resp.status_code == 500 and EXPECTED_ERROR_MESSAGE in resp.text:
            requests.put(f"{qdrant_host}/collections/{collection_name}/points?wait=true", json=batch_json)
        else:
            error_response = resp.json()
            print(f"Points insertions failed with response body:\n{error_response}")
            exit(-2)


def search_point(qdrant_host, collection_name):
    query = {
        "vector": [round(random.uniform(0, 1), 2) for _ in range(4)],
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


def initialize_qdrant(qdrant_host, collection_name, points_amount):
    create_collection(qdrant_host, collection_name)
    for points_batch in generate_points(points_amount):
        insert_points(qdrant_host, collection_name, points_batch)
        search_point(qdrant_host, collection_name)


def main():
    parser = argparse.ArgumentParser("Create all required test items")
    parser.add_argument("collection_name")
    parser.add_argument("points_amount", type=int)
    parser.add_argument("ports", type=int, nargs="+")
    args = parser.parse_args()

    qdrant_host = f"http://127.0.0.1:{args.ports[0]}"
    initialize_qdrant(qdrant_host, args.collection_name, args.points_amount)
    print("SUCCESS")


if __name__ == "__main__":
    main()
