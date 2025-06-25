import pytest
import time
import random
import uuid
from time import sleep
import requests
from docker.types import Mount
from typing import Literal


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


class TestLowDisk:
    """Test Qdrant behavior under low disk conditions."""
    
    @pytest.mark.parametrize("test_mode", ["search", "indexing"])
    def test_low_disk_handling(self, qdrant_container, test_mode: Literal["search", "indexing"]):
        """
        Test that Qdrant handles low disk conditions gracefully. Start a container with limited tmpfs mount (10MB),
        perform operations, ensure container doesn't crash.
        
        Args:
            test_mode: Either "search" (test during points insertion) or "indexing" (test during index building)
        """
        unique_suffix = str(uuid.uuid4())[:8]
        container_info = qdrant_container(
            name=f"qdrant-ood-{test_mode}-{unique_suffix}",
            mounts=[
                Mount(
                    target="/qdrant/storage",
                    source=None,
                    type="tmpfs",
                    tmpfs_size=10240000  # 10MB
                )
            ],
            remove=False  # Keep for logs
        )
        
        container = container_info["container"]
        qdrant_host = f"http://{container_info['host']}:{container_info['http_port']}"
        collection_name = "low-disk"
        points_amount = 2000
        
        if test_mode == "search":
            insert_points_and_search(qdrant_host, collection_name, points_amount)
        elif test_mode == "indexing":
            insert_points_then_index(qdrant_host, collection_name, points_amount)

        # Give some time for logs to be written
        time.sleep(5)

        logs = container.logs().decode('utf-8')
        expected_msg = "No space left on device:"
        assert expected_msg in logs, f"'{expected_msg}' log message not found in container logs"

        print(f"{test_mode}: OK")
