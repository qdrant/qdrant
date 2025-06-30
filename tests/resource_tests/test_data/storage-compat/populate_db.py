#!/usr/bin/env python3

import os
import random
import uuid
import datetime
from typing import List, Optional

import requests

QDRANT_HOST = os.environ.get("QDRANT_HOST", "localhost:6333")

POINTS_COUNT = 1000
DENSE_DIM = 256
MULTI_DENSE_DIM = 128


def drop_collection(name: str):
    # cleanup collection if it exists
    requests.delete(f"http://{QDRANT_HOST}/collections/{name}")


def create_collection(name: str, memmap_threshold_kb: int, on_disk: bool, datatype: str, quantization_config: Optional[dict] = None):
    # create collection with a lower `indexing_threshold_kb` to generate the HNSW index
    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}",
        headers={"Content-Type": "application/json"},
        json={
            "vectors": {
                "image": {
                    "size": DENSE_DIM,
                    "distance": "Dot",
                    "on_disk": on_disk,
                    "datatype": datatype,
                },
                "multi-image": {
                    "size": MULTI_DENSE_DIM,
                    "distance": "Dot",
                    "on_disk": on_disk,
                    "datatype": datatype,
                    "multivector_config": {
                        "comparator": "max_sim"
                    }
                }
            },
            "sparse_vectors": {
                "text": {
                    "index": {
                        "on_disk": on_disk,
                        "datatype": datatype,
                    }
                }
            },
            "optimizers_config": {
                "default_segment_number": 2,
                "indexing_threshold_kb": 10,
                "memmap_threshold_kb": memmap_threshold_kb,

            },
            "quantization_config": quantization_config,
            "on_disk_payload": on_disk,
        },
    )
    assert response.ok


def create_payload_indexes(name: str, on_disk_payload_index: bool):
    # keyword
    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={
            "field_name": "keyword_field",
            "field_schema": {
                "type": "keyword",
                "on_disk": on_disk_payload_index
            }
        },
    )
    assert response.ok

    # float
    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={
            "field_name": "float_field",
            "field_schema": {
                "type": "float",
                "on_disk": on_disk_payload_index
            }
        },
    )
    assert response.ok

    # integer
    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={
            "field_name": "integer_field",
            "field_schema": {
                "type": "integer",
                "on_disk": on_disk_payload_index,
                "lookup": True,
                "range": True
            }
        },
    )
    assert response.ok

    # boolean
    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={
            "field_name": "boolean_field",
            "field_schema": {
                "type": "bool",
                "on_disk": on_disk_payload_index
            }
        },
    )
    assert response.ok

    # geo
    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={
            "field_name": "geo_field",
            "field_schema": {
                "type": "geo",
                "on_disk": on_disk_payload_index
            }
        },
    )
    assert response.ok

    # text
    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={
            "field_name": "text_field",
            "field_schema": {
                "type": "text",
                "tokenizer": "word",
                "min_token_len": 2,
                "max_token_len": 20,
                "lowercase": True,
                "on_disk": on_disk_payload_index,
            },
        },
    )
    assert response.ok

    # uuid
    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={
            "field_name": "uuid_field",
            "field_schema": {
                "type": "uuid",
                "on_disk": on_disk_payload_index
            }
        },
    )
    assert response.ok

    # datetime
    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={
            "field_name": "datetime_field",
            "field_schema": {
                "type": "datetime",
                "on_disk": on_disk_payload_index
            }
        },
    )
    assert response.ok


def rand_dense_vec(dims: int = DENSE_DIM):
    return [(random.random() * 20) - 10 for _ in range(dims)]


# Create multiple dense vectors
def random_multi_dense_vec(dims: int = MULTI_DENSE_DIM):
    return [rand_dense_vec(dims) for _ in range(3)]


# Generate random sparse vector with given size and density
# The density is the probability of non-zero value over the whole vector
def rand_sparse_vec(size: int = 1000, density: float = 0.1):
    num_non_zero = int(size * density)
    indices: List[int] = random.sample(range(size), num_non_zero)
    values: List[float] = [round(random.random(), 6) for _ in range(num_non_zero)]
    sparse = {
        "indices": indices,
        "values": values,
    }
    return sparse


def rand_string():
    return random.choice(["hello", "world", "foo", "bar"])


def rand_int():
    return random.randint(0, 100)


def rand_bool():
    return random.random() < 0.5


def rand_text():
    return " ".join([rand_string() for _ in range(10)])


def rand_geo():
    return {
        "lat": random.random(),
        "lon": random.random(),
    }

def rand_uuid():
    return str(uuid.uuid4())


def rand_datetime():
    return str(datetime.datetime.now())


def single_or_multi_value(generator):
    if random.random() < 0.5:
        return generator()
    else:
        return [generator() for _ in range(random.randint(1, 3))]


def rand_point(num: int, use_uuid: bool):
    point_id = None
    if use_uuid:
        point_id = str(uuid.uuid1())
    else:
        point_id = num

    # draw [0, 1)
    vec_draw = random.random()
    vec = {}
    if vec_draw < 0.2:
        # just a dense vector
        vec = {"image": rand_dense_vec()}
    elif vec_draw < 0.4:
        # just a multi dense vector
        vec = {"multi-image": random_multi_dense_vec()}
    elif vec_draw < 0.6:
        # just a sparse vector
        vec = {"text": rand_sparse_vec()}
    else:
        # else mixed vector
        vec = {
            "image": rand_dense_vec(),
            "text": rand_sparse_vec(),
            "multi-image": random_multi_dense_vec(),
        }

    payload = {}
    if random.random() < 0.5:
        payload["keyword_field"] = single_or_multi_value(rand_string)

    if random.random() < 0.5:
        payload["count_field"] = single_or_multi_value(rand_int)

    if random.random() < 0.5:
        payload["float_field"] = single_or_multi_value(random.random)

    if random.random() < 0.5:
        payload["integer_field"] = single_or_multi_value(rand_int)

    if random.random() < 0.5:
        payload["boolean_field"] = single_or_multi_value(rand_bool)

    if random.random() < 0.5:
        payload["geo_field"] = single_or_multi_value(rand_geo)

    if random.random() < 0.5:
        payload["text_field"] = single_or_multi_value(rand_text)

    if random.random() < 0.5:
        payload["uuid_field"] = single_or_multi_value(rand_uuid)

    if random.random() < 0.5:
        payload["datetime_field"] = single_or_multi_value(rand_datetime)

    point = {
        "id": point_id,
        "vector": vec,
        "payload": payload,
    }
    return point


def upload_points(name: str):
    random.seed(42)

    points = []
    for i in range(POINTS_COUNT):
        # Use uuid as id for half of the points
        use_uuid = i > POINTS_COUNT / 2
        point = rand_point(i, use_uuid)
        points.append(point)

    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/points?wait=true",
        headers={"Content-Type": "application/json"},
        json={
            "points": points,
        },
    )

    assert response.ok


def basic_retrieve(name: str):
    response = requests.get(
        f"http://{QDRANT_HOST}/collections/{name}/points/2",
        headers={"Content-Type": "application/json"},
    )
    assert response.ok

    response = requests.post(
        f"http://{QDRANT_HOST}/collections/{name}/points",
        headers={"Content-Type": "application/json"},
        json={"ids": [1, 2]},
    )
    assert response.ok


# Populate collection with different configurations
#
# There are two ways to configure the usage of memmap storage:
# - `memmap_threshold_kb` - the threshold for the indexer to use memmap storage
# - `on_disk` - to store vectors immediately on disk
def populate_collection(name: str, on_disk: bool, quantization_config: Optional[dict] = None, memmap_threshold: bool = False, on_disk_payload_index: bool = False, datatype: str = "float32"):
    drop_collection(name)
    memmap_threshold_kb = 0
    if memmap_threshold:
        memmap_threshold_kb = 10  # low value to force transition to memmap storage
    create_collection(name, memmap_threshold_kb, on_disk, datatype, quantization_config)
    create_payload_indexes(name, on_disk_payload_index)
    upload_points(name)
    basic_retrieve(name)


if __name__ == "__main__":
    # Create collection
    populate_collection("test_collection_vector_memory", on_disk=False)
    populate_collection("test_collection_vector_on_disk", on_disk=True)
    populate_collection("test_collection_vector_on_disk_threshold", on_disk=False, memmap_threshold=True)
    populate_collection("test_collection_scalar_int8", on_disk=False, quantization_config={"scalar": {"type": "int8"}})
    populate_collection("test_collection_product_x64", on_disk=False, quantization_config={"product": {"compression": "x64"}})
    populate_collection("test_collection_product_x32", on_disk=False, quantization_config={"product": {"compression": "x32"}})
    populate_collection("test_collection_product_x16", on_disk=False, quantization_config={"product": {"compression": "x16"}})
    populate_collection("test_collection_product_x8", on_disk=False, quantization_config={"product": {"compression": "x8"}})
    populate_collection("test_collection_binary", on_disk=False, quantization_config={"binary": {"always_ram": True}})
    populate_collection("test_collection_mmap_field_index", on_disk=True, on_disk_payload_index=True)
    populate_collection("test_collection_vector_datatype_u8", on_disk=True, datatype="uint8")
    populate_collection("test_collection_vector_datatype_f16", on_disk=True, datatype="float16")
