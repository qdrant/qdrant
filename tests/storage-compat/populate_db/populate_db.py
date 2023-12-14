from typing import List

import requests
import random
import uuid
import os

QDRANT_HOST = os.environ.get("QDRANT_HOST", "localhost:6333")

POINTS_COUNT = 100


def drop_collection(name: str):
    # cleanup collection if it exists
    requests.delete(f"http://{QDRANT_HOST}/collections/{name}")


def create_collection(name: str, quantization_config: dict = None):
    # create collection with a lower `indexing_threshold_kb` to generate the HNSW index
    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}",
        headers={"Content-Type": "application/json"},
        json={
            "vectors": {
                "image": {
                    "size": 256,
                    "distance": "Dot"
                }
            },
            "sparse_vectors": {
                "text": {
                    "index": {
                        "on_disk": True,
                    }
                }
            },
            "optimizers_config": {
                "default_segment_number": 2,
                "indexing_threshold_kb": 10,
            },
            "quantization_config": quantization_config,
        },
    )
    assert response.ok


def create_payload_indexes(name: str):
    # Create some payload indexes
    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={"field_name": "keyword_field", "field_type": "keyword"},
    )
    assert response.ok

    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={"field_name": "float_field", "field_type": "float"},
    )
    assert response.ok

    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={"field_name": "integer_field", "field_type": "integer"},
    )
    assert response.ok

    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={"field_name": "boolean_field", "field_type": "bool"},
    )
    assert response.ok

    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={"field_name": "geo_field", "field_type": "geo"},
    )
    assert response.ok

    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={
            "field_name": "text_field",
            "field_schema": {
                "type": "text",
                "tokenizer": "word",
                "min_token_len": 2,
                "max_token_len": 20,
                "lowercase": True
            }
        }
    )
    assert response.ok


def rand_dense_vec(dims: int = 256):
    return [(random.random() * 20) - 10 for _ in range(dims)]


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

    vec_draw = random.random()
    vec = {}
    if vec_draw < 0.3:
        # dense vector
        vec = {"image": rand_dense_vec()}
    elif vec_draw < 0.6:
        # sparse vector
        vec = {"text": rand_sparse_vec()}
    else:
        # mixed vector
        vec = {
            "image": rand_dense_vec(),
            "text": rand_sparse_vec(),
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


def populate_collection(name: str, quantization_config: dict = None):
    drop_collection(name)
    create_collection(name, quantization_config)
    create_payload_indexes(name)
    upload_points(name)
    basic_retrieve(name)


if __name__ == "__main__":
    # Create collection
    populate_collection("test_collection")
    populate_collection("test_collection_scalar_int8", {"scalar": {"type": "int8"}})
    populate_collection(
        "test_collection_product_x64", {"product": {"compression": "x64"}}
    )
    populate_collection(
        "test_collection_product_x32", {"product": {"compression": "x32"}}
    )
    populate_collection(
        "test_collection_product_x16", {"product": {"compression": "x16"}}
    )
    populate_collection(
        "test_collection_product_x8", {"product": {"compression": "x8"}}
    )
