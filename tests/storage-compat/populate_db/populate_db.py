from typing import List

import requests
import random
import os

QDRANT_HOST = os.environ.get("QDRANT_HOST", "localhost:6333")


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
        json={"field_name": "city", "field_type": "keyword"},
    )
    assert response.ok

    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/index",
        json={"field_name": "count", "field_type": "integer"},
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


def upload_points(name: str):
    random.seed(42)

    response = requests.put(
        f"http://{QDRANT_HOST}/collections/{name}/points?wait=true",
        headers={"Content-Type": "application/json"},
        json={
            "points": [
                {
                    "id": 1,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {
                        "city": "Berlin",
                        "country": "Germany",
                        "count": 1000000,
                        "square": 12.5,
                        "coords": {"lat": 1.0, "lon": 2.0},
                    },
                },
                {
                    "id": 2,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 3,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 4,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 5,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 8,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 9,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 10,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 11,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 12,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 13,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 14,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 15,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 16,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 17,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 18,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 19,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 20,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 21,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 22,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 23,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 24,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 25,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 26,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 27,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 28,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 29,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 30,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 31,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 32,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 33,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 34,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 35,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 36,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 37,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 38,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 39,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 40,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 41,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 42,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 43,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 44,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 45,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 46,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 47,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 48,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 49,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 50,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 51,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 52,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 53,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 54,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 55,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 56,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 57,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 58,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 59,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 60,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 61,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 62,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 63,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 64,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 65,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 66,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 67,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 68,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 69,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 70,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 71,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 72,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 73,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 74,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 75,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 76,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 77,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 78,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 79,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 80,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 81,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 82,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 83,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 84,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 85,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 86,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 87,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 88,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 89,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 90,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 91,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 92,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 93,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": 94,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": 95,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
                {
                    "id": 96,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                },
                {
                    "id": 97,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "London"]},
                },
                {
                    "id": 98,
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["Berlin", "Moscow"]},
                },
                {
                    "id": "98a9a4b1-4ef2-46fb-8315-a97d874fe1d7",
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"city": ["London", "Moscow"]},
                },
                {
                    "id": "f0e09527-b096-42a8-94e9-ea94d342b925",
                    "vector": {
                        "image": rand_dense_vec(),
                        "text": rand_sparse_vec(),
                    },
                    "payload": {"count": [0]},
                },
            ]
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
