#!/usr/bin/env python3

from qdrant_edge import *
from common import *

# Create *new* edge shard and upsert some points
edge = load_new_shard()

edge.update(UpdateOperation.upsert_points([
    Point(
        1,
        [6.0, 9.0, 4.0, 2.0],
        {
            "null": None,
            "str": "string",
            "uint": 42,
            "int": -69,
            "float": 4.20,
            "bool": True,
            "obj": {
                "null": None,
                "str": "string",
                "uint": 42,
                "int": -69,
                "float": 4.20,
                "bool": True,
                "obj": {},
                "arr": [],
            },
            "arr": [None, "string", 42, -69, 4.20, True, {}, []],
        },
    ),
    Point(
        "e9408f2b-b917-4af1-ab75-d97ac6b2c047",
        [6.0, 9.0, 3.0, -2.0],
        {
            "hello": "world",
            "price": 199.99,
        },
    ),
    Point(
        uuid.uuid4(),
        [1.0, 6.0, 4.0, 2.0],
        {
            "hello": "world",
            "price": 999.99,
        },
    ),
]))

expected_points = edge.scroll(ScrollRequest(limit=5, with_vector=True, with_payload=True))
print(expected_points)

# Re-load edge shard from disk, assert points are available
del edge
edge = EdgeShard(DATA_DIRECTORY, None)

points = edge.scroll(ScrollRequest(limit=5, with_vector=True, with_payload=True))
print(points)

# TODO: Implement `__eq__` for edge types 🌚
# assert points == expected_points
