import os
import shutil
import uuid

from qdrant_edge import *

print("---- Load shard ----")

DATA_DIRECTORY = "./data"

# Clear and recreate data directory
if os.path.exists(DATA_DIRECTORY):
    shutil.rmtree(DATA_DIRECTORY)

os.makedirs(DATA_DIRECTORY)

# Load Qdrant Edge shard
config = SegmentConfig(
    vector_data={
        "": VectorDataConfig(
            size=4,
            distance=Distance.DOT,
            storage_type=VectorStorageType.CHUNKED_MMAP,
            index=Indexes.PLAIN,
            quantization_config=None,
            multivector_config=None,
            datatype=None,
        ),
    },
    sparse_vector_data={},
    payload_storage_type=PayloadStorageType.IN_RAM_MMAP,
)

shard = Shard(DATA_DIRECTORY, config)

print("---- Upsert ----")

shard.update(UpdateOperation.upsert_points([
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

print("---- Some other points ----")

some_other_points = [
    Point(10, [[1,2,3], [3, 4, 5]], {}),
    Point(11, {
        "sparse": SparseVector(indices=[0, 2], values=[1.0, 3.0])
    }, {}),
]


# Test points conversion into internal representation and back
for point in some_other_points:
    print(f"Point: {point.id}, vector: {point.vector}, payload: {point.payload}")


print("---- Search ----")

points = shard.search(SearchRequest(
    query=[1.0, 1.0, 1.0, 1.0],
    filter=None,
    params=None,
    limit=10,
    offset=0,
    with_vector=True,
    with_payload=True,
    score_threshold=None,
))

for point in points:
    print(f"Point: {point.id}, vector: {point.vector}, payload: {point.payload}, score: {point.score}")


print("---- Search Filter ----")

search_filter = Filter(
    must=[
        FieldCondition(
            key="hello",
            match=MatchTextAny(text_any="world"),
        ),
        FieldCondition(
            key="price",
            range=RangeFloat(gte=500.0),
        )
    ]
)

points = shard.search(SearchRequest(
    query=[1.0, 1.0, 1.0, 1.0],
    filter=search_filter,
    params=None,
    limit=10,
    offset=0,
    with_vector=True,
    with_payload=True,
    score_threshold=None,
))

for point in points:
    print(f"Point: {point.id}, vector: {point.vector}, payload: {point.payload}, score: {point.score}")

print("---- Retrieve ----")

points = shard.retrieve(point_ids=[1], with_vector=True, with_payload=True)

for point in points:
    print(f"Point: {point.id}, vector: {point.vector}, payload: {point.payload}")
