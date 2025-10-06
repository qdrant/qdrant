import os
import shutil
from qdrant_edge import *

config = SegmentConfig(
    vector_data={
        "": VectorDataConfig(
            size=4,
            distance=Distance.COSINE,
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

DATA_DIRECTORY = "./data"

# Clear and recreate data directory

if os.path.exists(DATA_DIRECTORY):
    shutil.rmtree(DATA_DIRECTORY)
os.makedirs(DATA_DIRECTORY)

shard = Shard(DATA_DIRECTORY, config)

shard.update(UpdateOperation.upsert_points([
    Point(
        PointId.num(1),
        Vector.single([6.0, 9.0, 4.0, 2.0]),
        Payload({
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
        }),
    ),
]))

points = shard.search(SearchRequest(
    query=Query.nearest(QueryVector.dense([1.0, 1.0, 1.0, 1.0]), None),
    filter=None,
    params=None,
    limit=10,
    offset=0,
    with_vector=WithVector(True),
    with_payload=WithPayload(True),
    score_threshold=None,
))

for point in points:
    print(f"Point: {point.id}, vector: {point.vector}, payload: {point.payload}, score: {point.score}")

retrieve = shard.retrieve(ids=[PointId.num(1)], with_vector=WithVector(True), with_payload=WithPayload(True))

for point in retrieve:
    print(f"Point: {point.id}, vector: {point.vector}, payload: {point.payload}")
