from qdrant_edge import *

config = SegmentConfig(
    vector_data = {
        "": VectorDataConfig(
            size = 4,
            distance = Distance.COSINE,
            storage_type = VectorStorageType.CHUNKED_MMAP,
            index = Indexes.PLAIN,
            quantization_config = None,
            multivector_config = None,
            datatype = None,
        ),
    },
    sparse_vector_data = {},
    payload_storage_type = PayloadStorageType.IN_RAM_MMAP,
)

shard = Shard(".", config)

shard.update(UpdateOperation.upsert_points([
    Point(PointId.num(1), Vector.single([6.0, 9.0, 4.0, 2.0]), None),
]))

points = shard.search(SearchRequest(
    query = Query.nearest(QueryVector.dense([1.0, 1.0, 1.0, 1.0]), None),
    filter = None,
    params = None,
    limit = 10,
    offset = 0,
    with_vector = WithVector(True),
    with_payload = WithPayload(True),
    score_threshold = None,
))

print(points[0].vector)
