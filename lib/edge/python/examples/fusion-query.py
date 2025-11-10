from qdrant_edge import *
from common import *



shard = load_new_shard()
fill_dummy_data(shard)


search_filter = Filter(
    must=[
        FieldCondition(
            key="color",
            match=MatchValue(value="red"),
        )
    ]
)


result = shard.query(QueryRequest(
    prefetches = [
        Prefetch(
            prefetches=[],
            query=Query.Nearest([6.0, 9.0, 4.0, 2.0]),
            limit=5,
            params=None,
            filter=None,
            score_threshold=None,
        ),
        Prefetch(
            prefetches=[],
            query=Query.Nearest([1.0, -3.0, 2.0, 8.0]),
            limit=5,
            params=None,
            filter=search_filter,
            score_threshold=None,
        )
    ],
    query = Fusion.rrfk(2),
    filter = None,
    score_threshold = None,
    limit = 10,
    offset = 0,
    params = None,
    with_vector = True,
    with_payload = True,
))


for point in result:
    print(f"Point: {point.id}, vector: {point.vector}, payload: {point.payload}, score: {point.score}")



