"""Execute a batch through QueryBatchRequest and compare with individual queries."""

from tempfile import TemporaryDirectory

from qdrant_edge import (
    Distance,
    EdgeConfig,
    EdgeShard,
    EdgeVectorParams,
    Point,
    Query,
    QueryBatchRequest,
    QueryRequest,
    UpdateOperation,
)


with TemporaryDirectory() as path:
    shard = EdgeShard.create(
        path, EdgeConfig(vectors=EdgeVectorParams(size=2, distance=Distance.Dot))
    )
    shard.update(
        UpdateOperation.upsert_points(
            [Point(1, [1.0, 0.0]), Point(2, [0.0, 1.0]), Point(3, [0.5, 0.5])]
        )
    )
    request = QueryBatchRequest(
        queries=[
            QueryRequest(limit=1, query=Query.Nearest([1.0, 0.0])),
            QueryRequest(limit=2, query=Query.Nearest([0.0, 1.0])),
        ]
    )
    batches = shard.query_batch(request)
    assert len(batches) == len(request.queries)
    for query, actual in zip(request.queries, batches):
        expected = shard.query(query)
        assert [(p.id, p.score) for p in actual] == [
            (p.id, p.score) for p in expected
        ]
    assert shard.query_batch(QueryBatchRequest(queries=[])) == []
    assert repr(request).startswith("QueryBatchRequest(queries=")
    shard.close()

print("QueryBatchRequest: batched results match individual queries")
