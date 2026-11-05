"""Match a payload field by string, integer and boolean values through MatchValue."""

from tempfile import TemporaryDirectory

from qdrant_edge import (
    Distance,
    EdgeConfig,
    EdgeShard,
    EdgeVectorParams,
    FieldCondition,
    Filter,
    MatchValue,
    Point,
    ScrollRequest,
    UpdateOperation,
)

# `bool` is a subclass of `int` in Python: `True` must stay a boolean, not become `1`.
assert MatchValue(value=True).value is True
assert MatchValue(value=False).value is False
assert MatchValue(value=1).value == 1 and MatchValue(value=1).value is not True
assert MatchValue(value="a").value == "a"

with TemporaryDirectory() as path:
    shard = EdgeShard.create(
        path, EdgeConfig(vectors=EdgeVectorParams(size=2, distance=Distance.Dot))
    )
    shard.update(
        UpdateOperation.upsert_points(
            [
                Point(1, [1.0, 0.0], {"flag": True, "count": 1, "name": "a"}),
                Point(2, [0.0, 1.0], {"flag": False, "count": 2, "name": "b"}),
            ]
        )
    )

    def matching_ids(key, value):
        request = ScrollRequest(
            filter=Filter(must=[FieldCondition(key=key, match=MatchValue(value=value))])
        )
        points, _ = shard.scroll(request)
        return sorted(point.id for point in points)

    assert matching_ids("flag", True) == [1]
    assert matching_ids("flag", False) == [2]
    assert matching_ids("count", 1) == [1]
    assert matching_ids("name", "b") == [2]
    shard.close()

print("MatchValue: string, integer and boolean values match their payload type")
