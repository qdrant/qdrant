"""BM25 over a text index: rank points by a payload field's own text index,
with no embedding step. The index must record document lengths (`scoring=True`).
"""

import os, shutil
from pathlib import Path

from qdrant_edge import (
    EdgeShard, EdgeConfig, EdgeVectorParams, Distance,
    Bm25Params, Fusion, Point, Prefetch, Query, QueryRequest,
    TextIndexParams, TextQuery, TokenizerType, UpdateOperation,
)


DATA_DIR = Path(__file__).parent.parent.parent / "data"
TMP_DIR = DATA_DIR / "tmp"

path = TMP_DIR / "qdrant_edge_text_bm25"
shutil.rmtree(path, ignore_errors=True)
os.makedirs(path)

config = EdgeConfig(vectors={"dense": EdgeVectorParams(size=2, distance=Distance.Dot)})
shard = EdgeShard.create(str(path), config)

index = TextIndexParams(tokenizer=TokenizerType.Word, scoring=True)
assert index.scoring
shard.update(UpdateOperation.create_field_index("text", index))
shard.update(UpdateOperation.upsert_points([
    Point(1, {"dense": [1.0, 0.0]}, {"text": "the quick brown fox"}),
    Point(2, {"dense": [0.0, 1.0]}, {"text": "a lazy dog sleeps"}),
    Point(3, {"dense": [0.5, 0.5]}, {"text": "fox fox fox, a very clever fox"}),
]))

results = shard.query(QueryRequest(limit=3, query=TextQuery("text", "clever fox")))
print(f"Text query: {results}")
assert [r.id for r in results] == [3, 1]
assert results[0].score > results[1].score

# Without length normalization the long document loses its length penalty.
flat = shard.query(QueryRequest(
    limit=3, query=TextQuery("text", "fox", scoring=Bm25Params(b=0.0)),
))
print(f"Text query, b=0: {flat}")
assert [r.id for r in flat] == [3, 1]

# A text prefetch fuses with a dense one.
hybrid = shard.query(QueryRequest(
    limit=3,
    prefetches=[
        Prefetch(limit=3, query=TextQuery("text", "dog")),
        Prefetch(limit=3, query=Query.Nearest([0.0, 1.0], using="dense")),
    ],
    query=Fusion.Rrf(k=2),
))
print(f"Hybrid query: {hybrid}")
assert hybrid[0].id == 2

# A text index without `scoring` cannot rank.
shard.update(UpdateOperation.create_field_index("plain", TextIndexParams()))
try:
    shard.query(QueryRequest(limit=3, query=TextQuery("plain", "fox")))
    raise AssertionError("expected a refusal")
except Exception as err:
    assert "does not score" in str(err), err

shard.close()
