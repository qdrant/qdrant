"""Native BM25 sparse search: embed text manually with `Bm25`, then upsert and query
the produced sparse vectors through the regular update/query path.
"""

import os, shutil
from pathlib import Path

from qdrant_edge import (
    EdgeShard, EdgeConfig, EdgeSparseVectorParams, Modifier,
    Bm25, Bm25Config,
    Point, Query, QueryRequest, UpdateOperation,
)


DATA_DIR = Path(__file__).parent.parent.parent / "data"
TMP_DIR = DATA_DIR / "tmp"

path = TMP_DIR / "qdrant_edge_bm25"
shutil.rmtree(path, ignore_errors=True)
os.makedirs(path)

config = EdgeConfig(
    sparse_vectors={"text": EdgeSparseVectorParams(modifier=Modifier.Idf)},
)
shard = EdgeShard.create(str(path), config)

bm25 = Bm25(Bm25Config(language="english"))

shard.update(UpdateOperation.upsert_points([
    Point(1, {"text": bm25.embed_document("the quick brown fox")}, {"doc": "1"}),
    Point(2, {"text": bm25.embed_document("a lazy dog sleeps")},   {"doc": "2"}),
    Point(3, {"text": bm25.embed_document("foxes are clever")},    {"doc": "3"}),
]))
shard.optimize()
print(f"Info: {shard.info()}")

q = bm25.embed_query("clever fox")
results = shard.query(QueryRequest(
    limit=3, query=Query.Nearest(q, using="text"), with_payload=True,
))
print(f"Got {len(results)} results")
for r in results:
    print(f"  id={r.id} score={r.score:.4f} payload={r.payload}")
