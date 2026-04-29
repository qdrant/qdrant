"""End-to-end BM25 inference: upsert by document, manual embed for queries.

Registers a BM25 model under "qdrant/bm25" in the shard config; the shard
then embeds `Document(text=..., model="qdrant/bm25")` inputs automatically
on `shard.upsert(...)`. For queries we embed manually via `embed_query()`
because the query path doesn't yet accept documents inline.
"""

import os, shutil
from pathlib import Path

from qdrant_edge import (
    EdgeShard, EdgeConfig, EdgeSparseVectorParams, Modifier,
    Bm25Config, Document, EdgePoint,
    Query, QueryRequest,
)


DATA_DIR = Path(__file__).parent.parent.parent / "data"
TMP_DIR = DATA_DIR / "tmp"

path = TMP_DIR / "qdrant_edge_bm25_document"
shutil.rmtree(path, ignore_errors=True)
os.makedirs(path)

config = EdgeConfig(
    sparse_vectors={"text": EdgeSparseVectorParams(modifier=Modifier.Idf)},
    inference_models={"qdrant/bm25": Bm25Config(language="english")},
)
shard = EdgeShard.create(path, config)

# Upsert by document — embedding happens inside the shard.
shard.upsert([
    EdgePoint(1, {"text": Document("the quick brown fox", "qdrant/bm25")}, {"doc": "1"}),
    EdgePoint(2, {"text": Document("a lazy dog sleeps", "qdrant/bm25")},   {"doc": "2"}),
    EdgePoint(3, {"text": Document("foxes are clever",   "qdrant/bm25")},  {"doc": "3"}),
])
shard.optimize()
print(f"Info: {shard.info()}")

# Query path: embed manually (query mode), then plug into a regular query.
q = shard.embed_query(Document("clever fox", "qdrant/bm25"))
results = shard.query(QueryRequest(
    limit=3, query=Query.Nearest(q, using="text"), with_payload=True,
))
print(f"Got {len(results)} results")
for r in results:
    print(f"  id={r.id} score={r.score:.4f} payload={r.payload}")
