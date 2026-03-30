import os, shutil
from pathlib import Path

from qdrant_edge import (
    EdgeShard, EdgeConfig, EdgeVectorParams, EdgeSparseVectorParams,
    Distance, Modifier, Query, QueryRequest,
    Point, SparseVector, UpdateOperation,
)


DATA_DIR = Path(__file__).parent.parent.parent / "data"
TMP_DIR = DATA_DIR / "tmp"

path = TMP_DIR / "qdrant_edge_sparse_bug"
shutil.rmtree(path, ignore_errors=True)
os.makedirs(path)

config = EdgeConfig(
    vectors=EdgeVectorParams(size=4, distance=Distance.Cosine),
    sparse_vectors={"sparse": EdgeSparseVectorParams(modifier=Modifier.Idf)},
)
shard = EdgeShard.create(path, config)

shard.update(UpdateOperation.upsert_points([
    Point(1, {"": [0.5, 0.5, 0.3, 0.1], "sparse": SparseVector(indices=[1, 2], values=[1.0, 0.5])}, {"text": "doc 1"}),
    Point(2, {"": [0.1, 0.9, 0.3, 0.1], "sparse": SparseVector(indices=[2, 3], values=[1.0, 0.5])}, {"text": "doc 2"}),
    Point(3, {"": [0.9, 0.1, 0.3, 0.1], "sparse": SparseVector(indices=[1, 3], values=[1.0, 0.5])}, {"text": "doc 3"}),
]))
shard.optimize()
print(f"Info: {shard.info()}")  # Shows indexed_vectors_count=3

# Dense search - works fine
r = shard.query(QueryRequest(limit=3, query=Query.Nearest([0.5, 0.5, 0.3, 0.1]), with_payload=True))
print(f"Dense: {len(r)} results")  # OK: 3 results

# Sparse search - panics
sv = SparseVector(indices=[1, 2], values=[1.0, 0.5])
r = shard.query(QueryRequest(limit=3, query=Query.Nearest(sv, using="sparse"), with_payload=True))
print(f"Sparse: {len(r)} results")