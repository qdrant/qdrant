#!/usr/bin/env python3
"""Hybrid dense + BM25 sparse search with DBSF fusion."""

import os
import shutil
from pathlib import Path

from qdrant_edge import (
    Bm25, Bm25Config, Distance,
    EdgeConfig, EdgeShard, EdgeSparseVectorParams,
    EdgeVectorParams, Fusion, Modifier, Point,
    Prefetch, Query, QueryRequest, UpdateOperation,
)


DATA_DIR = Path(__file__).parent.parent.parent / "data"
TMP_DIR = DATA_DIR / "tmp"
path = TMP_DIR / "qdrant_edge_hybrid_search_dbsf"
shutil.rmtree(path, ignore_errors=True)
os.makedirs(path)

config = EdgeConfig(
    vectors={"dense": EdgeVectorParams(size=4, distance=Distance.Cosine)},
    sparse_vectors={"sparse": EdgeSparseVectorParams(modifier=Modifier.Idf)},
)
shard = EdgeShard.create(path, config)

bm25 = Bm25(Bm25Config(language="english"))

documents = [
    (1, "red apple fresh fruit", [0.90, 0.10, 0.10, 0.05]),
    (2, "green apple tart fruit", [0.85, 0.15, 0.12, 0.10]),
    (3, "fast red sports car", [0.10, 0.90, 0.15, 0.05]),
    (4, "electric vehicle charging", [0.12, 0.80, 0.20, 0.20]),
    (5, "fresh fruit market", [0.88, 0.12, 0.18, 0.05]),
]



shard.update(UpdateOperation.upsert_points([
    Point(
        point_id,
        {
            "dense": dense_vector,
            "sparse": bm25.embed_document(text),
        },
        {"text": text},
    )
    for point_id, text, dense_vector in documents
]))
shard.optimize()

dense_query = [0.90, 0.10, 0.10, 0.05]
sparse_query = bm25.embed_query("fresh apple fruit")

results = shard.query(QueryRequest(
    prefetches=[
        Prefetch(
            query=Query.Nearest(dense_query, using="dense"),
            limit=3,
        ),
        Prefetch(
            query=Query.Nearest(sparse_query, using="sparse"),
            limit=3,
        ),
    ],
    query=Fusion.Dbsf(),
    limit=3,
    with_payload=True,
))

print("=== Hybrid Search with DBSF ===")
for point in results:
    print(f"id={point.id} score={point.score:.4f} text={point.payload['text']}")
