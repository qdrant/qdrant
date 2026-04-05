#!/usr/bin/env python3
"""
Example: Adding a named vector to an existing Edge shard.

Demonstrates the workflow for migrating to a new embedding model
or adding hybrid search by creating a new named vector after the
shard is already populated with data.
"""

import os, shutil
from pathlib import Path

from qdrant_edge import (
    Distance,
    EdgeConfig,
    EdgeShard,
    EdgeVectorParams,
    Modifier,
    Point,
    Query,
    QueryRequest,
    SparseVector,
    UpdateOperation,
    VectorStorageDatatype,
)


DATA_DIR = Path(__file__).parent.parent.parent / "data"
TMP_DIR = DATA_DIR / "tmp"
path = TMP_DIR / "qdrant_edge_add_vector"
shutil.rmtree(path, ignore_errors=True)
os.makedirs(path)


# ------------------------------------------------------------------
# 1. Create shard with a single dense vector and populate it
# ------------------------------------------------------------------
print("---- Create shard with initial vector ----")

config = EdgeConfig(
    vectors=EdgeVectorParams(size=4, distance=Distance.Cosine),
)
shard = EdgeShard.create(path, config)

shard.update(UpdateOperation.upsert_points([
    Point(1, [0.1, 0.2, 0.3, 0.4], {"text": "first document"}),
    Point(2, [0.5, 0.6, 0.7, 0.8], {"text": "second document"}),
    Point(3, [0.9, 0.1, 0.2, 0.3], {"text": "third document"}),
]))

print(f"Points after initial insert: {shard.info().points_count}")

# Verify search works on the default vector
results = shard.query(QueryRequest(
    query=Query.Nearest([0.1, 0.2, 0.3, 0.4]),
    limit=3,
    with_payload=True,
))
print(f"Search on default vector: {len(results)} results")
assert len(results) == 3


# ------------------------------------------------------------------
# 2. Add a new dense vector (e.g., a new embedding model)
# ------------------------------------------------------------------
print("\n---- Add new dense vector 'v2' ----")

shard.update(UpdateOperation.create_dense_vector(
    vector_name="v2",
    size=8,
    distance=Distance.Dot,
))

# Existing points don't have 'v2' yet - insert new points with both vectors
shard.update(UpdateOperation.upsert_points([
    Point(4, {"": [0.4, 0.3, 0.2, 0.1], "v2": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]}, {"text": "fourth document"}),
    Point(5, {"": [0.8, 0.7, 0.6, 0.5], "v2": [8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]}, {"text": "fifth document"}),
]))

print(f"Points after adding v2: {shard.info().points_count}")

# Search on the new vector - only points 4 and 5 have 'v2'
results = shard.query(QueryRequest(
    query=Query.Nearest([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], using="v2"),
    limit=5,
    with_payload=True,
))
print(f"Search on 'v2': {len(results)} results")
assert len(results) >= 1, "Should find at least point 4 or 5"


# ------------------------------------------------------------------
# 3. Add a sparse vector (e.g., for hybrid search with BM25/SPLADE)
# ------------------------------------------------------------------
print("\n---- Add sparse vector 'keywords' ----")

shard.update(UpdateOperation.create_sparse_vector(
    vector_name="keywords",
    modifier=Modifier.Idf,
))

# Insert a point with sparse keyword data
shard.update(UpdateOperation.upsert_points([
    Point(6, {
        "": [0.3, 0.3, 0.3, 0.3],
        "keywords": SparseVector(indices=[10, 25, 42], values=[0.8, 0.5, 0.3]),
    }, {"text": "sixth document with keywords"}),
]))

print(f"Points after adding keywords: {shard.info().points_count}")

# Search on sparse vector
results = shard.query(QueryRequest(
    query=Query.Nearest(SparseVector(indices=[10, 25], values=[1.0, 0.5]), using="keywords"),
    limit=5,
    with_payload=True,
))
print(f"Search on 'keywords': {len(results)} results")
assert len(results) >= 1


# ------------------------------------------------------------------
# 4. Delete a named vector
# ------------------------------------------------------------------
print("\n---- Delete vector 'v2' ----")

shard.update(UpdateOperation.delete_vector_name("v2"))

print(f"Points after deleting v2: {shard.info().points_count}")

# Search on the default vector still works
results = shard.query(QueryRequest(
    query=Query.Nearest([0.1, 0.2, 0.3, 0.4]),
    limit=5,
    with_payload=True,
))
print(f"Search on default vector after deleting v2: {len(results)} results")
assert len(results) >= 3


# ------------------------------------------------------------------
# 5. Verify persistence - close and reopen
# ------------------------------------------------------------------
print("\n---- Close and reopen ----")

shard.close()
shard = EdgeShard.load(path)

info = shard.info()
print(f"Reopened shard: {info.points_count} points")

results = shard.query(QueryRequest(
    query=Query.Nearest(SparseVector(indices=[10, 25], values=[1.0, 0.5]), using="keywords"),
    limit=5,
    with_payload=True,
))
print(f"Search on 'keywords' after reopen: {len(results)} results")
assert len(results) >= 1

print("\nDone!")
