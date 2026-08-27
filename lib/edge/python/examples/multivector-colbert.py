#!/usr/bin/env python3
"""ColBERT-style late-interaction search: each document is stored as a matrix of per-token embeddings in a named multi-vector field"""

import os
import shutil
from pathlib import Path

from qdrant_edge import (
    Distance,
    EdgeConfig,
    EdgeShard,
    EdgeVectorParams,
    MultiVectorComparator,
    MultiVectorConfig,
    Point,
    Query,
    QueryRequest,
    UpdateOperation,
)


DATA_DIR = Path(__file__).parent.parent.parent / "data"
TMP_DIR = DATA_DIR / "tmp"
path = TMP_DIR / "qdrant_edge_multivector_colbert"
shutil.rmtree(path, ignore_errors=True)
os.makedirs(path)

# The "colbert" field stores a matrix of vectors per point instead of one,
# with MaxSim comparing every query row against every stored row.
config = EdgeConfig(
    vectors={
        "colbert": EdgeVectorParams(
            size=4,
            distance=Distance.Cosine,
            multivector_config=MultiVectorConfig(MultiVectorComparator.MaxSim),
        ),
    },
)
shard = EdgeShard.create(path, config)

documents = [
    (1, "mountain hiking trail guide", [
        [0.85, 0.10, 0.30, 0.05],
        [0.80, 0.15, 0.35, 0.08],
        [0.82, 0.12, 0.32, 0.10],
    ]),
    (2, "sourdough bread baking recipe", [
        [0.10, 0.85, 0.15, 0.30],
        [0.15, 0.80, 0.10, 0.35],
        [0.12, 0.88, 0.18, 0.28],
    ]),
    (3, "vintage synthesizer sound design tutorial", [
        [0.20, 0.10, 0.85, 0.15],
        [0.25, 0.08, 0.80, 0.20],
        [0.18, 0.12, 0.82, 0.18],
        [0.22, 0.10, 0.78, 0.22],
    ]),
    (4, "alpine trekking gear checklist", [
        [0.88, 0.08, 0.28, 0.10],
        [0.83, 0.12, 0.30, 0.12],
        [0.86, 0.10, 0.25, 0.08],
    ]),
]

shard.update(UpdateOperation.upsert_points([
    Point(point_id, {"colbert": token_vectors}, {"text": text})
    for point_id, text, token_vectors in documents
]))

# The query is also a matrix - one row per query token.
query_tokens = [
    [0.85, 0.10, 0.30, 0.05],
    [0.84, 0.11, 0.27, 0.09],
]

results = shard.query(QueryRequest(
    query=Query.Nearest(query_tokens, using="colbert"),
    limit=4,
    with_payload=True,
))

print("=== ColBERT-style MaxSim search ===")
for point in results:
    print(f"id={point.id} score={point.score:.4f} text={point.payload['text']}")

assert results[0].id == 1, "the hiking document should score highest for a hiking-like query"

# Round-trip check: the stored matrix keeps one row per token (rows are
# normalized on write since the field uses cosine distance).
records = shard.retrieve([1], with_payload=False, with_vector=True)
stored = records[0].vector["colbert"]
print(f"\nStored matrix for point 1 has {len(stored)} rows")
assert len(stored) == len(documents[0][2])

print("\nDone!")
