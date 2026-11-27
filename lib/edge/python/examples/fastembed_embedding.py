#!/usr/bin/env python3
"""On-device text search: embed documents with FastEmbed, index in Qdrant Edge, query."""

import os
import shutil
from pathlib import Path

from fastembed import TextEmbedding
from qdrant_edge import (
    Distance,
    EdgeConfig,
    EdgeShard,
    EdgeVectorParams,
    Point,
    Query,
    QueryRequest,
    UpdateOperation,
)

MODEL_DIM = 512
MODEL_NAME = "Qdrant/clip-ViT-B-32-text"

path = Path(__file__).parent / "qdrant_edge_fastembed_text"
shutil.rmtree(path, ignore_errors=True)
os.makedirs(path)

embedder = TextEmbedding(model_name=MODEL_NAME)
shard = EdgeShard.create(
    path,
    EdgeConfig(vectors=EdgeVectorParams(size=MODEL_DIM, distance=Distance.Cosine)),
)

documents = [
    (1, "The bakery opens at dawn and sells warm sourdough loaves."),
    (2, "A golden retriever chased a tennis ball across the park."),
    (3, "Python decorators wrap functions to add logging or caching."),
    (4, "Heavy rain flooded the streets and delayed the morning commute."),
    (5, "She booked a window seat for the overnight train to Vienna along with her dog."),
]

shard.update(UpdateOperation.upsert_points([
    Point(
        point_id,
        list(embedder.embed([text]))[0].tolist(),
        {"text": text},
    )
    for point_id, text in documents
]))
shard.optimize()

query = "dog playing fetch in the park"
query_vector = list(embedder.embed([query]))[0].tolist()
results = shard.query(QueryRequest(
    query=Query.Nearest(query_vector),
    limit=2,
    with_payload=True,
))

print(f"Relevant search results for {query}:")
for point in results:
    print(f"- {point.payload['text']}")