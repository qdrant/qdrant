#!/usr/bin/env python3

from qdrant_edge import (
    Distance,
    EdgeConfig,
    EdgeSparseVectorParams,
    EdgeVectorParams,
    Memory,
    Modifier,
    VectorStorageDatatype,
)

config = EdgeConfig(
    vectors=EdgeVectorParams(
        size=128,
        distance=Distance.Cosine,
        memory=Memory.Cold,
    ),
    sparse_vectors={
        "sparse": EdgeSparseVectorParams(
            full_scan_threshold=1024,
            datatype=VectorStorageDatatype.Float32,
            modifier=Modifier.Idf,
            memory=Memory.Pinned,
        ),
    },
    payload_memory=Memory.Cold,
    id_tracker_memory=Memory.Pinned,
)

print(config)
