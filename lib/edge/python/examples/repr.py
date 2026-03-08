#!/usr/bin/env python3

from qdrant_edge import (
    Distance,
    EdgeConfig,
    EdgeSparseVectorParams,
    EdgeVectorParams,
    Modifier,
    VectorStorageDatatype,
)

config = EdgeConfig(
    vectors=EdgeVectorParams(
        size=128,
        distance=Distance.Cosine,
    ),
    sparse_vectors={
        "sparse": EdgeSparseVectorParams(
            full_scan_threshold=1024,
            datatype=VectorStorageDatatype.Float32,
            modifier=Modifier.Idf,
        ),
    },
)

print(config)
