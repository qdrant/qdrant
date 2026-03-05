#!/usr/bin/env python3

from qdrant_edge import *

config = EdgeConfig(
    vector_data = VectorDataConfig(
        size = 128,
        distance = Distance.Cosine,
    ),
    sparse_vector_data = {
        "sparse": SparseVectorDataConfig(
            index = SparseIndexConfig(
                full_scan_threshold = 1024,
                datatype = VectorStorageDatatype.Float32,
            ),
            modifier = Modifier.Idf,
        )
    },
)

print(config)
