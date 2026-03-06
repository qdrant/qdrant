#!/usr/bin/env python3

from qdrant_edge import *

config = SegmentConfig(
    vector_data = VectorDataConfig(
        size = 128,
        distance = Distance.Cosine,
        index = HnswIndexConfig(
            m = 16,
            ef_construct = 16,
            full_scan_threshold = 1024,
            on_disk = False,
            payload_m = 16,
            inline_storage = False,
        ),
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
