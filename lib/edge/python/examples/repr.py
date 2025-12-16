#!/usr/bin/env python3

from qdrant_edge import *

config = SegmentConfig(
    vector_data = {
        "": VectorDataConfig(
            size = 128,
            distance = Distance.Cosine,
            storage_type = VectorStorageType.ChunkedMmap,
            index = HnswIndexConfig(
                m = 16,
                ef_construct = 16,
                full_scan_threshold = 1024,
                on_disk = False,
                payload_m = 16,
                inline_storage = False,
            ),
        )
    },
    sparse_vector_data = {
        "sparse": SparseVectorDataConfig(
            index = SparseIndexConfig(
                full_scan_threshold = 1024,
                index_type = SparseIndexType.MutableRam,
                datatype = VectorStorageDatatype.Float32,
            ),
            storage_type = SparseVectorStorageType.Mmap,
            modifier = Modifier.Idf,
        )
    },
    payload_storage_type = PayloadStorageType.Mmap,
)

print(config)
