"""
Qdrant Edge supports all Qdrant quantization methods: Scalar, Product, Binary, and TurboQuant. 
Configure quantization globally on EdgeConfig.quantization_config or override per-vector on EdgeVectorParams.quantization_config. 
See more: https://qdrant.tech/documentation/manage-data/quantization/
"""

import os
import shutil
from pathlib import Path

from qdrant_edge import *

DATA_DIR = Path(__file__).parent.parent.parent / "data"
TMP_DIR = DATA_DIR / "tmp"
SHARD_DIR = TMP_DIR / "turboquant_shard"
VECTOR_SIZE = 384

def make_vector(point_id: int) -> list[float]:
    return [float(point_id)] * VECTOR_SIZE

shutil.rmtree(SHARD_DIR, ignore_errors=True)
os.makedirs(SHARD_DIR)

turboquant_config = TurboQuantQuantizationConfig(
    always_ram=True,
    bits=TurboQuantBitSize.Bits4,
)

config = EdgeConfig(
    vectors=EdgeVectorParams(size=VECTOR_SIZE, distance=Distance.Cosine),
    quantization_config=turboquant_config,
)

shard = EdgeShard.create(str(SHARD_DIR), config)
shard.update(
    UpdateOperation.upsert_points(
        [Point(point_id, make_vector(point_id), {}) for point_id in range(1, 21)]
    )
)
shard.optimize()

print(f"Points: {shard.info().points_count}")