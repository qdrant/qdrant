# Common helper items for the examples.
# See lib/edge/publish/examples/src/lib.rs for the equivalent Rust helpers.

import os
import shutil
import uuid
from pathlib import Path

from qdrant_edge import (
    Distance,
    EdgeConfig,
    EdgeShard,
    EdgeVectorParams,
    Point,
    UpdateOperation,
)

DATA_DIR = Path(__file__).parent.parent.parent / "data"
TMP_DIR = DATA_DIR / "tmp"


def load_new_shard():
    """Create a new edge shard (clears DATA_DIRECTORY first)."""
    print("---- Load shard ----")

    # Clear and recreate tmp directory
    if os.path.exists(TMP_DIR):
        shutil.rmtree(TMP_DIR)

    os.makedirs(TMP_DIR)

    config = EdgeConfig(
        vectors=EdgeVectorParams(size=4, distance=Distance.Dot),
    )

    return EdgeShard.create(TMP_DIR, config)


def fill_dummy_data(shard: EdgeShard):
    shard.update(
        UpdateOperation.upsert_points(
            [
                Point(
                    1,
                    [0.05, 0.61, 0.76, 0.74],
                    {"color": "red", "city": ["Moscow", "Berlin"]},
                ),
                Point(2, [0.19, 0.81, 0.75, 0.11], {"color": "red", "city": "Mexico"}),
                Point(
                    3,
                    [0.36, 0.55, 0.47, 0.94],
                    {"color": "blue", "city": ["Berlin", "Barcelona"]},
                ),
                Point(
                    4,
                    [0.12, 0.34, 0.56, 0.78],
                    {"color": "green", "city": "Lisbon", "rating": 4.5},
                ),
                Point(
                    5,
                    [0.88, 0.12, 0.33, 0.44],
                    {"color": "yellow", "city": ["Paris"], "active": True},
                ),
                Point(
                    6,
                    [0.21, 0.22, 0.23, 0.24],
                    {"color": "blue", "city": "Tokyo", "tags": ["night", "food"]},
                ),
                Point(
                    7,
                    [0.99, 0.01, 0.50, 0.50],
                    {"color": "red", "city": ["New York", "Boston"], "visits": 7},
                ),
                Point(
                    8,
                    [0.10, 0.20, 0.30, 0.40],
                    {"color": "blue", "city": "Seoul", "meta": {"source": "import"}},
                ),
                Point(
                    9,
                    [0.45, 0.55, 0.65, 0.75],
                    {"color": "green", "city": ["Berlin"], "score": 0.92},
                ),
                Point(
                    10,
                    [0.01, 0.02, 0.03, 0.04],
                    {"color": "yellow", "city": None, "featured": False},
                ),
            ]
        )
    )
