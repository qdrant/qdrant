#!/usr/bin/env python3
# Upsert and search with NumPy arrays, without converting them to Python lists.

import numpy as np
from common import *

from qdrant_edge import *

shard = load_new_shard()

rng = np.random.default_rng(42)
vectors = rng.standard_normal((100, 4)).astype(np.float32)


print("---- Upsert batch ----")

# One call per batch: ids, a 2-D array with one row per id, and a payload per id
shard.update(
    UpdateOperation.upsert_batch(
        ids=list(range(100)),
        vectors=vectors,
        payloads=[{"even": i % 2 == 0} for i in range(100)],
    )
)
assert shard.info().points_count == 100

# Stored vectors are exactly the array rows
(record,) = shard.retrieve(point_ids=[7], with_payload=True, with_vector=True)
assert record.vector == vectors[7].tolist(), record.vector
assert record.payload == {"even": False}, record.payload


print("---- Search with a NumPy query ----")

query = vectors[7]
result = shard.query(QueryRequest(query=Query.Nearest(query), limit=3, with_payload=True))
expected = np.argsort(-(vectors @ query))[:3].tolist()
assert [p.id for p in result] == expected, (result, expected)
print(result)


print("---- Points and other dtypes ----")

# float64 arrays are converted, `Point` accepts arrays too
shard.update(UpdateOperation.upsert_points([Point(100, np.ones(4), {"even": True})]))
(record,) = shard.retrieve(point_ids=[100], with_payload=False, with_vector=True)
assert record.vector == [1.0, 1.0, 1.0, 1.0], record.vector

# Lists still work, and payloads are optional
shard.update(UpdateOperation.upsert_batch(ids=[101, 102], vectors=[[0.1] * 4, [0.2] * 4]))
assert shard.info().points_count == 103


print("---- Length mismatch ----")

for kwargs in [
    dict(ids=[1, 2, 3], vectors=vectors[:2]),
    dict(ids=[1, 2], vectors=vectors[:2], payloads=[{}]),
]:
    try:
        UpdateOperation.upsert_batch(**kwargs)
    except ValueError as err:
        print(err)
    else:
        raise AssertionError(f"no error for {kwargs}")

shard.close()
