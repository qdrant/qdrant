"""Per-element payload indexes: index every object of an array as its own entry, so a
`nested` filter is answered from the indexes, matching conditions on the same object.
"""

import os, shutil
from pathlib import Path

from qdrant_edge import (
    CountRequest, Distance, EdgeConfig, EdgeShard, EdgeVectorParams, FieldCondition, Filter,
    IntegerIndexParams, KeywordIndexParams, MatchValue, NestedCondition, Point, RangeFloat,
    UpdateOperation,
)


DATA_DIR = Path(__file__).parent.parent.parent / "data"
TMP_DIR = DATA_DIR / "tmp"

path = TMP_DIR / "qdrant_edge_nested_index"
shutil.rmtree(path, ignore_errors=True)
os.makedirs(path)

config = EdgeConfig(vectors={"dense": EdgeVectorParams(size=2, distance=Distance.Dot)})
shard = EdgeShard.create(str(path), config)

shard.update(UpdateOperation.upsert_points([
    Point(1, {"dense": [1.0, 0.0]}, {"offers": [{"shop": "a", "price": 10}]}),
    # Shop "a" and a price under 20, but in different offers
    Point(2, {"dense": [0.0, 1.0]}, {"offers": [
        {"shop": "a", "price": 30},
        {"shop": "b", "price": 5},
    ]}),
]))

# `nested` names the array: "offers" and "offers[]" are the same
shard.update(UpdateOperation.create_field_index(
    "offers[].shop", KeywordIndexParams(nested="offers"),
))
shard.update(UpdateOperation.create_field_index(
    "offers[].price", IntegerIndexParams(nested="offers[]"),
))

info = shard.info()
for key in ["offers[].shop", "offers[].price"]:
    params = info.payload_schema[key].params
    print(f"{key}: nested={params.nested}")
    assert params.nested == "offers[]"

# An offer of shop "a" under 20: both conditions on the same offer
cheap_offer_of_a = Filter(must=[NestedCondition(
    key="offers",
    filter=Filter(must=[
        FieldCondition(key="shop", match=MatchValue(value="a")),
        FieldCondition(key="price", range=RangeFloat(lt=20.0)),
    ]),
)])
count = shard.count(CountRequest(exact=True, filter=cheap_offer_of_a))
print(f"Points with a cheap offer of shop a: {count}")
assert count == 1

# The indexed field must be inside the objects of the array
try:
    shard.update(UpdateOperation.create_field_index("title", KeywordIndexParams(nested="offers")))
    raise AssertionError("a field outside the array must be rejected")
except Exception as error:
    print(f"Rejected: {error}")

shard.close()
