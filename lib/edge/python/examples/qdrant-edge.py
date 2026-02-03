#!/usr/bin/env python3

from common import *

from qdrant_edge import *

print("---- Point conversions ----")

points = [
    Point(10, [[1, 2, 3], [3, 4, 5]], {}),
    Point(11, {"sparse": SparseVector(indices=[0, 2], values=[1.0, 3.0])}, {}),
]

# Test points conversion into internal representation and back
for point in points:
    print(point)


print("---- Load shard ----")

shard = load_new_shard()


print("---- Upsert ----")

shard.update(
    UpdateOperation.upsert_points(
        [
            Point(
                1,
                [6.0, 9.0, 4.0, 2.0],
                {
                    "null": None,
                    "str": "string",
                    "uint": 42,
                    "int": -69,
                    "float": 4.20,
                    "bool": True,
                    "obj": {
                        "null": None,
                        "str": "string",
                        "uint": 42,
                        "int": -69,
                        "float": 4.20,
                        "bool": True,
                        "obj": {},
                        "arr": [],
                    },
                    "arr": [None, "string", 42, -69, 4.20, True, {}, []],
                },
            ),
            Point(
                "e9408f2b-b917-4af1-ab75-d97ac6b2c047",
                [6.0, 9.0, 3.0, -2.0],
                {
                    "hello": "world",
                    "price": 199.99,
                },
            ),
            Point(
                uuid.uuid4(),
                [1.0, 6.0, 4.0, 2.0],
                {
                    "hello": "world",
                    "price": 999.99,
                },
            ),
        ]
    )
)


print("---- Query ----")

result = shard.query(
    QueryRequest(
        query=Query.Nearest([6.0, 9.0, 4.0, 2.0]),
        limit=10,
        with_vector=True,
        with_payload=True,
    )
)

for point in result:
    print(point)


print("---- Search ----")

points = shard.search(
    SearchRequest(
        query=Query.Nearest([1.0, 1.0, 1.0, 1.0]),
        limit=10,
        with_vector=True,
        with_payload=True,
    )
)

for point in points:
    print(point)


print("---- Search + Filter ----")

search_filter = Filter(
    must=[
        FieldCondition(
            key="hello",
            match=MatchTextAny(text_any="world"),
        ),
        FieldCondition(
            key="price",
            range=RangeFloat(gte=500.0),
        ),
    ]
)

points = shard.search(
    SearchRequest(
        query=Query.Nearest([1.0, 1.0, 1.0, 1.0]),
        filter=search_filter,
        limit=10,
        with_vector=True,
        with_payload=True,
    )
)

for point in points:
    print(point)


print("---- Retrieve ----")

points = shard.retrieve(point_ids=[1], with_vector=True, with_payload=True)

for point in points:
    print(point)

print("---- Scroll ----")

scroll_result, next_offset = shard.scroll(ScrollRequest(limit=2))
for point in scroll_result:
    print(point)

while next_offset is not None:
    print(f"--- Next scroll (offset = {next_offset})---")
    scroll_result, next_offset = shard.scroll(
        ScrollRequest(limit=2, offset=next_offset)
    )
    for point in scroll_result:
        print(point)

print("---- Count ----")

count = shard.count(CountRequest(exact=True))

print(f"Total points count: {count}")

print("---- Facet (requires payload index) ----")

# Note: Facet requires a payload index on the field being faceted.
# In Edge, payload indexes cannot be created directly - you need to:
# 1. Create the index in a full Qdrant instance
# 2. Create a shard snapshot
# 3. Load the snapshot into Edge
try:
    response = shard.facet(
        FacetRequest(
            key="hello",
            limit=10,
            exact=False,
        )
    )
    print(f"Facet results ({len(response)} hits):")
    for hit in response:
        print(f"  {hit.value}: {hit.count}")
except Exception as e:
    # Expected error when no payload index exists
    print(f"Facet error (expected without payload index): {e}")

print("---- info ----")

info = shard.info()
print(info)

print("---- Close and reopen shard ----")

shard.close()

reopened_shard = EdgeShard(DATA_DIRECTORY)

info = reopened_shard.info()

print(info)
