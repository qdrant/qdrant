#!/usr/bin/env python3
# See lib/edge/publish/examples/src/bin/mmr-query.rs for the equivalent Rust example.

from qdrant_edge import *
from common import *


shard = load_new_shard()
fill_dummy_data(shard)

result = shard.query(QueryRequest(
    prefetches = [],
    query = Mmr([6.0, 9.0, 4.0, 2.0], 0.9, candidates_limit=100),
    filter = None,
    limit = 10,
    offset = 0,
    with_vector = True,
    with_payload = True,
))

for point in result:
    print(point)
