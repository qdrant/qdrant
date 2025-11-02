from qdrant_edge import *
from common import *


shard = load_new_shard()
fill_dummy_data(shard)



result = shard.query(QueryRequest(
    prefetches = [],
    query = Mmr([6.0, 9.0, 4.0, 2.0], None, 0.9, 100),
    filter = None,
    score_threshold = None,
    limit = 10,
    offset = 0,
    params = None,
    with_vector = True,
    with_payload = True,
))


for point in result:
    print(f"Point: {point.id}, vector: {point.vector}, payload: {point.payload}, score: {point.score}")



