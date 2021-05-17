from qdrant_segment_py import PyVectorIndexType, PyPayloadIndexType, PyDistanceType, PyStorageType, PySegmentConfig, PySegment
import numpy as np

def get_random_numpy():
    return np.random.rand(100).astype('float32') # for now only accepts this type


vector_index_type = PyVectorIndexType(0)
payload_index_type = PyPayloadIndexType(0)
distance_type = PyDistanceType(0)
storage_type = PyStorageType(0)
vector_dim = 100

config = PySegmentConfig(vector_dim, vector_index_type, payload_index_type, distance_type, storage_type)
segment = PySegment('dir', config)

index_arrays = [get_random_numpy() for _ in range(1000)]
for i, array in enumerate(index_arrays):
   result = segment.index(i, array)

ids, scores = segment.search(get_random_numpy().astype('float32'), 10)
print(f' ids {ids}')
print(f' scores {scores}')
