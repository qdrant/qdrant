# Python bindings for qdrant segment

This is the first iteration to try to build Python bindings for the segment module of Qdrant.

The intention is to offer the ANN functionality directly from Python without needing to have a Qdrant server.

## Instructions to build

From this folder, run:

There may be some differences when running from macOS

```shell
cargo build --release --all-targets -j 4 --no-default-features 
```

Then you should see `../target/release/libqdrant_segment_py.so`.

(may need to change the name in macOS or Windows)
Then run:
```shell
cp ./target/release/libqdrant_segment_py.so qdrant_segment_py.so
```

Once done, we can start a python shell, and do:

```python
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
```

Limitations for Jina.

`PointIdType` is currently of `u64`.

# TODO
- Add tests with `python`
- Expose more advanced options from `segment` module
- Understand how to do `setuptools` to distribute
