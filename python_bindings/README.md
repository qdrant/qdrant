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
import numpy as np
from qdrant_segment_py import PySegmentConfig, PySegment

config = PySegmentConfig(vector_size=100, index=0, payload_index=0, distance=0, storage_type=0)
segment = PySegment('dir', config)

def get_random_numpy():
    return np.random.rand(100).astype('float32') # for now only accepts this type
 
segment = qdrant_segment_py.get_simple_segment('dir', 100)
index_arrays = [get_random_numpy() for _ in range(1000)]
for i, array in enumerate(index_arrays):
    segment.index(i, array)
ids, scores = segment.search(get_random_numpy().astype('float32'), 10)
>>> ids
[520, 319, 194, 672, 841, 793, 594, 596, 655, 42]
>>> scores
[0.9040817618370056, 0.8993460536003113, 0.8909826874732971, 0.890875518321991, 0.860325038433075, 0.8592538237571716, 0.8529136180877686, 0.8495206832885742, 0.8460708260536194, 0.8437867164611816]
```

Limitations for Jina.

`PointIdType` is currently of `u64`.

# TODO
- Add tests with `python`
- Expose more advanced options from `segment` module
- Understand how to do `setuptools` to distribute
