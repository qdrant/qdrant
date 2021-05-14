# Python bindings for qdrant segment

This is the first iteration to try to build Python bindings for the segment module of Qdrant.

The intention is to offer the ANN functionality directly from Python without needing to have a Qdrant server.

## Instructions to build

From this folder, run:

There may be some differences when running from macOS

```shell
cargo build --release --all-targets -j 4 
```

Then you should see `../target/release/libqdrant_segment_py.so`.

(may need to change the name in macOS or Windows)
Then run:
```shell
cp ./target/release/libqdrant_segment_py.so qdrant_segment_py.so
```

Once done, we can start a python shell, and do:

```python
import qdrant_segment_py # notice this module matches the `.so` name
import numpy as np

def get_random_numpy():
    return np.random.rand(100).astype('float32') # for now only accepts this type
 
segment = qdrant_segment_py.get_simple_segment('dir', 100)
index_arrays = [get_random_numpy() for _ in range(1000)]
for i, array in enumerate(index_arrays):
    qdrant_segment_py.index(segment, i, array)
ids, scores = qdrant_segment_py.search(segment, get_random_numpy().astype('float32'), 10)
>>> ids
[789, 82, 162, 168, 151, 575, 164, 673, 977, 60]
>>> scores
[26.745899200439453, 26.652542114257812, 26.365930557250977, 26.1746826171875, 26.01166343688965, 25.981624603271484, 25.94896697998047, 25.8475399017334, 25.7318172454834, 25.70850944519043]
```

Limitations for Jina.

`PointIdType` is currently of `u64`.

# TODO
- Add tests with `python`
- Expose more advanced options from `segment` module
- Understand how to do `setuptools` to distribute
