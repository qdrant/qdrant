# Python bindings for qdrant segment

This is the first iteration to try to build Python bindings for the segment module of Qdrant.

The intention is to offer the ANN functionality directly from Python without needing to have a Qdrant server.

## Instructions to build.

### Prerequisites

Check how to install [rust and cargo](https://www.rust-lang.org/tools/install) 

Install maturin as tool for building the project and wheels.

```shell
pip install maturin
```

From this folder, run:

There may be some differences when running from macOS

```shell
maturin build --no-sdist
pip install target/wheels/qdrant_segment_py*cp37*
```

Then you can run:
```shell
python app.py
```

This is the content inside app.py:

```python
import numpy as np
from qdrant_segment_py import
    PyVectorIndexType,
    PyPayloadIndexType,
    PyDistanceType,
    PyStorageType,
    PySegmentConfig,
    PySegment
from jina import Document, DocumentArray

def get_random_numpy():
    return np.random.rand(100).astype('float32') # for now only accepts this type


vector_index_type = PyVectorIndexType(0)
payload_index_type = PyPayloadIndexType(0)
distance_type = PyDistanceType(0)
storage_type = PyStorageType(0)
vector_dim = 100

config = PySegmentConfig(vector_dim, vector_index_type, payload_index_type, distance_type, storage_type)
segment = PySegment('dir', config)

docs = DocumentArray([Document(id=str(i), embedding=get_random_numpy(), text=f'I am document {i}', granularity=5, weight=5) for i in range(1000)])
for i, doc in enumerate(docs):
    result = segment.index(int(doc.id), doc.embedding)
    full_doc_dict = doc.dict()
    payload = { key: full_doc_dict[key] for key in ['id', 'text', 'granularity', 'weight'] }
    segment.set_full_payload(int(doc.id), payload)

query = get_random_numpy()
ids, scores = segment.search(query, 10)

for id in ids:
    payload = segment.get_full_payload(id)
    extracted_doc = Document(payload)
    print(f' extracted_doc {extracted_doc}')
    assert extracted_doc.text == f'I am document {id}'
    segment.delete(id)

new_ids, new_scores = segment.search(query, 10)
assert set(new_ids) != ids

for id in new_ids:
    payload = segment.get_full_payload(id)
    extracted_doc = Document(payload)
    print(f' extracted_doc {extracted_doc}')
    assert extracted_doc.text == f'I am document {id}'
```

Limitations for Jina.

`PointIdType` is currently of `u64`.

# TODO
- Add tests with `python`
- Expose more advanced options from `segment` module
- Understand how to do `setuptools` to distribute
