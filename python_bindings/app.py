import numpy as np
from qdrant_segment_py import \
    PyVectorIndexType, \
    PyPayloadIndexType, \
    PyDistanceType, \
    PyStorageType, \
    PySegmentConfig, \
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
    payload = {key: full_doc_dict[key] for key in ['id', 'text', 'granularity', 'weight']}
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
