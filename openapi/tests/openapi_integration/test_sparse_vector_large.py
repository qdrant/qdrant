import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

collection_name = 'test_large_sparse_vector'


@pytest.fixture(autouse=True)
def setup():
    sparse_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def sparse_collection_setup(collection_name='test_collection'):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "sparse_vectors": {
                "text": {}
            },
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok


def test_sparse_vector_large():
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {
                        "text": {
                            "indices": [808342246, 3331306528, 542569676, 913951781, 1436743712, 2336501044, 2690574963, 1871711020, 1555611613, 869159183, 1739377735, 1251323507, 1779632210, 3444800112, 3174105482, 2305639372, 1076060679, 4025475087, 4146668087, 2239416842, 1454622239, 2826065600, 1826244548],
                            "values": [0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632, 0.5249451281713632],
                        }
                    }
                },
            ]
        }
    )
    assert response.ok
