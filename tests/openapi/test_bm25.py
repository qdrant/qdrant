import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(autouse=True)
def setup(collection_name):
    bm25_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


# Sentences from public domain sci-fi books
TEXTS = [
    # Meaningful texts
    "I must not fear. Fear is the mind-killer.",
    "All animals are equal, but some animals are more equal than others.",
    "It was a pleasure to burn.",
    "The sky above the port was the color of television, tuned to a dead channel.",
    "In the beginning, the universe was created."
    " This has made a lot of people very angry and been widely regarded as a bad move.",
    "It's a truth universally acknowledged that a zombie in possession of brains must be in want of more brains.",
    "War is peace. Freedom is slavery. Ignorance is strength.",
    "We're not in Infinity; we're in the suburbs.",
    "I was a thousand times more evil than thou!",
    "History is merely a list of surprises... It can only prepare us to be surprised yet again.",
    # 20 Texts with a lot of stop words
    "The a an and or but",
    "A an the",
    "I am you are he she it we they",
    "Is are was were",
    "Do does did",
    "Have has had",
    "Can could",
    "I am is not the you are",
    "I is not the you are",
]

PROCESSED_TEXTS = list(map(
    lambda text: text.lower().replace('.', '').replace(',', '').replace('!', '').replace(';', '').split(' '),
    TEXTS
))


def bm25_collection_setup(
        collection_name='test_collection',
):
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
                "text": {

                }
            }
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": idx,
                    "vector": {
                        "text": {
                            "model": "qdrant/bm25",
                            "text": TEXTS[idx]
                        }
                    },
                    "payload": {
                        "text": TEXTS[idx]
                    }
                }
                for idx in range(len(PROCESSED_TEXTS))
            ]
        }
    )
    assert response.ok


def test_collection_request_with_idf(collection_name):
    queries = ["I should not fear", "It's universally acknowledged"]

    for query in queries:
        response = request_with_validation(
            api='/collections/{collection_name}/points/query',
            method="POST",
            path_params={'collection_name': collection_name},
            body={
                "query": {
                    "model": "qdrant/bm25",
                    "text": query,
                },
                "using": "text",
                "limit": 2
            }
        )
        assert response.ok
        assert len(response.json()['result']) == 1

