"""Regression test for https://github.com/qdrant/qdrant/issues/8724.

With `lowercase=false` and a mixed-case custom stopword list, the gRPC path
unconditionally lowercased custom stopwords while REST preserved them verbatim,
so the two transports stored different stopword sets on disk and disagreed on
identical MatchText queries.
"""

import pytest
from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchText,
    PointStruct,
    StopwordsSet,
    TextIndexParams,
    TextIndexType,
    TokenizerType,
    VectorParams,
)

from .helpers.helpers import qdrant_host_headers
from .helpers.settings import QDRANT_HOST


def _build_client(prefer_grpc: bool) -> QdrantClient:
    host_headers = qdrant_host_headers()
    grpc_host = host_headers.get("host") or host_headers.get("Host") or "127.0.0.1"

    # NOTE: The test setup is different compared to other pure HTTP or gRPC tests.
    # It intentionally compares REST and gRPC through QdrantClient instead of request_with_validation.
    # Both clients must still use the shared host-routing config for proxy-backed runs.
    if prefer_grpc:
        return QdrantClient(
            host=grpc_host,
            port=6333,
            grpc_port=6334,
            prefer_grpc=True,
            timeout=30,
        )

    return QdrantClient(
        url=QDRANT_HOST,
        prefer_grpc=False,
        timeout=30,
        headers=host_headers,
    )


def _points():
    return [
        PointStruct(
            id=1,
            vector=[1.0, 0.0],
            payload={"title": "The quick brown fox jumps over the lazy dog"},
        ),
        PointStruct(
            id=2,
            vector=[0.0, 1.0],
            payload={"title": "LAZY sentinel"},
        ),
    ]


def _scroll_ids(client: QdrantClient, collection: str, term: str):
    points, _ = client.scroll(
        collection_name=collection,
        scroll_filter=Filter(
            must=[FieldCondition(key="title", match=MatchText(text=term))]
        ),
        limit=32,
        with_payload=False,
        with_vectors=False,
    )
    return sorted(int(p.id) for p in points)


def _run(prefer_grpc: bool):
    collection = f"stopwords_repro_{'grpc' if prefer_grpc else 'rest'}"
    client = _build_client(prefer_grpc)
    try:
        if client.collection_exists(collection):
            client.delete_collection(collection)
        client.create_collection(
            collection_name=collection,
            vectors_config=VectorParams(size=2, distance=Distance.DOT),
        )
        client.upsert(collection_name=collection, points=_points(), wait=True)
        client.create_payload_index(
            collection_name=collection,
            field_name="title",
            field_schema=TextIndexParams(
                type=TextIndexType.TEXT,
                tokenizer=TokenizerType.WORD,
                lowercase=False,
                stopwords=StopwordsSet(custom=["the", "The", "LAZY"]),
            ),
            wait=True,
        )
        return {
            term: _scroll_ids(client, collection, term)
            for term in ("lazy", "The", "LAZY")
        }
    finally:
        if client.collection_exists(collection):
            client.delete_collection(collection)
        client.close()


@pytest.mark.parametrize("prefer_grpc", [False, True], ids=["rest", "grpc"])
def test_custom_stopwords_case_sensitive(prefer_grpc):
    """With lowercase=false and stopwords {"the","The","LAZY"}:
    - "lazy" is not a stopword -> matches point 1
    - "The" and "LAZY" are stopwords -> filtered from query -> []

    Before the fix, the gRPC parametrization failed: custom stopwords were
    lowercased during conversion, so the set was stored as {"the","lazy"}.
    """
    results = _run(prefer_grpc)
    assert results == {"lazy": [1], "The": [], "LAZY": []}
