from typing import Literal
import functools
from grpc import RpcError, StatusCode
from qdrant_client import QdrantClient, grpc as qgrpc
import pytest
from qdrant_client.conversions.conversion import payload_to_grpc
import os
import jwt


QDRANT_HOST = os.environ.get("QDRANT_HOST", "localhost")
CLIENT = QdrantClient(prefer_grpc=True, timeout=3.0, host=QDRANT_HOST)

JWT_EMPTY = jwt.encode({}, "my-secret", algorithm="HS256")
JWT_R = jwt.encode({"w": False}, "my-secret", algorithm="HS256")
JWT_RW = jwt.encode({"w": True}, "my-secret", algorithm="HS256")


def test_create_collection(assert_read_write):
    assert_read_write(
        CLIENT.grpc_collections.Create,
        qgrpc.CreateCollection(
            collection_name="to_be_deleted",
            vectors_config={"params": {"size": 4, "distance": "Dot"}},
        ),
    )


def test_delete_collection(assert_read_write):
    assert_read_write(
        CLIENT.grpc_collections.Delete,
        qgrpc.DeleteCollection(collection_name="to_be_deleted"),
    )


def test_list_collections(assert_read_only):
    assert_read_only(
        CLIENT.grpc_collections.List,
        qgrpc.ListCollectionsRequest(),
    )


def test_insert_points(assert_read_write):
    assert_read_write(
        CLIENT.grpc_points.Upsert,
        qgrpc.UpsertPoints(
            collection_name="test_collection",
            wait=True,
            points=[
                qgrpc.PointStruct(
                    id={"num": 1},
                    vectors={"vector": {"data": [0.05, 0.61, 0.76, 0.74]}},
                    payload=payload_to_grpc(
                        {
                            "city": {"string_value": "Berlin"},
                            "country": {"string_value": "Germany"},
                            "population": {"integer_value": 1000000},
                            "square": {"double_value": 12.5},
                            "coords": {
                                "struct_value": {
                                    "fields": {
                                        "lat": {"double_value": 1.0},
                                        "lon": {"double_value": 2.0},
                                    }
                                }
                            },
                        }
                    ),
                )
            ],
        ),
    )


def test_get_collection(assert_read_only):
    assert_read_only(
        CLIENT.grpc_collections.Get,
        qgrpc.GetCollectionInfoRequest(collection_name="test_collection"),
    )


def test_search_points(assert_read_only):
    assert_read_only(
        CLIENT.grpc_points.Search,
        qgrpc.SearchPoints(
            collection_name="test_collection", vector=[0.2, 0.1, 0.9, 0.7], limit=3
        ),
    )


def test_scroll_points(assert_read_only):
    assert_read_only(
        CLIENT.grpc_points.Scroll,
        qgrpc.ScrollPoints(
            collection_name="test_collection",
        ),
    )


def test_get_points(assert_read_only):
    assert_read_only(
        CLIENT.grpc_points.Get,
        qgrpc.GetPoints(
            collection_name="test_collection",
        ),
    )


def test_recommend_points(assert_read_only):
    assert_read_only(
        CLIENT.grpc_points.Recommend,
        qgrpc.RecommendPoints(
            collection_name="test_collection",
            positive=[{"num": 1}],
            negative=[{"num": 2}],
        ),
    )


def test_create_collection_alias(assert_read_write):
    assert_read_write(
        CLIENT.grpc_collections.UpdateAliases,
        qgrpc.ChangeAliases(
            actions=[
                qgrpc.AliasOperations(
                    create_alias=qgrpc.CreateAlias(
                        collection_name="test_collection", alias_name="test_alias"
                    )
                )
            ]
        ),
    )


def mkparams(mode: Literal["rw", "ro"]):
    return [
        ((), False),

        ((("api-key", "my-secret"),), True),
        ((("authorization", "Bearer my-secret"),), True),
        ((("authorization", f"Bearer {JWT_RW}"),), True),

        ((("api-key", "my-ro-secret"),), mode == "ro"),
        ((("authorization", "Bearer my-ro-secret"),), mode == "ro"),
        ((("authorization", f"Bearer {JWT_R}"),), mode == "ro"),
        ((("authorization", f"Bearer {JWT_EMPTY}"),), mode == "ro"),
    ]


@pytest.fixture(scope="module", params=mkparams("ro"))
def assert_read_only(request):
    yield functools.partial(assert_access, request.param)


@pytest.fixture(scope="module", params=mkparams("rw"))
def assert_read_write(request):
    yield functools.partial(assert_access, request.param)


def assert_access(param, stub, request):
    metadata, should_allow = param
    allowed = False
    try:
        stub(request, metadata=metadata, timeout=1.0)
        allowed = True
    except RpcError as e:
        allowed = e.code() != StatusCode.PERMISSION_DENIED
    assert allowed == should_allow
