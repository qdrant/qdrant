from grpc import RpcError
from qdrant_client import QdrantClient, grpc as qgrpc
import pytest
from qdrant_client.conversions.conversion import payload_to_grpc
import os


QDRANT_HOST = os.environ.get("QDRANT_HOST", "localhost")
CLIENT = QdrantClient(prefer_grpc=True, timeout=3.0, host=QDRANT_HOST)


def test_create_collection():
    assert_read_write(
        CLIENT.grpc_collections.Create,
        qgrpc.CreateCollection(
            collection_name="to_be_deleted",
            vectors_config={"params": {"size": 4, "distance": "Dot"}},
        ),
    )


def test_delete_collection():
    assert_read_write(
        CLIENT.grpc_collections.Delete,
        qgrpc.DeleteCollection(collection_name="to_be_deleted"),
    )


def test_list_collections():
    assert_read_only(
        CLIENT.grpc_collections.List,
        qgrpc.ListCollectionsRequest(),
    )


def test_insert_points():
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


def test_get_collection():
    assert_read_only(
        CLIENT.grpc_collections.Get,
        qgrpc.GetCollectionInfoRequest(collection_name="test_collection"),
    )


def test_search_points():
    assert_read_only(
        CLIENT.grpc_points.Search,
        qgrpc.SearchPoints(
            collection_name="test_collection", vector=[0.2, 0.1, 0.9, 0.7], limit=3
        ),
    )


def test_scroll_points():
    assert_read_only(
        CLIENT.grpc_points.Scroll,
        qgrpc.ScrollPoints(
            collection_name="test_collection",
        ),
    )


def test_get_points():
    assert_read_only(
        CLIENT.grpc_points.Get,
        qgrpc.GetPoints(
            collection_name="test_collection",
        ),
    )


def test_recommend_points():
    assert_read_only(
        CLIENT.grpc_points.Recommend,
        qgrpc.RecommendPoints(
            collection_name="test_collection",
            positive=[{"num": 1}],
            negative=[{"num": 2}],
        ),
    )


def test_create_collection_alias():
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


def assert_read_only(stub, request):
    assert_rw_token_success(stub, request)
    assert_ro_token_success(stub, request)


def assert_read_write(stub, request):
    assert_rw_token_success(stub, request)
    assert_ro_token_failure(stub, request)


def assert_rw_token_success(stub, request):
    """Perform the request with a read-write token and assert success"""
    stub(request, metadata=(("api-key", "my-secret"),), timeout=1.0)


def assert_ro_token_success(stub, request):
    """Perform the request with a read-only token and assert success"""
    stub(request, metadata=(("api-key", "my-ro-secret"),), timeout=1.0)


def assert_ro_token_failure(stub, request):
    """Perform the request with a read-only token and assert failure"""
    try:
        stub(request, metadata=(("api-key", "my-ro-secret"),), timeout=1.0)
        pytest.fail("Request should have failed")
    except RpcError:
        return
