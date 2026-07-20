import pytest

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation

TOTAL_POINTS = 32


@pytest.fixture(autouse=True, scope="module")
def setup(collection_name):
    create_collection(collection_name)
    yield
    drop_collection(collection_name=collection_name)


def create_collection(collection_name):
    drop_collection(collection_name)

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": 2,
                "distance": "Dot",
            },
        }
    )
    assert response.ok

    # Half numeric ids, half UUIDs — slice membership must work for both
    points = [
        {"id": point_id, "vector": [0.1, 0.2]}
        for point_id in range(TOTAL_POINTS // 2)
    ] + [
        {"id": f"00000000-0000-0000-0000-{seed:012x}", "vector": [0.1, 0.2]}
        for seed in range(TOTAL_POINTS // 2)
    ]

    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={"points": points}
    )
    assert response.ok


def scroll_slice(collection_name, total, index):
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {"must": [{"slice": {"total": total, "index": index}}]},
            "limit": TOTAL_POINTS + 1,
            "with_payload": False,
            "with_vector": False,
        }
    )
    assert response.ok
    return {str(p['id']) for p in response.json()['result']['points']}


def test_slice_partition(collection_name):
    total = 4
    seen = set()
    for index in range(total):
        ids = scroll_slice(collection_name, total, index)
        assert not (seen & ids), "slices must be disjoint"
        seen |= ids
    assert len(seen) == TOTAL_POINTS, "slices must cover all points"


def test_slice_must_not(collection_name):
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {"must_not": [{"slice": {"total": 4, "index": 0}}]},
            "limit": TOTAL_POINTS + 1,
            "with_payload": False,
            "with_vector": False,
        }
    )
    assert response.ok
    inverted = {str(p['id']) for p in response.json()['result']['points']}
    assert len(inverted) == TOTAL_POINTS - len(scroll_slice(collection_name, 4, 0))


@pytest.mark.parametrize(
    "slice_body, expected_status",
    [
        # index out of range: schema-valid, rejected by request validation
        ({"total": 4, "index": 4}, 422),
        # zero total: rejected at deserialization
        ({"total": 0, "index": 0}, 400),
    ],
)
def test_slice_invalid(collection_name, slice_body, expected_status):
    response = request_with_validation(
        api='/collections/{collection_name}/points/scroll',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "filter": {"must": [{"slice": slice_body}]},
            "limit": 1,
        }
    )
    assert response.status_code == expected_status
