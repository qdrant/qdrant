import pytest
import requests
import struct
import uuid

from .helpers.collection_setup import drop_collection
from .helpers.settings import QDRANT_HOST

HEADERS = {"Content-Type": "application/json"}


def float_to_bits(f: float) -> int:
    return struct.unpack(">Q", struct.pack(">d", f))[0]


def raw_post(endpoint: str, raw_json_str: str) -> requests.Response:
    url = f"{QDRANT_HOST}{endpoint}"
    return requests.post(url, data=raw_json_str.encode("utf-8"), headers=HEADERS)


def raw_put(endpoint: str, raw_json_str: str) -> requests.Response:
    url = f"{QDRANT_HOST}{endpoint}"
    return requests.put(url, data=raw_json_str.encode("utf-8"), headers=HEADERS)


def raw_get(endpoint: str) -> requests.Response:
    url = f"{QDRANT_HOST}{endpoint}"
    return requests.get(url, headers=HEADERS)


def create_test_collection(name: str):
    drop_collection(name)
    body = {"vectors": {"size": 4, "distance": "Dot"}}
    resp = requests.put(f"{QDRANT_HOST}/collections/{name}", json=body, headers=HEADERS)
    assert resp.ok, f"Failed to create collection: {resp.text}"


@pytest.fixture
def collection_name():
    """Isolated, function-scoped test collection with a unique name to avoid cross-worker collisions."""
    name = f"test_rest_float_{uuid.uuid4().hex[:12]}"
    create_test_collection(name)
    try:
        yield name
    finally:
        drop_collection(name)


def test_case_a_float_payload_roundtrip(collection_name):
    """Case A: Insert exact float payload, read it back via REST, assert numeric identity."""
    point_id = 1
    # Raw literal sent over the wire without python float serialization
    raw_upsert = f"""
    {{
        "points": [
            {{
                "id": {point_id},
                "vector": [0.1, 0.2, 0.3, 0.4],
                "payload": {{
                    "value": 2251799813685247.0
                }}
            }}
        ]
    }}
    """
    resp = raw_put(f"/collections/{collection_name}/points?wait=true", raw_upsert)
    assert resp.ok, resp.text

    # Read back point
    get_resp = raw_get(f"/collections/{collection_name}/points/{point_id}")
    assert get_resp.ok, get_resp.text
    data = get_resp.json()
    returned_val = data["result"]["payload"]["value"]

    # Verify exact IEEE-754 bit identity
    expected = 2251799813685247.0
    assert float_to_bits(returned_val) == float_to_bits(expected), (
        f"Bit mismatch: got {returned_val} ({float_to_bits(returned_val):x}), "
        f"expected {expected} ({float_to_bits(expected):x})"
    )
    # Check that it did NOT corrupt to 2251799813685247.25
    assert "2251799813685247.25" not in get_resp.text


@pytest.fixture
def adjacent_points(collection_name):
    """Explicitly upsert points 10 and 11 with adjacent float values into the test's isolated collection."""
    raw_upsert = """
    {
        "points": [
            {
                "id": 10,
                "vector": [0.1, 0.2, 0.3, 0.4],
                "payload": {"val": 2251799813685246.0}
            },
            {
                "id": 11,
                "vector": [0.1, 0.2, 0.3, 0.4],
                "payload": {"val": 2251799813685247.0}
            }
        ]
    }
    """
    resp = raw_put(f"/collections/{collection_name}/points?wait=true", raw_upsert)
    assert resp.ok, resp.text
    return [10, 11]


def test_case_b_adjacent_float_values_distinguishable(collection_name, adjacent_points):
    """Case B: Two adjacent exact floats must remain distinguishable."""
    p10 = raw_get(f"/collections/{collection_name}/points/10").json()["result"][
        "payload"
    ]["val"]
    p11 = raw_get(f"/collections/{collection_name}/points/11").json()["result"][
        "payload"
    ]["val"]

    assert p10 == 2251799813685246.0
    assert p11 == 2251799813685247.0
    assert p10 != p11
    assert float_to_bits(p11) != float_to_bits(p10)


def test_case_c_degenerate_range_filter(collection_name, adjacent_points):
    """Case C: Degenerate range filter gte=v, lte=v matches only the exact intended point."""
    raw_scroll = """
    {
        "limit": 10,
        "filter": {
            "must": [
                {
                    "key": "val",
                    "range": {
                        "gte": 2251799813685247.0,
                        "lte": 2251799813685247.0
                    }
                }
            ]
        }
    }
    """
    resp = raw_post(f"/collections/{collection_name}/points/scroll", raw_scroll)
    assert resp.ok, resp.text
    points = resp.json()["result"]["points"]
    assert len(points) == 1, f"Expected exactly 1 point, got: {points}"
    assert points[0]["id"] == 11
    assert points[0]["payload"]["val"] == 2251799813685247.0


def test_case_c_boundary_9007199254740991(collection_name):
    """Boundary 2^53 - 1 (9007199254740991.0) and adjacent 9007199254740990.0."""
    raw_upsert = """
    {
        "points": [
            {
                "id": 20,
                "vector": [0.1, 0.2, 0.3, 0.4],
                "payload": {"boundary": 9007199254740990.0}
            },
            {
                "id": 21,
                "vector": [0.1, 0.2, 0.3, 0.4],
                "payload": {"boundary": 9007199254740991.0}
            }
        ]
    }
    """
    resp = raw_put(f"/collections/{collection_name}/points?wait=true", raw_upsert)
    assert resp.ok, resp.text

    # Search degenerate range for 9007199254740991.0: must return ONLY point 21
    raw_scroll = """
    {
        "limit": 10,
        "filter": {
            "must": [
                {
                    "key": "boundary",
                    "range": {
                        "gte": 9007199254740991.0,
                        "lte": 9007199254740991.0
                    }
                }
            ]
        }
    }
    """
    resp = raw_post(f"/collections/{collection_name}/points/scroll", raw_scroll)
    assert resp.ok, resp.text
    points = resp.json()["result"]["points"]
    assert len(points) == 1, f"Expected only point 21, got: {points}"
    assert points[0]["id"] == 21


def test_case_d_integer_payload_float_bound(collection_name):
    """Case D: Integer-formatted payload, float-formatted range bounds."""
    raw_upsert = """
    {
        "points": [
            {
                "id": 30,
                "vector": [0.1, 0.2, 0.3, 0.4],
                "payload": {"int_val": 2251799813685246}
            },
            {
                "id": 31,
                "vector": [0.1, 0.2, 0.3, 0.4],
                "payload": {"int_val": 2251799813685247}
            }
        ]
    }
    """
    resp = raw_put(f"/collections/{collection_name}/points?wait=true", raw_upsert)
    assert resp.ok, resp.text

    # Query with float bound 2251799813685247.0
    raw_scroll = """
    {
        "limit": 10,
        "filter": {
            "must": [
                {
                    "key": "int_val",
                    "range": {
                        "gte": 2251799813685247.0,
                        "lte": 2251799813685247.0
                    }
                }
            ]
        }
    }
    """
    resp = raw_post(f"/collections/{collection_name}/points/scroll", raw_scroll)
    assert resp.ok, resp.text
    points = resp.json()["result"]["points"]
    assert len(points) == 1, (
        f"Expected 1 point for integer payload with float bound, got: {points}"
    )
    assert points[0]["id"] == 31


def test_case_e_float_payload_integer_bound(collection_name, adjacent_points):
    """Case E: Float payload, integer-formatted range bound."""
    raw_scroll = """
    {
        "limit": 10,
        "filter": {
            "must": [
                {
                    "key": "val",
                    "range": {
                        "gte": 2251799813685247,
                        "lte": 2251799813685247
                    }
                }
            ]
        }
    }
    """
    resp = raw_post(f"/collections/{collection_name}/points/scroll", raw_scroll)
    assert resp.ok, resp.text
    points = resp.json()["result"]["points"]
    assert len(points) == 1
    assert points[0]["id"] == 11


def test_case_f_unindexed_inclusive_range(collection_name, adjacent_points):
    """Case F: Unindexed inclusive range filter across adjacent points."""
    query_unindexed = """
    {
        "limit": 10,
        "filter": {
            "must": [
                {
                    "key": "val",
                    "range": {
                        "gte": 2251799813685246.0,
                        "lte": 2251799813685247.0
                    }
                }
            ]
        }
    }
    """
    resp_unindexed = raw_post(
        f"/collections/{collection_name}/points/scroll", query_unindexed
    )
    assert resp_unindexed.ok, resp_unindexed.text
    pts_unindexed = resp_unindexed.json()["result"]["points"]
    assert len(pts_unindexed) == 2
    assert sorted([p["id"] for p in pts_unindexed]) == [10, 11]


@pytest.fixture
def indexed_collection():
    """Independent collection dedicated to indexed operations, preventing any contamination of unindexed tests."""
    name = f"test_rest_float_indexed_{uuid.uuid4().hex[:12]}"
    create_test_collection(name)
    try:
        raw_upsert = """
        {
            "points": [
                {
                    "id": 10,
                    "vector": [0.1, 0.2, 0.3, 0.4],
                    "payload": {"val": 2251799813685246.0}
                },
                {
                    "id": 11,
                    "vector": [0.1, 0.2, 0.3, 0.4],
                    "payload": {"val": 2251799813685247.0}
                }
            ]
        }
        """
        resp = raw_put(f"/collections/{name}/points?wait=true", raw_upsert)
        assert resp.ok, resp.text
        yield name
    finally:
        drop_collection(name)


def test_case_f_and_g_indexed_vs_unindexed(indexed_collection):
    """Case F & G: Compare query results before and after creating numeric float index on dedicated collection."""
    query_unindexed = """
    {
        "limit": 10,
        "filter": {
            "must": [
                {
                    "key": "val",
                    "range": {
                        "gte": 2251799813685246.0,
                        "lte": 2251799813685247.0
                    }
                }
            ]
        }
    }
    """
    resp_unindexed = raw_post(
        f"/collections/{indexed_collection}/points/scroll", query_unindexed
    )
    assert resp_unindexed.ok, resp_unindexed.text
    pts_unindexed = resp_unindexed.json()["result"]["points"]
    assert len(pts_unindexed) == 2

    # Create numeric index on "val"
    create_index = """
    {
        "field_name": "val",
        "field_schema": "float"
    }
    """
    resp_idx = raw_put(
        f"/collections/{indexed_collection}/index?wait=true", create_index
    )
    assert resp_idx.ok, resp_idx.text

    # Re-run query against indexed field
    resp_indexed = raw_post(
        f"/collections/{indexed_collection}/points/scroll", query_unindexed
    )
    assert resp_indexed.ok, resp_indexed.text
    pts_indexed = resp_indexed.json()["result"]["points"]
    assert len(pts_indexed) == 2

    # Verify identical point IDs and payloads
    ids_unindexed = sorted([p["id"] for p in pts_unindexed])
    ids_indexed = sorted([p["id"] for p in pts_indexed])
    assert ids_unindexed == ids_indexed

    # Check degenerate range with index
    deg_query = """
    {
        "limit": 10,
        "filter": {
            "must": [
                {
                    "key": "val",
                    "range": {
                        "gte": 2251799813685247.0,
                        "lte": 2251799813685247.0
                    }
                }
            ]
        }
    }
    """
    resp_deg = raw_post(f"/collections/{indexed_collection}/points/scroll", deg_query)
    assert resp_deg.ok
    deg_points = resp_deg.json()["result"]["points"]
    assert len(deg_points) == 1
    assert deg_points[0]["id"] == 11


def test_case_h_negative_values(collection_name):
    """Case H: Negative large magnitude floats."""
    raw_upsert = """
    {
        "points": [
            {
                "id": 40,
                "vector": [0.1, 0.2, 0.3, 0.4],
                "payload": {"neg": -2251799813685247.0}
            },
            {
                "id": 41,
                "vector": [0.1, 0.2, 0.3, 0.4],
                "payload": {"neg": -2251799813685246.0}
            }
        ]
    }
    """
    resp = raw_put(f"/collections/{collection_name}/points?wait=true", raw_upsert)
    assert resp.ok, resp.text

    # Read back
    p40 = raw_get(f"/collections/{collection_name}/points/40").json()["result"][
        "payload"
    ]["neg"]
    assert float_to_bits(p40) == float_to_bits(-2251799813685247.0)

    # Degenerate range query for negative bound
    query = """
    {
        "limit": 10,
        "filter": {
            "must": [
                {
                    "key": "neg",
                    "range": {
                        "gte": -2251799813685247.0,
                        "lte": -2251799813685247.0
                    }
                }
            ]
        }
    }
    """
    resp_query = raw_post(f"/collections/{collection_name}/points/scroll", query)
    assert resp_query.ok, resp_query.text
    points = resp_query.json()["result"]["points"]
    assert len(points) == 1
    assert points[0]["id"] == 40


def test_case_i_and_j_various_magnitudes(collection_name):
    """Case I & J: Neighboring good values and representative small floats."""
    cases = [
        (50, 4503599627370494.0, "4503599627370494.0"),
        (51, 4503599627370495.0, "4503599627370495.0"),
        (52, 2251799813685245.0, "2251799813685245.0"),
        (53, 0.1, "0.1"),
        (54, 1.5, "1.5"),
        (55, 12345.6789, "12345.6789"),
    ]

    points_json = ", ".join(
        f'{{"id": {cid}, "vector": [0.1, 0.2, 0.3, 0.4], "payload": {{"v": {c_lit}}}}}'
        for cid, _, c_lit in cases
    )
    raw_upsert = f'{{"points": [{points_json}]}}'
    resp = raw_put(f"/collections/{collection_name}/points?wait=true", raw_upsert)
    assert resp.ok, resp.text

    for cid, c_val, c_lit in cases:
        get_resp = raw_get(f"/collections/{collection_name}/points/{cid}")
        assert get_resp.ok
        val = get_resp.json()["result"]["payload"]["v"]
        assert float_to_bits(val) == float_to_bits(c_val), f"Mismatch for {c_lit}"

        # Degenerate range filter
        scroll_body = f"""
        {{
            "limit": 10,
            "filter": {{
                "must": [
                    {{
                        "key": "v",
                        "range": {{
                            "gte": {c_lit},
                            "lte": {c_lit}
                        }}
                    }}
                ]
            }}
        }}
        """
        resp_scroll = raw_post(
            f"/collections/{collection_name}/points/scroll", scroll_body
        )
        assert resp_scroll.ok
        pts = resp_scroll.json()["result"]["points"]
        assert len(pts) == 1, f"Failed degenerate range for {c_lit}: got {pts}"
        assert pts[0]["id"] == cid


def test_case_k_negative_zero(collection_name):
    """Case K: Negative zero (-0.0) representation and range query parity."""
    raw_upsert = """
    {
        "points": [
            {
                "id": 60,
                "vector": [0.1, 0.2, 0.3, 0.4],
                "payload": {"val": -0.0}
            }
        ]
    }
    """
    resp = raw_put(f"/collections/{collection_name}/points?wait=true", raw_upsert)
    assert resp.ok, resp.text

    # Read back point
    p60 = raw_get(f"/collections/{collection_name}/points/60").json()["result"]["payload"]["val"]
    # In IEEE-754, -0.0 == 0.0 numerically, but sign bit must be preserved
    assert p60 == 0.0
    assert float_to_bits(p60) == float_to_bits(-0.0)

    # Range query with -0.0 bounds
    query_neg = """
    {
        "limit": 10,
        "filter": {
            "must": [
                {
                    "key": "val",
                    "range": {
                        "gte": -0.0,
                        "lte": -0.0
                    }
                }
            ]
        }
    }
    """
    resp_neg = raw_post(f"/collections/{collection_name}/points/scroll", query_neg)
    assert resp_neg.ok, resp_neg.text
    pts_neg = resp_neg.json()["result"]["points"]
    assert len(pts_neg) == 1
    assert pts_neg[0]["id"] == 60

    # Range query with 0.0 bounds
    query_pos = """
    {
        "limit": 10,
        "filter": {
            "must": [
                {
                    "key": "val",
                    "range": {
                        "gte": 0.0,
                        "lte": 0.0
                    }
                }
            ]
        }
    }
    """
    resp_pos = raw_post(f"/collections/{collection_name}/points/scroll", query_pos)
    assert resp_pos.ok, resp_pos.text
    pts_pos = resp_pos.json()["result"]["points"]
    assert len(pts_pos) == 1
    assert pts_pos[0]["id"] == 60


def test_case_l_exponent_ranges_and_subnormals(collection_name):
    """Case L: Extreme exponent ranges, large floats, and subnormal precision."""
    cases = [
        (70, 1e100, "1e100"),
        (71, 1e-100, "1e-100"),
        (72, 2.2250738585072014e-308, "2.2250738585072014e-308"),
        (73, 5e-324, "5e-324"),
    ]
    points_json = ", ".join(
        f'{{"id": {cid}, "vector": [0.1, 0.2, 0.3, 0.4], "payload": {{"exp": {c_lit}}}}}'
        for cid, _, c_lit in cases
    )
    raw_upsert = f'{{"points": [{points_json}]}}'
    resp = raw_put(f"/collections/{collection_name}/points?wait=true", raw_upsert)
    assert resp.ok, resp.text

    for cid, c_val, c_lit in cases:
        get_resp = raw_get(f"/collections/{collection_name}/points/{cid}")
        assert get_resp.ok
        val = get_resp.json()["result"]["payload"]["exp"]
        assert float_to_bits(val) == float_to_bits(c_val), f"Mismatch for {c_lit}"

        scroll_body = f"""
        {{
            "limit": 10,
            "filter": {{
                "must": [
                    {{
                        "key": "exp",
                        "range": {{
                            "gte": {c_lit},
                            "lte": {c_lit}
                        }}
                    }}
                ]
            }}
        }}
        """
        resp_scroll = raw_post(f"/collections/{collection_name}/points/scroll", scroll_body)
        assert resp_scroll.ok
        pts = resp_scroll.json()["result"]["points"]
        assert len(pts) == 1, f"Failed range for {c_lit}: got {pts}"
        assert pts[0]["id"] == cid


def test_case_m_adjacent_representable_epsilon(collection_name):
    """Case M: Adjacent representable floats separated by 1 ULP (0.9999999999999999 vs 1.0 vs 1.0000000000000002)."""
    val0 = 0.9999999999999999
    val1 = 1.0
    val2 = 1.0 + 2.220446049250313e-16  # 1.0000000000000002
    assert float_to_bits(val1) == float_to_bits(val0) + 1
    assert float_to_bits(val2) == float_to_bits(val1) + 1

    raw_upsert = """
    {
        "points": [
            {
                "id": 79,
                "vector": [0.1, 0.2, 0.3, 0.4],
                "payload": {"v": 0.9999999999999999}
            },
            {
                "id": 80,
                "vector": [0.1, 0.2, 0.3, 0.4],
                "payload": {"v": 1.0}
            },
            {
                "id": 81,
                "vector": [0.1, 0.2, 0.3, 0.4],
                "payload": {"v": 1.0000000000000002}
            }
        ]
    }
    """
    resp = raw_put(f"/collections/{collection_name}/points?wait=true", raw_upsert)
    assert resp.ok, resp.text

    p79 = raw_get(f"/collections/{collection_name}/points/79").json()["result"]["payload"]["v"]
    p80 = raw_get(f"/collections/{collection_name}/points/80").json()["result"]["payload"]["v"]
    p81 = raw_get(f"/collections/{collection_name}/points/81").json()["result"]["payload"]["v"]
    assert float_to_bits(p79) == float_to_bits(val0)
    assert float_to_bits(p80) == float_to_bits(val1)
    assert float_to_bits(p81) == float_to_bits(val2)
    assert float_to_bits(p79) != float_to_bits(p80)
    assert float_to_bits(p80) != float_to_bits(p81)

    # Degenerate range for 0.9999999999999999 must return ONLY point 79
    scroll_79 = """
    {
        "limit": 10,
        "filter": {
            "must": [
                {
                    "key": "v",
                    "range": {
                        "gte": 0.9999999999999999,
                        "lte": 0.9999999999999999
                    }
                }
            ]
        }
    }
    """
    resp_79 = raw_post(f"/collections/{collection_name}/points/scroll", scroll_79)
    assert resp_79.ok
    pts_79 = resp_79.json()["result"]["points"]
    assert len(pts_79) == 1
    assert pts_79[0]["id"] == 79

    # Degenerate range for 1.0 must return ONLY point 80
    scroll_80 = """
    {
        "limit": 10,
        "filter": {
            "must": [
                {
                    "key": "v",
                    "range": {
                        "gte": 1.0,
                        "lte": 1.0
                    }
                }
            ]
        }
    }
    """
    resp_80 = raw_post(f"/collections/{collection_name}/points/scroll", scroll_80)
    assert resp_80.ok
    pts_80 = resp_80.json()["result"]["points"]
    assert len(pts_80) == 1
    assert pts_80[0]["id"] == 80

    # Degenerate range for 1.0000000000000002 must return ONLY point 81
    scroll_81 = """
    {
        "limit": 10,
        "filter": {
            "must": [
                {
                    "key": "v",
                    "range": {
                        "gte": 1.0000000000000002,
                        "lte": 1.0000000000000002
                    }
                }
            ]
        }
    }
    """
    resp_81 = raw_post(f"/collections/{collection_name}/points/scroll", scroll_81)
    assert resp_81.ok
    pts_81 = resp_81.json()["result"]["points"]
    assert len(pts_81) == 1
    assert pts_81[0]["id"] == 81
