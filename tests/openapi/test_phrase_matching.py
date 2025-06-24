import pytest
import random

from .helpers.collection_setup import (
    drop_collection,
)
from .helpers.helpers import (
    request_with_validation,
)

# Field name constant
FIELD_NAME = "text"


def setup_phrase_collection(
    collection_name="test_phrase_collection",
):
    """Setup a collection with phrase matching enabled for text fields."""
    drop_collection(collection_name)

    # Create collection
    response = request_with_validation(
        api="/collections/{collection_name}",
        method="PUT",
        path_params={"collection_name": collection_name},
        body={
            "vectors": {
                "size": 4,
                "distance": "Dot",
            },
        },
    )
    assert response.ok

    # Create text index with phrase matching enabled
    response = request_with_validation(
        api="/collections/{collection_name}/index",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={
            "field_name": FIELD_NAME,
            "field_schema": {
                "type": "text",
                "tokenizer": "word",
                "phrase_matching": True,
            },
        },
    )
    assert response.ok

    # Insert test points with various sentences
    phrases = [
        "the quick brown fox jumps over the lazy dog",
        "a quick brown cat runs through the garden",
        "the brown fox is very quick and agile",
        "lazy dogs sleep all day long",
        "machine learning algorithms are powerful tools",
        "natural language processing with transformers",
        "the artificial intelligence revolution",
        "deep learning neural networks",
        "brown sugar and quick oats",
        "New York City is a bustling metropolis",
        "writing tests is very very very important",
    ]

    test_data = [
        {
            "id": i + 1,
            "vector": [random.random() for _ in range(4)],
            "payload": {FIELD_NAME: phrase},
        }
        for i, phrase in enumerate(phrases)
    ]

    response = request_with_validation(
        api="/collections/{collection_name}/points",
        method="PUT",
        path_params={"collection_name": collection_name},
        query_params={"wait": "true"},
        body={"points": test_data},
    )
    assert response.ok

    return collection_name


@pytest.fixture(scope="module")
def phrase_collection():
    collection_name = setup_phrase_collection()
    yield collection_name
    drop_collection(collection_name)


@pytest.mark.parametrize(
    "phrase,expected_count,expected_ids,description",
    [
        (
            "quick brown fox",
            1,
            {1},
            "exact sequence match",
        ),
        (
            "brown quick",
            0,
            set(),
            "wrong order no match",
        ),
        (
            "quick brown",
            2,
            {1, 2},
            "correct order",
        ),
        (
            "brown",
            4,
            {1, 2, 3, 9},
            "single word phrase",
        ),
        (
            "machine learning algorithms are powerful tools",
            1,
            {5},
            "longer phrase match",
        ),
        ("", 0, set(), "empty phrase"),
        ("very very very", 1, {11}, "repeated word"),
        ("very very very very", 0, set(), "repeated word too many times"),
    ],
)
def test_phrase_matching(
    phrase_collection,
    phrase,
    expected_count,
    expected_ids,
    description,
):
    """Test phrase matching with various phrases and expected results."""

    # Test scroll endpoint
    scroll_response = request_with_validation(
        api="/collections/{collection_name}/points/scroll",
        method="POST",
        path_params={"collection_name": phrase_collection},
        body={
            "limit": 10,
            "with_payload": True,
            "with_vector": False,
            "filter": {
                "must": [
                    {
                        "key": FIELD_NAME,
                        "match": {"phrase": phrase},
                    }
                ]
            },
        },
    )

    assert scroll_response.ok
    scroll_result = scroll_response.json()["result"]

    # Verify expected count
    assert len(scroll_result["points"]) == expected_count, (
        f"Failed for {description}: expected {expected_count} points"
    )

    # Verify expected IDs if any matches
    if expected_count > 0:
        matched_ids = {point["id"] for point in scroll_result["points"]}
        assert matched_ids == expected_ids, (
            f"Failed for {description}: expected IDs {expected_ids}, got {matched_ids}"
        )

        # Verify that phrase appears in matched documents (except empty phrase)
        if phrase:
            for point in scroll_result["points"]:
                assert phrase in point["payload"][FIELD_NAME], (
                    f"Phrase '{phrase}' not found in matched document"
                )

    # Check count
    count_response = request_with_validation(
        api="/collections/{collection_name}/points/count",
        method="POST",
        path_params={"collection_name": phrase_collection},
        body={
            "filter": {
                "must": [
                    {
                        "key": FIELD_NAME,
                        "match": {"phrase": phrase},
                    }
                ]
            },
        },
    )

    assert count_response.ok
    count_result = count_response.json()["result"]

    # Verify count matches scroll results
    assert count_result["count"] == expected_count, f"Count endpoint mismatch for {description}"
