from .helpers.helpers import request_with_validation
import pytest
import jsonschema

collection_name = 'test_collection'


# validate that malformed conditions raise a JsonSchema ValidationError

def test_malformed_condition():
    # Should raise a ValidationError because the condition key is not defined
    # see https://github.com/qdrant/qdrant/issues/1664
    with pytest.raises(jsonschema.exceptions.ValidationError):
        malformed_invalid_condition()
    test_search_without_filter()


def malformed_invalid_condition():
    # disallow invalid FieldCondition payloads
    request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 5,
            "filter": {
                "should": [
                    {
                        "undefined_condition_key": {
                            "key": "city"
                        }
                    }
                ]
            },
            "with_payload": True
        }
    )

def test_search_without_filter():
    request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 5,
            "with_payload": True
        }
    )