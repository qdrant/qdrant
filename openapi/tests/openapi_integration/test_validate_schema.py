from .helpers.helpers import request_with_validation
import pytest
import jsonschema

collection_name = 'test_collection'


def test_validate_schema():
    search_without_filter()
    malformed_condition()
    malformed_path_param()
    malformed_query_param()


def malformed_condition():
    """
    Should raise a ValidationError because the condition key is not defined
    """
    with pytest.raises(jsonschema.exceptions.ValidationError):
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

def malformed_query_param():
    """
    Should raise a ValidationError because the URL path param is not a string
    """
    with pytest.raises(jsonschema.exceptions.ValidationError):
        request_with_validation(
            api='/telemetry',
            method="GET",
            query_params={'anonymize': "non-boolean-value"}
        )

def malformed_path_param():
    """
    Should raise a ValidationError because the URL query param is not a string
    """
    with pytest.raises(jsonschema.exceptions.ValidationError):
        request_with_validation(
            api='/collections/{collection_name}/points/search',
            method="POST",
            path_params={'collection_name': 1},
            body={
                "vector": [0.2, 0.1, 0.9, 0.7],
                "limit": 5,
                "filter": {
                    "should": [
                        {
                            "key": "city"
                        }
                    ]
                },
                "with_payload": True
            }
        )

def search_without_filter():
    """
    Test client-side validation of search request without any filtering
    """
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