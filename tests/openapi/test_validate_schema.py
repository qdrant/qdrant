from .helpers.helpers import request_with_validation


# validate that malformed conditions raise a JsonSchema ValidationError

def test_malformed_condition(collection_name):
    # Should raise a ValidationError because the condition key is not defined
    # see https://github.com/qdrant/qdrant/issues/1664
    # with pytest.raises(jsonschema.exceptions.ValidationError):
    malformed_condition(collection_name)


def malformed_condition(collection_name):
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
