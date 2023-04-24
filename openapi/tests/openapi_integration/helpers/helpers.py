from typing import Any, Dict
import jsonschema
import requests
from schemathesis.models import APIOperation
from schemathesis.specs.openapi.references import ConvertingResolver
from schemathesis.specs.openapi.schemas import OpenApi30

from .settings import QDRANT_HOST, SCHEMA


def get_api_string(host, api, path_params):
    """
    >>> get_api_string('http://localhost:6333', '/collections/{name}', {'name': 'hello', 'a': 'b'})
    'http://localhost:6333/collections/hello'
    """
    return f"{host}{api}".format(**path_params)


def validate_schema(data, operation_schema: OpenApi30, raw_definitions):
    """
    :param data: concrete values to validate
    :param operation_schema: operation schema
    :param raw_definitions: definitions to check data with
    :return:
    """
    resolver = ConvertingResolver(
        operation_schema.location or "",
        operation_schema.raw_schema,
        nullable_name=operation_schema.nullable_name,
        is_response_schema=False
    )
    jsonschema.validate(data, raw_definitions, cls=jsonschema.Draft201909Validator, resolver=resolver)


def request_with_validation(
        api: str,
        method: str,
        path_params: dict = {},
        query_params: dict = {},
        body: dict = None,
        validate_request: bool = True # causes client-side request validation
) -> requests.Response:
    operation: APIOperation = SCHEMA[api][method]

    assert isinstance(operation.schema, OpenApi30)

    if body and validate_request:
        validate_schema(
            data=body,
            operation_schema=operation.schema,
            raw_definitions=operation.definition.raw['requestBody']['content']['application/json']['schema']
        )
    
    action = getattr(requests, method.lower(), None)

    if not action:
        raise RuntimeError(f"Method {method} does not exists")

    if validate_request:
        for param in operation.path_parameters.items:
            if param.is_required:
                assert param.name in path_params

        for param in operation.query.items:
            if param.is_required:
                assert param.name in query_params

        allowed_path_params = set(p.name for p in operation.path_parameters.items)
        # Map of <path param name,schema>
        path_params_schemas: Dict[str, Any] = dict([(p["name"], p["schema"]) for p in operation.definition.raw.get('parameters',[]) if p["in"] == "path"])
        for param in path_params.keys():
            assert param in allowed_path_params
            value = path_params[param]
            validate_schema(value, operation.schema, raw_definitions=path_params_schemas[param])

        allowed_query_params = set(p.name for p in operation.query.items)
        # Map of <query param name,schema>
        query_params_schemas: Dict[str, Any] = dict([(p["name"], p["schema"]) for p in operation.definition.raw.get('parameters', []) if p["in"] == "query"])
        for param in query_params.keys():
            assert param in allowed_query_params
            value = query_params[param]
            validate_schema(value, operation.schema, raw_definitions=query_params_schemas[param])

    response = action(
        url=get_api_string(QDRANT_HOST, api, path_params),
        params=query_params,
        json=body
    )

    operation.validate_response(response)

    return response
