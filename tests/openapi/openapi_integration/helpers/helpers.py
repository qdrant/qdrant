from typing import Any, Dict, List
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
    jsonschema.validate(data, raw_definitions, cls=jsonschema.Draft7Validator, resolver=resolver)


def request_with_validation(
        api: str,
        method: str,
        path_params: dict = None,
        query_params: dict = None,
        body: dict = None
) -> requests.Response:
    operation: APIOperation = SCHEMA[api][method]

    assert isinstance(operation.schema, OpenApi30)

    if body:
        validate_schema(
            data=body,
            operation_schema=operation.schema,
            raw_definitions=operation.definition.raw['requestBody']['content']['application/json']['schema']
        )

    if path_params is None:
        path_params = {}
    if query_params is None:
        query_params = {}
    action = getattr(requests, method.lower(), None)

    for param in operation.path_parameters.items:
        if param.is_required:
            assert param.name in path_params

    for param in operation.query.items:
        if param.is_required:
            assert param.name in query_params

    for param in path_params.keys():
        assert param in set(p.name for p in operation.path_parameters.items)

    for param in query_params.keys():
        assert param in set(p.name for p in operation.query.items)

    if not action:
        raise RuntimeError(f"Method {method} does not exists")

    response = action(
        url=get_api_string(QDRANT_HOST, api, path_params),
        params=query_params,
        json=body
    )

    operation.validate_response(response)

    return response

# from client implementation:
# https://github.com/qdrant/qdrant-client/blob/d18cb1702f4cf8155766c7b32d1e4a68af11cd6a/qdrant_client/hybrid/fusion.py#L6C1-L31C25
def reciprocal_rank_fusion(
    responses: List[List[Any]], limit: int = 10
) -> List[Any]:
    def compute_score(pos: int) -> float:
        ranking_constant = (
            2  # the constant mitigates the impact of high rankings by outlier systems
        )
        return 1 / (ranking_constant + pos)

    scores: Dict[Any, float] = {} # id -> score
    point_pile = {}
    for response in responses:
        for i, scored_point in enumerate(response):
            if scored_point["id"] in scores:
                scores[scored_point["id"]] += compute_score(i)
            else:
                point_pile[scored_point["id"]] = scored_point
                scores[scored_point["id"]] = compute_score(i)

    sorted_scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    sorted_points = []
    for point_id, score in sorted_scores[:limit]:
        point = point_pile[point_id]
        point["score"] = score
        sorted_points.append(point)
    return sorted_points
