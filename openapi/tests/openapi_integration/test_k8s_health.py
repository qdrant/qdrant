import json

import pytest

from .helpers.helpers import request_with_validation


@pytest.mark.parametrize("endpoint", ["/healthz", "/livez", "/readyz"])
def test_k8s_health(endpoint):
    response = request_with_validation(
        api=endpoint,
        method="GET",
    )
    assert response.ok
    assert response.text == "healthz check passed"
