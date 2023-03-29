import pytest

from .helpers.helpers import request_with_validation


def test_metrics():
    response = request_with_validation(
        api='/metrics',
        method="GET",
    )
    assert response.ok

    # Probe some strings that must exist in the metrics output
    assert '# HELP app_info information about qdrant server' in response.text
    assert '# TYPE app_info counter' in response.text
    assert 'app_info{name="qdrant",version="' in response.text
    assert 'collections_total ' in response.text
