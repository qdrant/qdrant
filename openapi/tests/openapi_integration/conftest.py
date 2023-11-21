import pytest


@pytest.fixture(params=[False, True], scope="module")
def on_disk_vectors(request):
    return request.param


@pytest.fixture(params=[False, True], scope="module")
def on_disk_payload(request):
    return request.param
