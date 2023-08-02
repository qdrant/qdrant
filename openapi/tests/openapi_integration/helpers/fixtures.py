import pytest


@pytest.fixture(params=[False, True], scope="module")
def on_disk_vectors(request):
    return request.param
