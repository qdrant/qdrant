import pytest

from .helpers.collection_setup import basic_collection_setup, drop_collection
from .test_basic_retrieve_api import points_retrieve, exclude_payload, is_empty_condition, \
    recommendation, query_nested

collection_name = 'test_collection'


@pytest.fixture(autouse=True, scope="module")
def setup():
    basic_collection_setup(collection_name=collection_name, on_disk_payload=True)
    yield
    drop_collection(collection_name=collection_name)


def test_points_retrieve_on_disk():
    points_retrieve()


def test_exclude_payload_on_disk():
    exclude_payload()


def test_is_empty_condition_on_disk():
    is_empty_condition()


def test_recommendation_on_disk():
    recommendation()


def test_query_nested_on_disk():
    query_nested()
