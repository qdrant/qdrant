import random

import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import geo_collection_setup, drop_collection

collection_name = "test_collection_filter"


@pytest.fixture(autouse=True, scope="module")
def setup():
    geo_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_geo_polygon():
    response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 3,
            "filter": {
                "must": [
                    {
                        "key": "location",
                        "geo_polygon": {
                            "rings": [
                                {
                                    "points": [
                                        {"lon": 55.455868, "lat": 55.495862},
                                        {"lon": 86.455868, "lat": 55.495862},
                                        {"lon": 86.455868, "lat": 86.495862},
                                        {"lon": 55.455868, "lat": 86.495862},
                                        {"lon": 55.455868, "lat": 55.495862},
                                    ]
                                }
                            ]
                        },
                    }
                ]
            },
        },
    )
    assert response.ok

    json = response.json()
    assert len(json["result"]) == 2

    ids = [x["id"] for x in json["result"]]
    assert 2 in ids
    assert 4 in ids

    # a polygon spans from negative to positive
    response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 3,
            "filter": {
                "must": [
                    {
                        "key": "location",
                        "geo_polygon": {
                            "rings": [
                                {
                                    "points": [
                                        {"lon": -70.0, "lat": -70.0},
                                        {"lon": 60.0, "lat": -70.0},
                                        {"lon": 60.0, "lat": 60.0},
                                        {"lon": -70.0, "lat": 60.0},
                                        {"lon": -70.0, "lat": -70.0},
                                    ]
                                }
                            ]
                        },
                    }
                ]
            },
        },
    )
    assert response.ok

    json = response.json()
    assert len(json["result"]) == 2

    ids = [x["id"] for x in json["result"]]
    assert 1 in ids
    assert 3 in ids

    # a polygon cover all geo space lat[-90, -90], lon[-180, 180]
    response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 3,
            "filter": {
                "must": [
                    {
                        "key": "location",
                        "geo_polygon": {
                            "rings": [
                                {
                                    "points": [
                                        {"lon": -180.0, "lat": -90.0},
                                        {"lon": 180.0, "lat": -90.0},
                                        {"lon": 180.0, "lat": 90.0},
                                        {"lon": -180.0, "lat": 90.0},
                                        {"lon": -180.0, "lat": -90.0},
                                    ]
                                }
                            ]
                        },
                    }
                ]
            },
        },
    )
    assert response.ok

    json = response.json()
    assert len(json["result"]) == 3

    ids = [x["id"] for x in json["result"]]
    assert 1 in ids
    assert 3 in ids
    assert 4 in ids
