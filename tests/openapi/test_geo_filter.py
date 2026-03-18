import pytest

from .helpers.helpers import request_with_validation
from .helpers.collection_setup import geo_collection_setup, drop_collection


@pytest.fixture(autouse=True)
def setup(on_disk_vectors, collection_name):
    geo_collection_setup(collection_name=collection_name, on_disk_vectors=on_disk_vectors)
    yield
    drop_collection(collection_name=collection_name)


def test_geo_polygon_simple(collection_name):
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
                            "exterior": {
                                "points": [
                                    {"lon": 55.455868, "lat": 55.495862},
                                    {"lon": 86.455868, "lat": 55.495862},
                                    {"lon": 86.455868, "lat": 86.495862},
                                    {"lon": 55.455868, "lat": 86.495862},
                                    {"lon": 55.455868, "lat": 55.495862},
                                ]
                            },
                            "interiors": [],
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

    # interiors is optional
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
                            "exterior": {
                                "points": [
                                    {"lon": 55.455868, "lat": 55.495862},
                                    {"lon": 86.455868, "lat": 55.495862},
                                    {"lon": 86.455868, "lat": 86.495862},
                                    {"lon": 55.455868, "lat": 86.495862},
                                    {"lon": 55.455868, "lat": 55.495862},
                                ]
                            },
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
                            "exterior": {
                                "points": [
                                    {"lon": -70.0, "lat": -70.0},
                                    {"lon": 60.0, "lat": -70.0},
                                    {"lon": 60.0, "lat": 60.0},
                                    {"lon": -70.0, "lat": 60.0},
                                    {"lon": -70.0, "lat": -70.0},
                                ]
                            },
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
                            "exterior": {
                                    "points": [
                                        {"lon": -180.0, "lat": -90.0},
                                        {"lon": 180.0, "lat": -90.0},
                                        {"lon": 180.0, "lat": 90.0},
                                        {"lon": -180.0, "lat": 90.0},
                                        {"lon": -180.0, "lat": -90.0},
                                    ]
                            },
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


def test_geo_polygon_with_interiors(collection_name):
    # a polygon with interior
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
                            "exterior": {
                                    "points": [
                                        {"lon": -70.0, "lat": -70.0},
                                        {"lon": 60.0, "lat": -70.0},
                                        {"lon": 60.0, "lat": 60.0},
                                        {"lon": -70.0, "lat": 60.0},
                                        {"lon": -70.0, "lat": -70.0},
                                    ]
                            },
                            "interiors": [
                                {
                                    "points": [
                                        {"lon": -65.0, "lat": -65.0},
                                        {"lon": 0.0, "lat": -65.0},
                                        {"lon": 0.0, "lat": 0.0},
                                        {"lon": -65.0, "lat": 0.0},
                                        {"lon": -65.0, "lat": -65.0},
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
    assert len(json["result"]) == 1

    ids = [x["id"] for x in json["result"]]
    assert 1 in ids


def test_geo_polygon_invalid(collection_name):
    # invalid polygons should be rejected
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
                            "exterior": {
                                    "points": [
                                        {"lon": -180.0, "lat": -90.0},
                                        {"lon": 180.0, "lat": -90.0},
                                        {"lon": 180.0, "lat": 90.0},
                                        {"lon": -180.0, "lat": 90.0},
                                    ]
                            },
                        },
                    }
                ]
            },
        },
    )
    assert not response.ok

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
                            "exterior": {
                                    "points": [
                                        {"lon": -180.0, "lat": -90.0},
                                        {"lon": 180.0, "lat": -90.0},
                                        {"lon": -180.0, "lat": -90.0},
                                    ]
                            },
                        },
                    }
                ]
            },
        },
    )
    assert not response.ok


def test_geo_polygon_multiple(collection_name):
    # multiple polygons
    response = request_with_validation(
        api="/collections/{collection_name}/points/search",
        method="POST",
        path_params={"collection_name": collection_name},
        body={
            "vector": [0.2, 0.1, 0.9, 0.7],
            "limit": 3,
            "filter": {
                "should": [
                    {
                        "key": "location",
                        "geo_polygon": {
                            "exterior": {
                                "points": [
                                    {"lon": 55.0, "lat": 55.0},
                                    {"lon": 65.0, "lat": 55.0},
                                    {"lon": 65.0, "lat": 65.0},
                                    {"lon": 55.0, "lat": 65.0},
                                    {"lon": 55.0, "lat": 55.0},
                                ]
                            },
                        },
                    },
                    {
                        "key": "location",
                        "geo_polygon": {
                            "exterior": {
                                "points": [
                                    {"lon": 75.0, "lat": 75.0},
                                    {"lon": 85.0, "lat": 75.0},
                                    {"lon": 85.0, "lat": 85.0},
                                    {"lon": 75.0, "lat": 85.0},
                                    {"lon": 75.0, "lat": 75.0},
                                ]
                            },
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

    # multiple polygons no overlap
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
                            "exterior": {
                                "points": [
                                    {"lon": 55.0, "lat": 55.0},
                                    {"lon": 65.0, "lat": 55.0},
                                    {"lon": 65.0, "lat": 65.0},
                                    {"lon": 55.0, "lat": 65.0},
                                    {"lon": 55.0, "lat": 55.0},
                                ]
                            },
                        },
                    },
                    {
                        "key": "location",
                        "geo_polygon": {
                            "exterior": {
                                "points": [
                                    {"lon": 75.0, "lat": 75.0},
                                    {"lon": 85.0, "lat": 75.0},
                                    {"lon": 85.0, "lat": 85.0},
                                    {"lon": 75.0, "lat": 85.0},
                                    {"lon": 75.0, "lat": 75.0},
                                ]
                            },
                        },
                    }
                ]
            },
        },
    )
    assert response.ok

    json = response.json()
    assert len(json["result"]) == 0
