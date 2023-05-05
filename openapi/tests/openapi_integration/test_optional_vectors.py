import pytest

from .helpers.collection_setup import drop_collection, multivec_collection_setup
from .helpers.helpers import request_with_validation

collection_name = 'test_collection'


@pytest.fixture(autouse=True)
def setup():
    multivec_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def upsert_partial_vectors():
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 101,
                    "vector": {
                        "text": [0.05, 0.61, 0.76, 0.74, 0.05, 0.61, 0.76, 0.74],
                    },
                    "payload": {"city": "Berlin"}
                },
                {
                    "id": 102,
                    "vector": {
                        "image": [0.19, 0.81, 0.75, 0.11],
                    },
                    "payload": {"city": ["Berlin", "London"]}
                },
                {
                    "id": 103,
                    "vector": {},
                    "payload": {"city": ["Berlin", "Moscow"]}
                },
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 101},
    )
    assert response.ok
    result = response.json()["result"]
    assert result["id"] == 101
    assert "image" not in result["vector"]
    assert "text" in result["vector"]

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 102},
    )
    assert response.ok
    result = response.json()["result"]
    assert result["id"] == 102
    assert "image" in result["vector"]
    assert "text" not in result["vector"]

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 103},
    )
    assert response.ok
    result = response.json()["result"]
    assert result["id"] == 103
    assert "image" not in result["vector"]
    assert "text" not in result["vector"]


def test_upsert_partial_vectors():
    upsert_partial_vectors()


def update_vectors():
    text_vector = [0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98]
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {
                        "text": text_vector,
                    }
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 1},
    )
    assert response.ok
    result = response.json()["result"]
    assert result["vector"]["text"] == text_vector

    text_vector = [0.12, 0.34, 0.56, 0.78, 0.90, 0.12, 0.34, 0.56]
    image_vector = [0.19, 0.28, 0.37, 0.46]
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {
                        "text": text_vector,
                        "image": image_vector,
                    }
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 1},
    )
    assert response.ok
    result = response.json()["result"]
    assert result["vector"]["image"] == image_vector
    assert result["vector"]["text"] == text_vector

    image_vector = [0.00, 0.01, 0.00, 0.01]
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {
                        "image": image_vector,
                    }
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 1},
    )
    assert response.ok
    result = response.json()["result"]
    assert result["vector"]["image"] == image_vector
    assert result["vector"]["text"] == text_vector


def test_update_vectors():
    update_vectors()


def update_empty_vectors():
    """
    Remove all named vectors for a point. Then add named vectors and test
    against it.
    """

    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "point_selector": {"points": [1]},
            "vector": ["image", "text"]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 1},
    )
    assert response.ok
    result = response.json()["result"]
    assert "text" not in result["vector"]
    assert "image" not in result["vector"]

    text_vector = [0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98]
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {
                        "text": text_vector,
                    }
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 1},
    )
    assert response.ok
    result = response.json()["result"]
    assert result["vector"]["text"] == text_vector
    assert "image" not in result["vector"]

    image_vector = [0.19, 0.28, 0.37, 0.46]
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {
                        "image": image_vector,
                    }
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 1},
    )
    assert response.ok
    result = response.json()["result"]
    assert result["vector"]["image"] == image_vector
    assert result["vector"]["text"] == text_vector


def test_update_empty_vectors():
    update_empty_vectors()


def no_vectors():
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": 1,
                    "vector": {},
                }
            ]
        }
    )
    assert not response.ok
    assert response.status_code == 422
    error = response.json()["status"]["error"]
    assert error.__contains__("Validation error in JSON body: [points[0].vector: must specify vectors to update for point]")


def test_no_vectors():
    no_vectors()


def delete_vectors():
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "point_selector": {"points": [1, 2]},
            "vector": ["image"]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "point_selector": {"points": [2, 3]},
            "vector": ["text"]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 1},
    )
    assert response.ok
    result = response.json()["result"]
    assert "image" not in result["vector"]
    assert "text" in result["vector"]

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 2},
    )
    assert response.ok
    result = response.json()["result"]
    assert "image" not in result["vector"]
    assert "text" not in result["vector"]

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': 3},
    )
    assert response.ok
    result = response.json()["result"]
    assert "image" in result["vector"]
    assert "text" not in result["vector"]


def test_delete_vectors():
    delete_vectors()
    # Deleting a second time should work fine
    delete_vectors()


def delete_all_vectors():
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "point_selector": {"filter": {}},
            "vector": ["image", "text"]
        }
    )
    assert response.ok


def test_delete_all_vectors():
    delete_all_vectors()


def delete_unknown_vectors():
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "point_selector": {"points": [1, 2]},
            "vector": ["a"]
        }
    )

    assert not response.ok
    assert response.status_code == 400
    error = response.json()["status"]["error"]
    assert error.__contains__("Wrong input: Not existing vector name error: a")


def test_delete_unknown_vectors():
    delete_unknown_vectors()
