import pytest

from .helpers.collection_setup import drop_collection, multivec_collection_setup
from .helpers.helpers import request_with_validation

collection_name = 'test_collection'


@pytest.fixture(autouse=True)
def setup():
    multivec_collection_setup(collection_name=collection_name)
    yield
    drop_collection(collection_name=collection_name)


def test_delete_and_search():
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "points": [2, 3],
            "vector": ["text"]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/search',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "vector": {
                "name": "image",
                "vector": [0.0, 0.0, 1.0, 1.0]
            },
            "limit": 10,
            "with_payload": True,
            "with_vector": ["text"],
        }
    )

    assert response.ok


def test_retrieve_deleted_vector():
    # Delete vector
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "points": [1, 3],
            "vector": ["text"]
        }
    )
    assert response.ok

    # Retrieve deleted vector
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "ids": [1, 2, 3, 4],
            "with_vectors": ["text", "image"],
            "with_payload": True,
        }
    )

    assert response.ok
    result = response.json()["result"]
    assert len(result) == 4

    id_to_result = {r["id"]: r for r in result}

    assert id_to_result[1]["vector"].get("image") is not None
    assert id_to_result[1]["vector"].get("text") is None

    assert id_to_result[2]["vector"].get("image") is not None
    assert id_to_result[2]["vector"].get("text") is not None

    assert id_to_result[3]["vector"].get("image") is not None
    assert id_to_result[3]["vector"].get("text") is None

    assert id_to_result[4]["vector"].get("image") is not None
    assert id_to_result[4]["vector"].get("text") is not None


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
    POINT_ID = 1000

    # Put empty vector first
    response = request_with_validation(
        api='/collections/{collection_name}/points',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": POINT_ID,
                    "vector": {},
                    "payload": {"city": "Berlin"}
                }
            ]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/{id}',
        method="GET",
        path_params={'collection_name': collection_name, 'id': POINT_ID},
    )
    assert response.ok
    result = response.json()["result"]
    assert result["vector"].get("text") is None

    text_vector = [0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98]
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors',
        method="PUT",
        path_params={'collection_name': collection_name},
        query_params={'wait': 'true'},
        body={
            "points": [
                {
                    "id": POINT_ID,
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
        path_params={'collection_name': collection_name, 'id': POINT_ID},
    )
    assert response.ok
    result = response.json()["result"]
    assert result["vector"]["text"] == text_vector
    assert result["payload"]["city"] == "Berlin"

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
                    "id": POINT_ID,
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
        path_params={'collection_name': collection_name, 'id': POINT_ID},
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
                    "id": POINT_ID,
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
        path_params={'collection_name': collection_name, 'id': POINT_ID},
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
            "points": [1],
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


def update_vectors_unknown_point():
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
                        "text": [0.05, 0.61, 0.76, 0.74, 0.05, 0.61, 0.76, 0.74],
                    },
                    "payload": {"city": "Berlin"}
                },
                {
                    "id": 424242424242424242,
                    "vector": {
                        "image": [0.19, 0.81, 0.75, 0.11],
                    },
                    "payload": {"city": ["Berlin", "London"]}
                }
            ]
        }
    )
    assert not response.ok
    assert response.status_code == 404
    error = response.json()["status"]["error"]
    assert error == "Not found: No point with id 424242424242424242 found"


def test_update_vectors_unknown_point():
    update_vectors_unknown_point()


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
    assert error.__contains__(
        "Validation error in JSON body: [points[0].vector: must specify vectors to update for point]")


def test_no_vectors():
    no_vectors()


def delete_vectors():
    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "points": [1, 2],
            "vector": ["image"]
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}/points/vectors/delete',
        method="POST",
        path_params={'collection_name': collection_name},
        body={
            "points": [2, 3],
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
            "filter": {},
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
            "points": [1, 2],
            "vector": ["a"]
        }
    )

    assert not response.ok
    assert response.status_code == 400
    error = response.json()["status"]["error"]
    assert error.__contains__("Wrong input: Not existing vector name error: a")


def test_delete_unknown_vectors():
    delete_unknown_vectors()
