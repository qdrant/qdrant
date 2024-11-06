import pytest
import jsons
from pytest_cases import parametrize, fixture_ref

from .helpers.collection_setup import drop_collection
from .helpers.helpers import request_with_validation


@pytest.fixture(scope='module', autouse=True)
def source_collection_name(collection_name):
    return f"source_{collection_name}"


@pytest.fixture(scope='module', autouse=True)
def set_serializer(source_collection_name):
    def custom_serializer(obj: fixture_ref, **kwargs) -> str:
        return source_collection_name
    jsons.set_serializer(custom_serializer, fixture_ref)


@pytest.fixture(autouse=True, scope="module")
def setup(collection_name):
    yield
    drop_collection(collection_name=collection_name)
    drop_collection(collection_name=source_collection_name)


# Delete and create collection
def advanced_collection_setup(collection_name, size, distance, on_disk_vectors, on_disk_payload):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": size,
                "distance": distance,
                "on_disk": on_disk_vectors,
            },
            "on_disk_payload": on_disk_payload
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok


# Delete and create collection
def advanced_collection_multi_setup(collection_name, vectors_config):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": vectors_config
        }
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="GET",
        path_params={'collection_name': collection_name},
    )
    assert response.ok


def create_from_collection(collection_name, source_collection_name, size, distance, on_disk_vectors, on_disk_payload):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": {
                "size": size,
                "distance": distance,
                "on_disk": on_disk_vectors,
            },
            "on_disk_payload": on_disk_payload,
            "init_from": {
                "collection": jsons.load(jsons.dump(source_collection_name))
            }
        }
    )
    return response


def create_multi_from_collection(collection_name, source_collection_name, vectors_config):
    response = request_with_validation(
        api='/collections/{collection_name}',
        method="DELETE",
        path_params={'collection_name': collection_name},
    )
    assert response.ok

    response = request_with_validation(
        api='/collections/{collection_name}',
        method="PUT",
        path_params={'collection_name': collection_name},
        body={
            "vectors": vectors_config,
            "init_from": {
                "collection": source_collection_name
            }
        }
    )
    return response


@pytest.mark.parametrize("ok,source,size,distance", [
    (True, fixture_ref(source_collection_name), 4, 'Dot'), # ok
    (False, "i-do-not-exist", 4, 'Dot'), # fail: nonexistent source collection
    (False, fixture_ref(source_collection_name), 8, 'Dot'), # fail: bad size
    (False, fixture_ref(source_collection_name), 4, 'Cosine'), # fail: bad distance
])
def test_init_from_collection(
    collection_name,
    source_collection_name,
    ok,
    source,
    size,
    distance,
    on_disk_vectors,
    on_disk_payload,
):
    advanced_collection_setup(source_collection_name, 4, 'Dot', on_disk_vectors, on_disk_payload)
    response = create_from_collection(collection_name, source, size, distance, on_disk_vectors, on_disk_payload)
    assert response.ok == ok


def test_init_from_collection_multivec(on_disk_vectors, collection_name, source_collection_name):
    config = {
        "image": {
            "size": 4,
            "distance": "Dot",
            "on_disk": on_disk_vectors,
        },
        "audio": {
            "size": 8,
            "distance": "Dot",
            "on_disk": on_disk_vectors,
        }
    }
    # test successful init from collection
    advanced_collection_multi_setup(source_collection_name, config)
    response = create_multi_from_collection(collection_name, source_collection_name, config)
    assert response.ok

    larger_config = {
        "image": {
            "size": 4,
            "distance": "Dot",
            "on_disk": on_disk_vectors,
        },
        "audio": {
            "size": 8,
            "distance": "Dot",
            "on_disk": on_disk_vectors,
        },
        "video": {  # new vector
            "size": 16,
            "distance": "Dot",
            "on_disk": on_disk_vectors,
        }
    }
    # test successful init from collection (source is subset from target)
    advanced_collection_multi_setup(source_collection_name, config)
    response = create_multi_from_collection(collection_name, source_collection_name, larger_config)
    assert response.ok

    larger_config = {
        "image": {
            "size": 4,
            "distance": "Dot",
            "on_disk": on_disk_vectors,
        },
        "audio": {
            "size": 9,  # bad size
            "distance": "Dot",
            "on_disk": on_disk_vectors,
        },
        "video": {
            "size": 16,
            "distance": "Dot",
            "on_disk": on_disk_vectors,
        }
    }
    # test failed init from collection (target changes size of existing vector)
    advanced_collection_multi_setup(source_collection_name, config)
    response = create_multi_from_collection(collection_name, source_collection_name, larger_config)
    assert not response.ok
