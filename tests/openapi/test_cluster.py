from .helpers.helpers import request_with_validation


def test_cluster_recover_standalone_returns_405():
    """
    POST /cluster/recover on a standalone (non-distributed) node must return
    405 Method Not Allowed, not 500 Internal Server Error.

    See https://github.com/qdrant/qdrant/issues/9421
    """
    response = request_with_validation(
        api="/cluster/recover",
        method="POST",
    )
    assert response.status_code == 405
    assert "standalone mode" in response.json()["status"]["error"]


def test_remove_peer_standalone_returns_405():
    """
    DELETE /cluster/peer/{peer_id} on a standalone node must return
    405 Method Not Allowed, consistent with the other cluster endpoints.
    """
    response = request_with_validation(
        api="/cluster/peer/{peer_id}",
        method="DELETE",
        path_params={"peer_id": 1},
    )
    assert response.status_code == 405
    assert "standalone mode" in response.json()["status"]["error"]
