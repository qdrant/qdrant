import pytest
from consensus_tests.auth_tests.utils import start_jwt_protected_cluster
from consensus_tests.utils import kill_all_processes


@pytest.fixture(scope="module")
def jwt_cluster(tmp_path_factory: pytest.TempPathFactory):
    tmp_path = tmp_path_factory.mktemp("api_key_instance")

    peer_api_uris, peer_dirs, bootstrap_uri = start_jwt_protected_cluster(tmp_path)

    yield peer_api_uris, peer_dirs, bootstrap_uri

    kill_all_processes()
