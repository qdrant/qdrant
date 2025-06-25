import pytest
import requests


class TestContainerExample:
    """Simple example test demonstrating container setup and API calls."""
    
    def test_qdrant_fixture_defaults(self, qdrant):
        """
        Test that spins up a container with default configuration,
        using the new 'qdrant' fixture.
        """
        api_url = f"http://{qdrant['host']}:{qdrant['http_port']}"
        
        response = requests.get(f"{api_url}/collections")
        assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"

    @pytest.mark.parametrize("qdrant", [
        {"mem_limit": "256m", "environment": {"QDRANT__LOG_LEVEL": "DEBUG"}}
    ], indirect=True)
    def test_qdrant_fixture_with_custom_config(self, qdrant):
        """
        Test with custom container configuration using indirect parametrization.
        """
        api_url = f"http://{qdrant['host']}:{qdrant['http_port']}"
        
        response = requests.get(f"{api_url}/collections")
        assert response.status_code == 200
        
    def test_factory_qdrant_container_fixture_defaults(self, qdrant_container):
        """
        Test that the factory pattern works for multiple containers.
        """
        container1 = qdrant_container()
        container2 = qdrant_container()
        
        for container in [container1, container2]:
            api_url = f"http://{container['host']}:{container['http_port']}"
            response = requests.get(f"{api_url}/collections")
            assert response.status_code == 200

    def test_factory_qdrant_container_fixture_with_custom_config(self, qdrant_container):
        """
        Test that the factory pattern works for multiple containers.
        """
        container1 = qdrant_container(
            name=f"qdrant-test-1",
            environment={"QDRANT__LOG_LEVEL": "DEBUG"}
        )
        container2 = qdrant_container(
            name=f"qdrant-test-2",
            environment={"QDRANT__STORAGE__HANDLE_COLLECTION_LOAD_ERRORS": "true"}
        )

        for container in [container1, container2]:
            api_url = f"http://{container['host']}:{container['http_port']}"
            response = requests.get(f"{api_url}/collections")
            assert response.status_code == 200
