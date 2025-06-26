import pytest
import requests


class TestComposeExample:
    """Example tests demonstrating docker-compose fixture usage."""
    
    @pytest.mark.parametrize("qdrant_compose", [
        {"compose_file": "test_data/docker-compose.yaml"}
    ], indirect=True)
    def test_qdrant_compose_basic(self, qdrant_compose):
        """
        Test that creates a Qdrant container using docker-compose.yaml file.
        """
        # qdrant_compose returns the same structure as other fixtures
        api_url = f"http://{qdrant_compose['host']}:{qdrant_compose['http_port']}"
        
        # Test basic connectivity
        response = requests.get(f"{api_url}/collections")
        assert response.status_code == 200
        
        data = response.json()
        assert "result" in data
        assert "collections" in data["result"]
        
        # Additional info available
        assert "compose_project" in qdrant_compose
        print(f"Running with compose project: {qdrant_compose['compose_project']}")
    
    @pytest.mark.parametrize("qdrant_compose", [
        {"compose_file": "test_data/multi-service-compose.yaml", "service_name": "qdrant-node1"}
    ], indirect=True)
    def test_qdrant_compose_with_service_name(self, qdrant_compose):
        """
        Test using a specific service from a multi-service docker-compose file.
        """
        api_url = f"http://{qdrant_compose['host']}:{qdrant_compose['http_port']}"
        
        # Create a collection
        collection_name = "compose_test_collection"
        create_response = requests.put(
            f"{api_url}/collections/{collection_name}",
            json={
                "vectors": {
                    "size": 128,
                    "distance": "Cosine"
                }
            }
        )
        
        assert create_response.status_code == 200
        
        # Verify collection exists
        list_response = requests.get(f"{api_url}/collections")
        assert list_response.status_code == 200
        
        collections = list_response.json()["result"]["collections"]
        collection_names = [col["name"] for col in collections]
        assert collection_name in collection_names

    @pytest.mark.parametrize("qdrant_compose", [
        {"compose_file": "test_data/docker-compose.yaml"}  # Single service
    ], indirect=True)
    def test_single_service_compose_returns_object(self, qdrant_compose):
        """
        Test that a single-service compose file always returns a single object,
        regardless of whether service_name is provided.
        """
        # Should be a dict, not a list
        assert isinstance(qdrant_compose, dict)
        assert "http_port" in qdrant_compose
        assert "host" in qdrant_compose
        
        # Test connectivity
        api_url = f"http://{qdrant_compose['host']}:{qdrant_compose['http_port']}"
        response = requests.get(f"{api_url}/collections")
        assert response.status_code == 200

    @pytest.mark.parametrize("qdrant_compose", [
        {"compose_file": "test_data/multi-service-compose.yaml"}  # No service_name
    ], indirect=True)
    def test_multi_service_without_service_name_returns_array(self, qdrant_compose):
        """
        Test that a multi-service compose file without service_name returns an array.
        """
        # Should be a list of dicts
        assert isinstance(qdrant_compose, list)
        assert len(qdrant_compose) == 3  # Should have 3 nodes
        
        for container_info in qdrant_compose:
            assert isinstance(container_info, dict)
            assert "http_port" in container_info
            assert "host" in container_info
            
            # Test connectivity to each node
            api_url = f"http://{container_info['host']}:{container_info['http_port']}"
            response = requests.get(f"{api_url}/collections")
            assert response.status_code == 200
