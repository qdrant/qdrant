"""
This module contains examples demonstrating the usage of fixtures
"""
import docker
import pytest
import requests
import time
import uuid


class TestMultiContainer:
    def test_cluster_with_fixture(self, qdrant_cluster):
        """
        Example test using the qdrant_cluster fixture (default: 1 leader + 2 followers).
        
        This demonstrates how to use the simplified cluster fixture for testing
        cluster functionality without manual setup.
        """
        cluster = qdrant_cluster
        
        print(f"Cluster created with {len(cluster.all_nodes)} nodes:")
        print(f"  Leader: {cluster.leader.name} on port {cluster.leader.http_port}")
        for i, follower in enumerate(cluster.followers):
            print(f"  Follower {i+1}: {follower.name} on port {follower.http_port}")
        
        # Verify cluster is formed - check cluster info on leader
        response = requests.get(
            f"http://{cluster.leader.host}:{cluster.leader.http_port}/cluster"
        )
        assert response.status_code == 200
        
        cluster_info = response.json()
        peers = cluster_info.get("result", {}).get("peers", {})
        expected_nodes = len(cluster.all_nodes)
        assert len(peers) == expected_nodes, f"Expected {expected_nodes} peers, found {len(peers)}"
        
        # Create a collection on the leader
        collection_response = requests.put(
            f"http://{cluster.leader.host}:{cluster.leader.http_port}/collections/test-collection",
            json={
                "vectors": {
                    "size": 4,
                    "distance": "Cosine"
                }
            }
        )
        assert collection_response.status_code == 200
        
        # Wait for replication
        time.sleep(2)
        
        # Verify collection exists on all nodes
        for node_info in cluster.all_nodes:
            response = requests.get(
                f"http://{node_info.host}:{node_info.http_port}/collections/test-collection"
            )
            assert response.status_code == 200, f"Collection not found on node {node_info.name}"
        
        print("Cluster test with fixture passed!")
    
    @pytest.mark.parametrize("qdrant_cluster", [{"follower_count": 4}], indirect=True)
    def test_large_cluster(self, qdrant_cluster):
        """
        Example test with a larger cluster (1 leader + 4 followers).
        
        This shows how to customize the cluster size using parametrization.
        """
        cluster = qdrant_cluster
        
        # Verify we have the expected number of nodes
        assert len(cluster.all_nodes) == 5, f"Expected 5 nodes, got {len(cluster.all_nodes)}"
        assert len(cluster.followers) == 4, f"Expected 4 followers, got {len(cluster.followers)}"
        
        print(f"Large cluster created with {len(cluster.all_nodes)} nodes")
        
        # Test basic cluster functionality
        response = requests.get(
            f"http://{cluster.leader.host}:{cluster.leader.http_port}/cluster"
        )
        assert response.status_code == 200
        
        cluster_info = response.json()
        peers = cluster_info.get("result", {}).get("peers", {})
        assert len(peers) == 5, f"Expected 5 peers in cluster, found {len(peers)}"
        
        print("Large cluster test passed!")
    
    def test_three_node_cluster_manual(self, qdrant_container):
        """
        Example test that manually sets up 3 Qdrant containers in a cluster configuration.
        
        This is the original implementation kept for comparison with the fixture approach.
        Use the qdrant_cluster fixture instead unless you need custom cluster configuration.
        """
        # Create a unique test ID for this run
        test_id = str(uuid.uuid4())[:8]
        
        # Create a custom Docker network for the containers to communicate
        docker_client = docker.from_env()
        network_name = f"qdrant-test-network-{test_id}"
        network = docker_client.networks.create(network_name, driver="bridge")
        
        try:
            # Container 1: Leader node
            leader_info = qdrant_container(
                name=f"qdrant-leader-{test_id}",
                network=network_name,
                environment={
                    "QDRANT__SERVICE__GRPC_PORT": "6334",
                    "QDRANT__CLUSTER__ENABLED": "true",
                    "QDRANT__CLUSTER__P2P__PORT": "6335",
                },
                # Use custom command to set URI
                command=["./qdrant", "--uri", f"http://qdrant-leader-{test_id}:6335"],
            )
            
            # Container 2: Follower node 1
            follower1_info = qdrant_container(
                name=f"qdrant-follower1-{test_id}",
                network=network_name,
                environment={
                    "QDRANT__SERVICE__GRPC_PORT": "6334",
                    "QDRANT__CLUSTER__ENABLED": "true",
                    "QDRANT__CLUSTER__P2P__PORT": "6335",
                },
                # Bootstrap from leader
                command=[
                    "bash", "-c",
                    f"sleep 3 && ./qdrant --bootstrap 'http://qdrant-leader-{test_id}:6335' --uri 'http://qdrant-follower1-{test_id}:6335'"
                ],
            )
            
            # Container 3: Follower node 2
            follower2_info = qdrant_container(
                name=f"qdrant-follower2-{test_id}",
                network=network_name,
                environment={
                    "QDRANT__SERVICE__GRPC_PORT": "6334",
                    "QDRANT__CLUSTER__ENABLED": "true",
                    "QDRANT__CLUSTER__P2P__PORT": "6335",
                },
                # Bootstrap from leader with slight delay
                command=[
                    "bash", "-c",
                    f"sleep 4 && ./qdrant --bootstrap 'http://qdrant-leader-{test_id}:6335' --uri 'http://qdrant-follower2-{test_id}:6335'"
                ],
            )
            
            # Give cluster time to stabilize
            time.sleep(8)
            
            # Verify cluster is formed - check cluster info on leader
            response = requests.get(
                f"http://{leader_info.host}:{leader_info.http_port}/cluster"
            )
            assert response.status_code == 200
            
            cluster_info = response.json()
            print(f"Manual cluster info: {cluster_info}")
            
            # Verify we have 3 peers in the cluster
            peers = cluster_info.get("result", {}).get("peers", {})
            assert len(peers) == 3, f"Expected 3 peers, found {len(peers)}"
            
            print("Manual 3-node cluster test passed!")
            
        finally:
            # Clean up the network
            try:
                network.remove()
            except Exception as e:
                print(f"Error removing network: {e}")


class TestComposeExample:
    """Example tests demonstrating docker-compose fixture usage."""

    @pytest.mark.parametrize("qdrant_compose", [
        {"compose_file": "docker-compose.yaml"}
    ], indirect=True)
    def test_qdrant_compose_basic(self, qdrant_compose):
        """
        Test that creates a Qdrant container using docker-compose.yaml file.
        """
        # qdrant_compose returns the same structure as other fixtures
        api_url = f"http://{qdrant_compose.host}:{qdrant_compose.http_port}"

        # Test basic connectivity
        response = requests.get(f"{api_url}/collections")
        assert response.status_code == 200

        data = response.json()
        assert "result" in data
        assert "collections" in data["result"]

        # Additional info available
        print(f"Running with compose project: {qdrant_compose.compose_project}")

    @pytest.mark.parametrize("qdrant_compose", [
        {"compose_file": "multi-service-compose.yaml", "service_name": "qdrant-node1"}
    ], indirect=True)
    def test_qdrant_compose_with_service_name(self, qdrant_compose):
        """
        Test using a specific service from a multi-service docker-compose file.
        """
        api_url = f"http://{qdrant_compose.host}:{qdrant_compose.http_port}"

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
        {"compose_file": "docker-compose.yaml"}  # Single service
    ], indirect=True)
    def test_single_service_compose_returns_object(self, qdrant_compose):
        """
        Test that a single-service compose file always returns a single object,
        regardless of whether service_name is provided.
        """
        # Test connectivity
        api_url = f"http://{qdrant_compose.host}:{qdrant_compose.http_port}"
        response = requests.get(f"{api_url}/collections")
        assert response.status_code == 200

    @pytest.mark.parametrize("qdrant_compose", [
        {"compose_file": "multi-service-compose.yaml"}  # No service_name
    ], indirect=True)
    def test_multi_service_without_service_name_returns_array(self, qdrant_compose):
        """
        Test that a multi-service compose file without service_name returns an array.
        """
        # Should be a list of dicts
        assert isinstance(qdrant_compose, list)
        assert len(qdrant_compose) == 3  # Should have 3 nodes

        for container_info in qdrant_compose:
            # Test connectivity to each node
            api_url = f"http://{container_info.host}:{container_info.http_port}"
            response = requests.get(f"{api_url}/collections")
            assert response.status_code == 200


class TestContainerExample:
    """Simple example test demonstrating container setup and API calls."""

    def test_qdrant_fixture_defaults(self, qdrant):
        """
        Test that spins up a container with default configuration,
        using the new 'qdrant' fixture.
        """
        api_url = f"http://{qdrant.host}:{qdrant.http_port}"

        response = requests.get(f"{api_url}/collections")
        assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"

    @pytest.mark.parametrize("qdrant", [
        {"mem_limit": "256m", "environment": {"QDRANT__LOG_LEVEL": "DEBUG"}}
    ], indirect=True)
    def test_qdrant_fixture_with_custom_config(self, qdrant):
        """
        Test with custom container configuration using indirect parametrization.
        """
        api_url = f"http://{qdrant.host}:{qdrant.http_port}"

        response = requests.get(f"{api_url}/collections")
        assert response.status_code == 200

    def test_factory_qdrant_container_fixture_defaults(self, qdrant_container):
        """
        Test that the factory pattern works for multiple containers.
        """
        container1 = qdrant_container()
        container2 = qdrant_container()

        for container in [container1, container2]:
            api_url = f"http://{container.host}:{container.http_port}"
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
            api_url = f"http://{container.host}:{container.http_port}"
            response = requests.get(f"{api_url}/collections")
            assert response.status_code == 200
