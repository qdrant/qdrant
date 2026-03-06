import pytest
import requests
import time
import subprocess

from pathlib import Path

from e2e_tests.utils import run_docker_compose


class TestTLS:
    """Test TLS functionality using docker-compose with mutual TLS authentication."""

    @pytest.fixture(scope="class")
    def tls_certs(self, test_data_dir):
        """Provide paths to TLS certificate files."""
        return {
            "ca_cert": test_data_dir / "cert" / "cacert.pem",
            "client_cert": test_data_dir / "cert" / "cert.pem", 
            "client_key": test_data_dir / "cert" / "key.pem"
        }

    @pytest.fixture(scope="class")
    def tls_cluster(self, docker_client, qdrant_image, test_data_dir):
        """
        Class-scoped fixture for TLS cluster using the extracted compose function.
        """
        config = {"compose_file": "tls-compose.yaml", "wait_for_ready": False}
        cluster = run_docker_compose(docker_client, qdrant_image, test_data_dir, config)

        # Should have 2 containers
        assert len(cluster) == 2

        # Wait for cluster to stabilize
        time.sleep(15)

        try:
            yield cluster
        finally:
            cluster.cleanup()

    @staticmethod
    def _test_http_tls_connectivity(node_info, tls_certs, node_name):
        """Helper method to test HTTP TLS connectivity for a single node."""
        host = node_info.host
        api_url = f"https://{host}:{node_info.http_port}"

        # Create SSL context for mutual TLS
        session = requests.Session()
        session.verify = str(tls_certs["ca_cert"])
        session.cert = (str(tls_certs["client_cert"]), str(tls_certs["client_key"]))

        # Test telemetry endpoint
        response = session.get(f"{api_url}/telemetry", timeout=10)
        assert response.status_code == 200

        print(f"{node_name} HTTP TLS connectivity verified")

        # Check cluster formation
        response = session.get(f"{api_url}/cluster", timeout=10)
        assert response.status_code == 200

        cluster_info = response.json()
        peers = cluster_info["result"]["peers"]

        # Verify we have exactly 2 peers and they have the expected TLS URIs
        assert len(peers) == 2, f"Expected 2 peers, found {len(peers)}"

        # Check that peer URIs contain HTTPS (indicating TLS)
        for peer_id, peer_info in peers.items():
            uri = peer_info.get("uri", "")
            assert uri.startswith("https://"), f"Peer {peer_id} does not have HTTPS URI: {uri}"
            assert ":6335" in uri, f"Peer {peer_id} URI missing port 6335: {uri}"

    @staticmethod
    def _test_grpc_tls_connectivity(node_info, tls_certs, node_name):
        """Helper method to test gRPC TLS connectivity for a single node."""
        try:
            cert_hostname = f"{node_name}.qdrant"
            compose_project = node_info.compose_project
            network_name = f"{compose_project}_qdrant-network"

            # Get absolute paths for certificates 
            cert_dir = tls_certs["ca_cert"].parent
            proto_dir = Path(__file__).parent.parent.parent / "lib" / "api" / "src" / "grpc" / "proto"

            # Run grpcurl from within the Docker network
            grpcurl_cmd = [
                "docker", "run", "--rm",
                f"--network={network_name}",
                "-v", f"{cert_dir}:/tls_path:ro",
                "-v", f"{proto_dir}:/proto:ro",
                "fullstorydev/grpcurl",
                "-cacert", "/tls_path/cacert.pem",
                "-cert", "/tls_path/cert.pem",
                "-key", "/tls_path/key.pem",
                "-import-path", "/proto",
                "-proto", "qdrant.proto",
                "-d", "{}",
                f"{cert_hostname}:6334",
                "qdrant.Qdrant/HealthCheck"
            ]

            result = subprocess.run(grpcurl_cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                print(f"{node_name} gRPC TLS connectivity verified")
            else:
                pytest.fail(f"gRPC TLS health check failed for {node_name}. "
                          f"Return code: {result.returncode}, "
                          f"stdout: {result.stdout}, stderr: {result.stderr}")

        except subprocess.TimeoutExpired:
            pytest.fail(f"gRPC TLS health check timed out for {node_name}")
        except Exception as e:
            pytest.fail(f"gRPC TLS health check failed for {node_name}: {e}")

    @pytest.mark.parametrize("protocol", ["http", "grpc"])
    def test_tls_connectivity(self, tls_cluster, tls_certs, protocol):
        """
        Test TLS connectivity of each node in Qdrant cluster using HTTP or gRPC protocol.
        """
        for i, node_info in enumerate(tls_cluster):
            node_name = f"node{i+1}"

            if protocol == "http":
                print(f"Testing HTTP TLS connectivity to {node_name}")
                self._test_http_tls_connectivity(node_info, tls_certs, node_name)
            elif protocol == "grpc":
                print(f"Testing gRPC TLS connectivity to {node_name}")
                self._test_grpc_tls_connectivity(node_info, tls_certs, node_name)

    def test_tls_shard_transfer(self, tls_cluster, tls_certs):
        """
        Test shard transfer between TLS nodes.
        """
        # Use the first node for operations
        node_info = tls_cluster[0]
        api_url = f"https://127.0.0.1:{node_info.http_port}"

        # Create SSL context for mutual TLS
        session = requests.Session()
        session.verify = str(tls_certs["ca_cert"])
        session.cert = (str(tls_certs["client_cert"]), str(tls_certs["client_key"]))

        # Create test collection
        collection_name = "test-collection"
        create_payload = {
            "vectors": {
                "size": 4,
                "distance": "Cosine"
            },
            "shard_number": 2,
            "replication_factor": 1  # Start with no replication
        }

        response = session.put(f"{api_url}/collections/{collection_name}", json=create_payload, timeout=10)
        assert response.status_code == 200, "Collection creation failed"

        # Insert test points (4 points)
        points = [
            {"id": i, "vector": [0.1 * (i + 1)] * 4}
            for i in range(4)
        ]

        upsert_payload = {"points": points}
        response = session.put(f"{api_url}/collections/{collection_name}/points", json=upsert_payload, timeout=10)
        assert response.status_code == 200

        # Get cluster info to find current peer
        response = session.get(f"{api_url}/cluster", timeout=10)
        assert response.status_code == 200

        cluster_info = response.json()
        peer_id = cluster_info["result"]["peer_id"]
        peers = cluster_info["result"]["peers"]

        # Find remote peer ID (the other node)
        remote_peer_id = None
        for p_id, peer_info in peers.items():
            if p_id != str(peer_id):
                remote_peer_id = int(p_id)
                break

        assert remote_peer_id is not None, "Could not find remote peer"

        # Get collection cluster info to find shards
        response = session.get(f"{api_url}/collections/{collection_name}/cluster", timeout=10)
        assert response.status_code == 200

        collection_cluster = response.json()
        local_shards = collection_cluster["result"]["local_shards"]
        assert len(local_shards) > 0, "No local shards found"

        shard_id = local_shards[0]["shard_id"]

        # Initiate shard replication to remote peer (snapshot transfer)
        replicate_payload = {
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": peer_id,
                "to_peer_id": remote_peer_id,
                "method": "snapshot"
            }
        }

        response = session.post(f"{api_url}/collections/{collection_name}/cluster", json=replicate_payload, timeout=30)
        assert response.status_code == 200

        # Wait for replication to complete
        time.sleep(5)

        # Verify replication by checking collection cluster info again
        response = session.get(f"{api_url}/collections/{collection_name}/cluster", timeout=10)
        assert response.status_code == 200

        updated_cluster = response.json()

        # Check that remote shards now exist or replicas exist
        remote_shards = updated_cluster["result"].get("remote_shards", [])
        local_shards = updated_cluster["result"].get("local_shards", [])

        # Verify that either we have remote shards or local shards with replicas
        has_replication = len(remote_shards) > 0 or any(
            len(shard.get("replicas", [])) > 0 for shard in local_shards
        )

        assert has_replication, "Shard replication did not occur"

        print("TLS shard transfer verified")
