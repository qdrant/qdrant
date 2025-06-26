import os
import shutil
import subprocess
import tarfile
import time
import uuid
import zipfile

import pytest
import docker
from docker.errors import ImageNotFound, NotFound
import requests
from pathlib import Path


@pytest.fixture(scope="session")
def docker_client():
    """Create a Docker client instance."""
    return docker.from_env()


@pytest.fixture(scope="session")
def qdrant_image(docker_client, request):
    """
    Build Qdrant Docker image once per test session.
    
    Can be used directly or with indirect parametrization:
    
    Direct usage:
        def test_something(qdrant_image):
            # Uses default tag "qdrant-recovery-test"
    
    Indirect parametrization:
        @pytest.mark.parametrize("qdrant_image", [
            {"tag": "custom-tag", "rebuild_image": True}
        ], indirect=True)
        def test_something(qdrant_image):
            # Uses custom tag and forces rebuild
    """
    # Get configuration from parametrization or use defaults
    if hasattr(request, "param") and isinstance(request.param, dict):
        config = request.param
    else:
        config = {}
    
    # Determine image tag
    image_tag = config.get("tag", "qdrant/qdrant:dev")
    rebuild_image = config.get("rebuild_image", False)
    
    project_root = Path(__file__).parent.parent.parent
    
    # Check if image already exists
    image_exists = False
    try:
        docker_client.images.get(image_tag)
        image_exists = True
        print(f"Docker image {image_tag} already exists")
    except ImageNotFound:
        print(f"Docker image {image_tag} not found, will build")
    
    # Build image if it doesn't exist or if rebuild is requested
    if not image_exists or rebuild_image:
        if rebuild_image and image_exists:
            print(f"Rebuilding Docker image {image_tag} (rebuild_image=True)...")
        else:
            print(f"Building Docker image {image_tag}...")
        
        # Build image using docker buildx
        build_cmd = [
            "docker", "buildx", "build",
            "--build-arg=PROFILE=ci",
            "--load",
            str(project_root),
            f"--tag={image_tag}"
        ]
        
        result = subprocess.run(build_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to build Docker image: {result.stderr}")
        
        print(f"Successfully built image {image_tag}")
    else:
        print(f"Using existing Docker image {image_tag}")
    
    return image_tag


def _get_default_qdrant_config(qdrant_image):
    """Get default configuration for Qdrant container."""
    return {
        "image": qdrant_image,
        "ports": {'6333/tcp': ('127.0.0.1', None), '6334/tcp': ('127.0.0.1', None)},
        "detach": True,
        "remove": True,
    }


def _extract_container_ports(container):
    """Extract HTTP and gRPC ports from container."""
    container.reload()
    http_port = container.attrs['NetworkSettings']['Ports']['6333/tcp'][0]['HostPort']
    grpc_port = container.attrs['NetworkSettings']['Ports']['6334/tcp'][0]['HostPort']
    return int(http_port), int(grpc_port)


def _create_container_info(container, http_port, grpc_port):
    """Create standardized container info dictionary."""
    return {
        "container": container,
        "host": "127.0.0.1",
        "name": container.name,
        "http_port": http_port,
        "grpc_port": grpc_port
    }


def _cleanup_container(container):
    """Clean up a Docker container."""
    try:
        container.reload()
        container.stop()
        if not container.attrs.get('HostConfig', {}).get('AutoRemove', True):
            container.remove(force=True)
            print(f"Removed container: {container.name}")
    except NotFound:
        print("Container already removed. OK.")
    except Exception as e:
        print(f"Error stopping container {container.name if hasattr(container, 'name') else 'unknown'}: {e}")


def _create_qdrant_container(docker_client, qdrant_image, config=None):
    """Core function to create a Qdrant container with given configuration."""
    if config is None:
        config = {}
    
    # Make a copy to avoid modifying the original
    config = dict(config)
    
    # Extract custom parameters
    exit_on_error = config.pop("exit_on_error", True)
    
    # Merge configurations
    default_config = _get_default_qdrant_config(qdrant_image)
    merged_config = {**default_config, **config}
    
    # Create container
    container = docker_client.containers.run(**merged_config)
    
    try:
        # Get ports
        http_port, grpc_port = _extract_container_ports(container)
        
        # Wait for readiness
        if not _wait_for_qdrant_ready(port=http_port, timeout=30):
            if exit_on_error:
                raise RuntimeError("Qdrant failed to start within 30 seconds")
        
        # Return container info
        return _create_container_info(container, http_port, grpc_port)
    except Exception:
        # Clean up on failure
        _cleanup_container(container)
        raise


def _extract_compose_container_info(container, project_name):
    """Extract container info from a docker-compose container."""
    container.reload()
    
    # Extract ports - compose might map them differently
    port_bindings = container.attrs['NetworkSettings']['Ports']
    
    # Find HTTP port (6333)
    http_port = None
    if '6333/tcp' in port_bindings and port_bindings['6333/tcp']:
        http_port = int(port_bindings['6333/tcp'][0]['HostPort'])
    else:
        # Look for any exposed HTTP port
        for port_key, bindings in port_bindings.items():
            if bindings and port_key.endswith('/tcp'):
                port_num = int(port_key.split('/')[0])
                if 6000 <= port_num <= 7000:  # Reasonable range for Qdrant
                    http_port = int(bindings[0]['HostPort'])
                    break
    
    if not http_port:
        raise RuntimeError(f"Could not find HTTP port mapping for container {container.name}")
    
    # Find gRPC port (6334) - optional
    grpc_port = None
    if '6334/tcp' in port_bindings and port_bindings['6334/tcp']:
        grpc_port = int(port_bindings['6334/tcp'][0]['HostPort'])
    
    return {
        "container": container,
        "host": "127.0.0.1",
        "name": container.name,
        "http_port": http_port,
        "grpc_port": grpc_port,
        "compose_project": project_name
    }


def _wait_for_qdrant_ready(port: int = 6333, timeout: int = 30) -> bool:
    """Wait for Qdrant service to be ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"http://localhost:{port}")
            if response.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
    return False


@pytest.fixture
def qdrant_container(docker_client, qdrant_image, request):
    """
    Fixture for creating Qdrant containers with different configurations.
    
    Can be used as a factory or with indirect parametrization:
    
    Factory usage (returns a callable):
        def test_something(qdrant_container):
            container_info = qdrant_container(mem_limit="128m", environment={...})
            
    Indirect parametrization (returns container info directly):
        @pytest.mark.parametrize("qdrant_container", [
            {"mem_limit": "256m", "environment": {"KEY": "value"}}
        ], indirect=True)
        def test_something(qdrant_container):
            # qdrant_container is already the container info dict
            host = qdrant_container["host"]
            port = qdrant_container["http_port"]
    
    Returns a dict with:
        - container: The Docker container object
        - host: The host address (always "127.0.0.1")
        - name: The container name
        - http_port: The HTTP API port (6333)
        - grpc_port: The gRPC API port (6334)
        
    Special parameters:
        - exit_on_error (bool): If True (default), raises RuntimeError when Qdrant fails to start.
                               If False, returns container info even if Qdrant doesn't start successfully.
    """
    containers = []
    
    def _create_container(**kwargs):
        container_info = _create_qdrant_container(docker_client, qdrant_image, kwargs)
        containers.append(container_info["container"])
        return container_info
    
    # Check if this is being used with indirect parametrization
    if hasattr(request, "param") and isinstance(request.param, dict):
        # Indirect parametrization mode - create container directly with provided config
        container_info = _create_container(**request.param)
        yield container_info
    else:
        # Factory mode - return the factory function
        yield _create_container
    
    # Cleanup all containers
    for container in containers:
        _cleanup_container(container)


@pytest.fixture
def qdrant(docker_client, qdrant_image, request):
    """
    Fixture that automatically creates a Qdrant container and returns its info.
    
    Direct usage (default configuration):
        def test_something(qdrant):
            # qdrant is already the container info dict
            host = qdrant["host"]
            port = qdrant["http_port"]
            
    With parametrization (custom configuration):
        @pytest.mark.parametrize("qdrant", [
            {"mem_limit": "256m", "environment": {"KEY": "value"}}
        ], indirect=True)
        def test_something(qdrant):
            # qdrant is the container info dict with custom config
            
    Returns a dict with:
        - container: The Docker container object
        - host: The host address (always "127.0.0.1")
        - name: The container name
        - http_port: The HTTP API port (6333)
        - grpc_port: The gRPC API port (6334)
    """
    # Get configuration from parametrization or use empty dict for defaults
    config = {}
    if hasattr(request, "param") and isinstance(request.param, dict):
        config = request.param
    
    container_info = _create_qdrant_container(docker_client, qdrant_image, config)
    
    try:
        yield container_info
    finally:
        # Cleanup
        _cleanup_container(container_info["container"])


@pytest.fixture
def qdrant_compose(docker_client, qdrant_image, request):
    """
    Fixture for creating Qdrant containers using docker-compose.yaml files.
    
    Usage with parametrization:
        @pytest.mark.parametrize("qdrant_compose", [
            {"compose_file": "path/to/docker-compose.yaml"}
        ], indirect=True)
        def test_something(qdrant_compose):
            # qdrant_compose is a dict with container info
            host = qdrant_compose["host"]
            port = qdrant_compose["http_port"]
            
        # With custom service name (if compose has multiple services):
        @pytest.mark.parametrize("qdrant_compose", [
            {"compose_file": "docker-compose.yaml", "service_name": "qdrant-node"}
        ], indirect=True)
        def test_something(qdrant_compose):
            # Uses specific service from compose file
            
    Returns a dict with:
        - container: The Docker container object
        - host: The host address (always "127.0.0.1")
        - name: The container name
        - http_port: The HTTP API port (6333)
        - grpc_port: The gRPC API port (6334)
        - compose_project: The docker-compose project name (for cleanup)
    """
    if not hasattr(request, "param") or not isinstance(request.param, dict):
        raise ValueError("qdrant_compose fixture requires parametrization with compose_file path")
    
    config = request.param
    compose_file = config.get("compose_file")
    if not compose_file:
        raise ValueError("compose_file parameter is required")
    
    # Convert to absolute path if relative
    compose_path = Path(compose_file)
    if not compose_path.is_absolute():
        # Resolve relative to the test file's directory
        test_dir = Path(__file__).parent
        compose_path = test_dir / compose_path
    
    if not compose_path.exists():
        raise FileNotFoundError(f"Docker compose file not found: {compose_path}")
    
    # Generate unique project name to avoid conflicts
    project_name = f"qdrant-test-{uuid.uuid4().hex[:8]}"
    service_name = config.get("service_name")  # None means return all services
    compose_cmd = None  # Initialize to handle cleanup in finally block
    
    try:
        # Try docker compose v2 first, then fall back to docker-compose v1
        compose_commands = [
            ["docker", "compose"],  # v2
            ["docker-compose"]      # v1
        ]
        
        compose_cmd = None
        for cmd_prefix in compose_commands:
            test_cmd = cmd_prefix + ["version"]
            result = subprocess.run(test_cmd, capture_output=True, text=True)
            if result.returncode == 0:
                compose_cmd = cmd_prefix
                break
        
        if not compose_cmd:
            raise RuntimeError("Neither 'docker compose' nor 'docker-compose' command found")
        
        # Get list of services from compose file
        services_cmd = compose_cmd + ["-f", str(compose_path), "config", "--services"]
        result = subprocess.run(services_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to get services from compose file: {result.stderr}")
        
        services = [s.strip() for s in result.stdout.strip().split('\n') if s.strip()]
        service_count = len(services)
        
        # Start the compose project with custom image override
        # Set environment variable to override the image in compose services
        env = dict(os.environ)
        env["QDRANT_IMAGE"] = qdrant_image
        
        compose_up_cmd = compose_cmd + [
            "-f", str(compose_path),
            "-p", project_name,
            "up", "-d"
        ]
        
        result = subprocess.run(compose_up_cmd, capture_output=True, text=True, env=env)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to start docker-compose: {result.stderr}")
        # Wait for ports to be assigned
        time.sleep(2)

        if service_count == 1:
            # Single service compose file - always return single object
            project_containers = docker_client.containers.list(filters={"name": project_name})
            if not project_containers:
                raise RuntimeError(f"No containers found for project: {project_name}")
            
            container = project_containers[0]
            container_info = _extract_compose_container_info(container, project_name)
            
            # Wait for this specific container to be ready
            if not _wait_for_qdrant_ready(port=container_info["http_port"], timeout=60):
                raise RuntimeError("Qdrant failed to start within 60 seconds")
            
        else:
            # Multiple services compose file
            if service_name:
                # Specific service requested - return single object
                container_name_prefix = f"{project_name}-{service_name}"
                containers = docker_client.containers.list(filters={"name": container_name_prefix})
                if not containers:
                    raise RuntimeError(f"No container found with prefix: {container_name_prefix}")
                
                container = containers[0]
                container_info = _extract_compose_container_info(container, project_name)
                
                # Wait for this specific container to be ready
                if not _wait_for_qdrant_ready(port=container_info["http_port"], timeout=60):
                    raise RuntimeError("Qdrant failed to start within 60 seconds")
                
            else:
                # No specific service - return array of all container info
                project_containers = docker_client.containers.list(filters={"name": project_name})
                if not project_containers:
                    raise RuntimeError(f"No containers found for project: {project_name}")
                
                container_infos = []
                for container in project_containers:
                    try:
                        container_info = _extract_compose_container_info(container, project_name)
                        container_infos.append(container_info)
                    except Exception as e:
                        print(f"Warning: Could not extract info for container {container.name}: {e}")
                        continue
                
                if not container_infos:
                    raise RuntimeError(f"No valid Qdrant containers found in project: {project_name}")
                
                # Wait for all containers to be ready
                for info in container_infos:
                    if not _wait_for_qdrant_ready(port=info["http_port"], timeout=60):
                        print(f"Warning: Container {info['name']} failed to start within 60 seconds")
                
                container_info = container_infos  # Return the array
        
        yield container_info
        
    finally:
        # Clean up the compose project using the same command that started it
        if compose_cmd:
            compose_down_cmd = compose_cmd + [
                "-f", str(compose_path),
                "-p", project_name,
                "down", "-v"  # Also remove volumes
            ]
            
            result = subprocess.run(compose_down_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Warning: Failed to stop docker-compose: {result.stderr}")
            else:
                print(f"Cleaned up compose project: {project_name}")
        else:
            print(f"Warning: Could not clean up compose project {project_name} - compose command not found")


@pytest.fixture
def qdrant_cluster(docker_client, qdrant_container, request):
    """
    Create a Qdrant cluster with 1 leader and configurable number of followers.
    
    Returns a dict with:
        - leader: Leader node info dict (container, host, name, http_port, grpc_port)
        - followers: List of follower node info dicts
        - all_nodes: List of all nodes (leader + followers)
        - network: Docker network object
    
    Usage:
        # Default: 1 leader + 2 followers
        def test_cluster(qdrant_cluster):
            cluster = qdrant_cluster
            
        # Custom number of followers
        @pytest.mark.parametrize("qdrant_cluster", [{"follower_count": 4}], indirect=True)
        def test_large_cluster(qdrant_cluster):
            cluster = qdrant_cluster  # 1 leader + 4 followers
    """
    config = {}
    if hasattr(request, "param") and isinstance(request.param, dict):
        config = request.param
    
    follower_count = config.get("follower_count", 2)
    test_id = str(uuid.uuid4())[:8]
    
    # Create a custom Docker network for cluster communication
    network_name = f"qdrant-cluster-{test_id}"
    network = docker_client.networks.create(network_name, driver="bridge")
    
    try:
        # Create leader node
        leader_name = f"qdrant-leader-{test_id}"
        leader_info = qdrant_container(
            name=leader_name,
            network=network_name,
            environment={
                "QDRANT__SERVICE__GRPC_PORT": "6334",
                "QDRANT__CLUSTER__ENABLED": "true",
                "QDRANT__CLUSTER__P2P__PORT": "6335",
            },
            command=["./qdrant", "--uri", f"http://{leader_name}:6335"],
            remove=False  # Keep for potential log inspection
        )
        
        # Wait for leader to be ready
        _wait_for_qdrant_ready(port=leader_info["http_port"], timeout=30)
        
        # Create follower nodes
        followers = []
        for i in range(follower_count):
            follower_name = f"qdrant-follower{i+1}-{test_id}"
            sleep_time = 3 + i  # Stagger startup times
            
            follower_info = qdrant_container(
                name=follower_name,
                network=network_name,
                environment={
                    "QDRANT__SERVICE__GRPC_PORT": "6334",
                    "QDRANT__CLUSTER__ENABLED": "true",
                    "QDRANT__CLUSTER__P2P__PORT": "6335",
                },
                command=[
                    "bash", "-c",
                    f"sleep {sleep_time} && ./qdrant --bootstrap 'http://{leader_name}:6335' --uri 'http://{follower_name}:6335'"
                ],
                remove=False
            )
            followers.append(follower_info)
        
        # Wait for all followers to be ready
        for follower_info in followers:
            _wait_for_qdrant_ready(port=follower_info["http_port"], timeout=60)
        
        # Give cluster time to stabilize
        time.sleep(5)
        
        # Verify cluster formation
        try:
            response = requests.get(
                f"http://{leader_info['host']}:{leader_info['http_port']}/cluster",
                timeout=10
            )
            if response.status_code == 200:
                cluster_info = response.json()
                peers = cluster_info.get("result", {}).get("peers", {})
                expected_nodes = 1 + follower_count
                if len(peers) != expected_nodes:
                    print(f"Warning: Expected {expected_nodes} nodes in cluster, found {len(peers)}")
            else:
                print(f"Warning: Could not verify cluster status: {response.status_code}")
        except Exception as e:
            print(f"Warning: Could not verify cluster: {e}")
        
        cluster_data = {
            "leader": leader_info,
            "followers": followers,
            "all_nodes": [leader_info] + followers,
            "network": network,
            "network_name": network_name
        }
        
        yield cluster_data
        
    finally:
        # Clean up the network
        try:
            network.remove()
            print(f"Cleaned up cluster network: {network_name}")
        except Exception as e:
            print(f"Error removing network {network_name}: {e}")


@pytest.fixture
def temp_storage_dir(request):
    """
    Create a temporary storage directory and ensure its removal after test.
    
    Usage:
        def test_something(temp_storage_dir):
            # Returns Path to a "storage" directory that will be cleaned up
    """
    # Use test name as folder name
    folder_name = request.node.name
    test_dir = Path(__file__).parent / folder_name
    storage_path = test_dir / "storage"
    
    try:
        storage_path.mkdir(parents=True, exist_ok=True)
        yield storage_path
    finally:
        if test_dir.exists():
            try:
                shutil.rmtree(test_dir)
                print(f"Cleaned up temp storage directory: {test_dir}")
            except Exception as e:
                print(f"Warning: Failed to remove temp storage directory {test_dir}: {e}")


@pytest.fixture
def storage_from_archive(request, temp_storage_dir):
    """
    Extract an archive from test_data directory into the storage directory.
    
    Usage with parametrization:
        @pytest.mark.parametrize("storage_with_archive", ["storage.tar.xz"], indirect=True)
        def test_something(storage_with_archive):
            # Archive will be extracted into the storage directory
    """
    if hasattr(request, "param"):
        archive_name = request.param
    else:
        raise ValueError("storage_with_archive fixture requires archive name via parametrization")
    
    # Path to test_data directory
    test_data_dir = Path(__file__).parent / "test_data"
    archive_file = test_data_dir / archive_name
    
    if not archive_file.exists():
        raise FileNotFoundError(f"Archive not found after git lfs pull: {archive_file}")
    
    # Get the parent directory of storage (where we want to extract)
    extract_to = temp_storage_dir.parent
    file_name = archive_file.name.lower()

    try:
        if file_name.endswith(('.tar.xz', '.tar.gz', '.tar.bz2', '.tgz', '.tbz2')):
            # Handle compressed tar files
            with tarfile.open(archive_file, 'r:*') as tar:
                tar.extractall(path=extract_to)
                print(f"Extracted {archive_file} to {extract_to}")
        elif file_name.endswith('.tar'):
            # Handle uncompressed tar files
            with tarfile.open(archive_file, 'r:') as tar:
                tar.extractall(path=extract_to)
                print(f"Extracted {archive_file} to {extract_to}")
        elif file_name.endswith('.zip'):
            # Handle zip files
            with zipfile.ZipFile(archive_file, 'r') as zip_file:
                zip_file.extractall(path=extract_to)
                print(f"Extracted {archive_file} to {extract_to}")
        else:
            raise ValueError(f"Unsupported archive format: {archive_file}")
    except Exception as e:
        print(f"Failed to extract archive {archive_file}: {e}")
        # Try fallback to subprocess for tar files
        if file_name.endswith(('.tar.xz', '.tar.gz', '.tar.bz2', '.tgz', '.tbz2', '.tar')):
            try:
                print(f"Trying fallback extraction with tar command...")
                subprocess.run(["tar", "-xf", str(archive_file)], cwd=str(extract_to), check=True)
                print(f"Successfully extracted {archive_file} using tar command")
            except subprocess.CalledProcessError as tar_error:
                raise RuntimeError(f"Failed to extract archive: {tar_error}")
        else:
            raise
    
    return temp_storage_dir
