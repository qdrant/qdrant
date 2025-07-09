import os
import shutil
import subprocess
import tarfile
import time
import uuid
import zipfile
from typing import Optional, List, Dict, Any, Tuple, Generator

import pytest
import docker
import docker.models.containers
import docker.models.networks
from docker.errors import ImageNotFound, NotFound
import requests
from pathlib import Path

from resource_tests.utils import wait_for_qdrant_ready


class QdrantContainer:
    """Container info object for Qdrant containers."""
    
    def __init__(self, container: docker.models.containers.Container, host: str, name: str, 
                 http_port: int, grpc_port: Optional[int], compose_project: Optional[str] = None) -> None:
        self.container = container
        self.host = host
        self.name = name
        self.http_port = http_port
        self.grpc_port = grpc_port
        self.compose_project = compose_project
    
    def __repr__(self) -> str:
        return f"QdrantContainer(name='{self.name}', host='{self.host}', http_port={self.http_port}, grpc_port={self.grpc_port})"


class QdrantCluster:
    """Cluster info object for Qdrant clusters."""
    
    def __init__(self, leader: QdrantContainer, followers: List[QdrantContainer], 
                 network: docker.models.networks.Network, network_name: str) -> None:
        self.leader = leader
        self.followers = followers
        self.all_nodes = [leader] + followers
        self.network = network
        self.network_name = network_name
    
    def __repr__(self) -> str:
        return f"QdrantCluster(leader={self.leader.name}, followers={len(self.followers)}, network='{self.network_name}')"


def _get_default_qdrant_config(qdrant_image: str) -> Dict[str, Any]:
    """Get default configuration for Qdrant container.
    
    Args:
        qdrant_image: The Qdrant Docker image to use
        
    Returns:
        dict: Default container configuration with image, ports, detach, and remove settings
    """
    return {
        "image": qdrant_image,
        "ports": {'6333/tcp': ('127.0.0.1', None), '6334/tcp': ('127.0.0.1', None)},
        "detach": True,
        "remove": True,
    }


def _extract_container_ports(container: docker.models.containers.Container) -> Tuple[int, int]:
    """Extract HTTP and gRPC ports from container.
    For host network mode, returns standard Qdrant ports (6333, 6334).
    For bridge/custom networks, extracts mapped ports from container attributes.
    
    Args:
        container: Docker container object
        
    Returns:
        tuple: (http_port, grpc_port) - extracted port numbers
    """
    container.reload()
    
    if container.attrs.get('HostConfig', {}).get('NetworkMode') == 'host':
        # For host network mode, use standard Qdrant ports
        return 6333, 6334
    
    # For bridge/custom networks, extract mapped ports
    http_port = container.attrs['NetworkSettings']['Ports']['6333/tcp'][0]['HostPort']
    grpc_port = container.attrs['NetworkSettings']['Ports']['6334/tcp'][0]['HostPort']
    return int(http_port), int(grpc_port)


def _create_container_info(container: docker.models.containers.Container, http_port: int, grpc_port: int) -> QdrantContainer:
    """Create standardized container info object.
    
    Args:
        container: Docker container object
        http_port: HTTP API port number
        grpc_port: gRPC API port number
        
    Returns:
        QdrantContainer: Container info object with container, host, name, http_port, and grpc_port attributes
    """
    container.reload()
    if container.attrs.get('HostConfig', {}).get('NetworkMode') == 'host':
        host = "localhost"
    else:
        host = "127.0.0.1"
    
    return QdrantContainer(
        container=container,
        host=host,
        name=container.name,
        http_port=http_port,
        grpc_port=grpc_port
    )


def _cleanup_container(container: docker.models.containers.Container) -> None:
    """Clean up a Docker container.
    Stops the container and removes it if AutoRemove is not enabled.
    Handles NotFound exceptions gracefully.

    Args:
        container: Docker container object to clean up
    """
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


def _create_qdrant_container(docker_client: docker.DockerClient, qdrant_image: str, config: Optional[Dict[str, Any]] = None) -> QdrantContainer:
    """Core function to create a Qdrant container with given configuration.
    
    Args:
        docker_client: Docker client instance
        qdrant_image: Qdrant Docker image to use
        config: Optional dict with container configuration. Special parameters:
            - exit_on_error (bool): If True (default), raises RuntimeError when Qdrant fails to start.
                                   If False, returns container info even if Qdrant doesn't start.
            All other parameters are passed to docker_client.containers.run()
            
    Returns:
        QdrantContainer: Container info object (see _create_container_info)
        
    Raises:
        RuntimeError: If Qdrant fails to start and exit_on_error=True
    """
    if config is None:
        config = {}
    
    config = dict(config)
    
    # Extract custom parameters
    exit_on_error = config.pop("exit_on_error", True)
    
    default_config = _get_default_qdrant_config(qdrant_image)
    merged_config = {**default_config, **config}
    
    # If using host network mode, remove port bindings
    if merged_config.get('network_mode') == 'host':
        merged_config.pop('ports', None)
    
    container = docker_client.containers.run(**merged_config)
    
    try:
        http_port, grpc_port = _extract_container_ports(container)
        
        if not wait_for_qdrant_ready(port=http_port, timeout=30):
            if exit_on_error:
                raise RuntimeError("Qdrant failed to start within 30 seconds")
        
        return _create_container_info(container, http_port, grpc_port)
    except Exception:
        _cleanup_container(container)
        raise


def _run_docker_compose(docker_client, qdrant_image, test_data_dir, config):
    """
    Core function to run docker-compose and return container info.
    
    Args:
        docker_client: Docker client instance
        qdrant_image: Qdrant image to use
        test_data_dir: Path to test data directory
        config: Configuration dict with compose_file, wait_for_ready, service_name
        
    Returns:
        tuple: (container_info, cleanup_function)
    """
    wait_for_ready = config.get("wait_for_ready", True)
    compose_file = config.get("compose_file")
    if not compose_file:
        raise ValueError("compose_file parameter is required")
    
    # Construct the path to the compose file
    compose_path = test_data_dir / compose_file
    if not compose_path.exists():
        raise FileNotFoundError(f"Docker compose file not found: {compose_path}")
    
    # Generate unique project name to avoid conflicts
    project_name = f"qdrant-test-{uuid.uuid4().hex[:8]}"
    service_name = config.get("service_name")  # None means return all services
    
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
        if wait_for_ready:
            if not wait_for_qdrant_ready(port=container_info.http_port, timeout=60):
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
            if wait_for_ready:
                if not wait_for_qdrant_ready(port=container_info.http_port, timeout=60):
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
            if wait_for_ready:
                for info in container_infos:
                    if not wait_for_qdrant_ready(port=info.http_port, timeout=60):
                        print(f"Warning: Container {info.name} failed to start within 60 seconds")
            
            container_info = container_infos  # Return the array
    
    def cleanup():
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
    
    return container_info, cleanup


def _extract_compose_container_info(container: docker.models.containers.Container, project_name: str) -> QdrantContainer:
    """Extract container info from a docker-compose container.
    
    Args:
        container: Docker container object from docker-compose
        project_name: Docker-compose project name
        
    Returns:
        QdrantContainer: Container info object with compose_project set
            
    Raises:
        RuntimeError: If HTTP port mapping cannot be found
    """
    container.reload()
    
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
    
    return QdrantContainer(
        container=container,
        host="127.0.0.1",
        name=container.name,
        http_port=http_port,
        grpc_port=grpc_port,
        compose_project=project_name
    )


@pytest.fixture(scope="session")
def docker_client() -> docker.DockerClient:
    """Create a Docker client instance.
    
    Returns:
        docker.DockerClient: Docker client connected to local Docker daemon
    """
    return docker.from_env()


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Path to the test data directory.
    
    Returns:
        Path: Absolute path to tests/resource_tests/test_data directory
    """
    return Path(__file__).parent / "test_data"


@pytest.fixture(scope="session")
def qdrant_image(docker_client: docker.DockerClient, request) -> str:
    """
    Build Qdrant Docker image once per test session.

    Can be used directly or with indirect parametrization:

    Direct usage:
        def test_something(qdrant_image):
            # Uses default tag "qdrant/qdrant:e2e-tests"

    Indirect parametrization:
        @pytest.mark.parametrize("qdrant_image", [
            {"tag": "qdrant-e2e-test", "rebuild_image": True}
        ], indirect=True)
        def test_something(qdrant_image):
            # Uses custom tag and forces rebuild
            
    Parameters (via indirect parametrization):
        - tag (str): Custom image tag (default: "qdrant/qdrant:e2e-tests")
        - rebuild_image (bool): Force rebuild even if image exists (default: False)
        
    Returns:
        str: The Docker image tag that was built or already exists
    """
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


@pytest.fixture(scope="function")
def qdrant_container(docker_client, qdrant_image, request):
    """
    Fixture for creating Qdrant containers with provided configuration.
    For a simple use case with default configuration, use qdrant fixture instead.

    Can be used as a factory or with indirect parametrization:
    
    Factory usage (returns a callable):
        def test_something(qdrant_container):
            container_info = qdrant_container(mem_limit="128m", environment={...})
            
    Indirect parametrization (returns container info directly):
        @pytest.mark.parametrize("qdrant_container", [
            {"mem_limit": "256m", "environment": {"KEY": "value"}}
        ], indirect=True)
        def test_something(qdrant_container):
            # qdrant_container is already the container info object
            host = qdrant_container.host
            port = qdrant_container.http_port
    
    Returns a QdrantContainer object with:
        - container: The Docker container object
        - host: The host address ("127.0.0.1" or "localhost" for host network)
        - name: The container name
        - http_port: The HTTP API port (6333)
        - grpc_port: The gRPC API port (6334)
        
    Parameters (all passed to docker_client.containers.run, common ones include):
        - name (str): Container name
        - mem_limit (str): Memory limit (e.g., "128m", "256m")
        - volumes (dict): Volume mounts, e.g., {'/host/path': {'bind': '/container/path', 'mode': 'rw'}}
        - mounts (list): Docker mount objects for advanced mounts (e.g., tmpfs)
        - environment (dict): Environment variables
        - network (str): Docker network name for cluster setups
        - network_mode (str): Network mode (e.g., "host")
        - command (str/list): Override default container command
        - remove (bool): Auto-remove container after exit (default: True)
        - exit_on_error (bool): If True (default), raises RuntimeError when Qdrant fails to start.
                               If False, returns container info even if Qdrant doesn't start successfully.
    """
    containers = []
    
    def _create_container(**kwargs):
        container_info = _create_qdrant_container(docker_client, qdrant_image, kwargs)
        containers.append(container_info.container)
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


@pytest.fixture(scope="function")
def qdrant(docker_client, qdrant_image, request):
    """
    A simplification of a qdrant_container fixture.
    If a default qdrant setup is needed, this fixture is the one that should be used.

    Direct usage (default configuration):
        def test_something(qdrant):
            host = qdrant.host
            port = qdrant.http_port

    Indirect parametrization (custom configuration):
        @pytest.mark.parametrize("qdrant", [
            {"mem_limit": "256m", "environment": {"KEY": "value"}}
        ], indirect=True)
        def test_something(qdrant):
            # qdrant is the container info object with custom config

    Returns a QdrantContainer object with:
        - container: The Docker container object
        - host: The host address ("127.0.0.1" or "localhost" for host network)
        - name: The container name
        - http_port: The HTTP API port (6333)
        - grpc_port: The gRPC API port (6334)

    Parameters (via indirect parametrization, all passed to docker_client.containers.run):
        - mem_limit (str): Memory limit (e.g., "256m")
        - environment (dict): Environment variables (e.g., {"QDRANT__LOG_LEVEL": "DEBUG"})
        - Any other Docker container run parameters
    """
    config = {}
    if hasattr(request, "param") and isinstance(request.param, dict):
        config = request.param

    container_info = _create_qdrant_container(docker_client, qdrant_image, config)

    try:
        yield container_info
    finally:
        _cleanup_container(container_info.container)


@pytest.fixture(scope="function")
def qdrant_compose(docker_client, qdrant_image, test_data_dir, request):
    """
    Fixture for creating Qdrant containers using docker-compose.yaml files from test_data folder.
    
    Usage with parametrization:
        @pytest.mark.parametrize("qdrant_compose", [
            {"compose_file": "docker-compose.yaml"}
        ], indirect=True)
        def test_something(qdrant_compose):
            # qdrant_compose is a QdrantContainer object with container info
            host = qdrant_compose.host
            port = qdrant_compose.http_port
            
        # With custom service name (if compose has multiple services):
        @pytest.mark.parametrize("qdrant_compose", [
            {"compose_file": "docker-compose.yaml", "service_name": "qdrant-node"}
        ], indirect=True)
        def test_something(qdrant_compose):
            # Uses specific service from compose file
            
    Parameters (via indirect parametrization):
        - compose_file (str, required): Name of compose file in test_data directory
        - service_name (str, optional): Specific service name for multi-service compose files.
                                       If not provided:
                                       - Single-service compose: returns QdrantContainer object
                                       - Multi-service compose: returns list of QdrantContainer objects
        - wait_for_ready (bool): Whether to wait for containers to be ready (default: True)
        
    Returns:
        Single-service compose or when service_name specified:
            QdrantContainer: Container info object with:
                - container: The Docker container object
                - host: The host address (always "127.0.0.1")
                - name: The container name
                - http_port: The HTTP API port
                - grpc_port: The gRPC API port (may be None)
                - compose_project: The docker-compose project name
                
        Multi-service compose without service_name:
            list: Array of QdrantContainer objects (one per service)
    """
    if not hasattr(request, "param") or not isinstance(request.param, dict):
        raise ValueError("qdrant_compose fixture requires parametrization with compose_file path")
    
    config = request.param
    container_info, cleanup = _run_docker_compose(docker_client, qdrant_image, test_data_dir, config)
    
    try:
        yield container_info
    finally:
        cleanup()


@pytest.fixture(scope="function")
def qdrant_cluster(docker_client, qdrant_container, request):
    """
    Create a Qdrant cluster with 1 leader and configurable number of followers.

    - Creates a custom Docker network for cluster communication
    - Followers are started with staggered delays to ensure proper cluster formation
    - Verifies cluster formation via the /cluster endpoint
    - All containers are kept (remove=False) for potential log inspection

    Usage:
        # Default: 1 leader + 2 followers
        def test_cluster(qdrant_cluster):
            cluster = qdrant_cluster
            
        # Custom number of followers
        @pytest.mark.parametrize("qdrant_cluster", [{"follower_count": 4}], indirect=True)
        def test_large_cluster(qdrant_cluster):
            cluster = qdrant_cluster  # 1 leader + 4 followers
            
    Parameters (via indirect parametrization):
        - follower_count (int): Number of follower nodes (default: 2)
        
    Returns a QdrantCluster object with:
        - leader: Leader node (QdrantContainer object)
        - followers: List of follower nodes (QdrantContainer objects)
        - all_nodes: List of all nodes (leader + followers)
        - network: Docker network object
        - network_name: Docker network name
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
        wait_for_qdrant_ready(port=leader_info.http_port, timeout=30)
        
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
            wait_for_qdrant_ready(port=follower_info.http_port, timeout=60)
        
        # Give cluster time to stabilize
        time.sleep(5)
        
        # Verify cluster formation
        try:
            response = requests.get(
                f"http://{leader_info.host}:{leader_info.http_port}/cluster",
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
        
        cluster_data = QdrantCluster(
            leader=leader_info,
            followers=followers,
            network=network,
            network_name=network_name
        )
        
        yield cluster_data
        
    finally:
        # Clean up the network
        try:
            network.remove()
            print(f"Cleaned up cluster network: {network_name}")
        except Exception as e:
            print(f"Error removing network {network_name}: {e}")


@pytest.fixture(scope="function")
def temp_storage_dir(request) -> Generator[Path, None, None]:
    """
    Create a temporary storage directory and ensure its removal after test.

    The entire test directory (including the storage subdirectory) is removed after the test.

    Usage:
        def test_something(temp_storage_dir):
            # Returns Path to a "storage" directory that will be cleaned up
            
    Returns:
        Path: Path to a temporary "storage" directory within a test-specific folder.
              The directory structure is: ./{test_name}/storage/
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


@pytest.fixture(scope="function")
def storage_from_archive(request, test_data_dir: Path, temp_storage_dir: Path) -> Path:
    """
    Extract an archive from test_data directory into the storage directory.

    - The archive should contain a "storage" directory that will be extracted
    - Uses temp_storage_dir fixture internally for cleanup
    - Falls back to subprocess tar command if Python extraction fails

    Usage with parametrization:
        @pytest.mark.parametrize("storage_from_archive", ["storage.tar.xz"], indirect=True)
        def test_something(storage_from_archive):
            # Archive will be extracted into the storage directory
            
    Parameters (via indirect parametrization):
        Archive filename (str): Name of archive file in test_data directory
                               Supported formats: .tar.xz, .tar.gz, .tar.bz2, .tgz, .tbz2, .tar, .zip
                               
    Returns:
        Path: Path to the temp_storage_dir where archive was extracted
    """
    if hasattr(request, "param"):
        archive_name = request.param
    else:
        raise ValueError("storage_with_archive fixture requires archive name via parametrization")
    
    # Path to test_data directory
    archive_file = test_data_dir / archive_name
    
    if not archive_file.exists():
        raise FileNotFoundError(f"Archive not found: {archive_file}")
    
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
