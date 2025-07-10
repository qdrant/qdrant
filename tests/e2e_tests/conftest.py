import subprocess
from typing import Optional, List, Dict, Any, Tuple, Union
from dataclasses import dataclass, field

import pytest
import docker
import docker.models.containers
import docker.models.networks
from docker.errors import ImageNotFound, NotFound
from pathlib import Path

from e2e_tests.utils import wait_for_qdrant_ready


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


@dataclass
class QdrantContainerConfig:
    """Configuration for creating Qdrant containers with proper typing and defaults."""
    
    # Container identification
    name: Optional[str] = None
    
    # Resource limits
    mem_limit: Optional[str] = None
    cpu_limit: Optional[str] = None
    
    # Network configuration
    network: Optional[str] = None
    network_mode: Optional[str] = None
    
    # Storage configuration
    volumes: Optional[Dict[str, Dict[str, str]]] = None
    mounts: Optional[List[Any]] = None
    
    # Environment and command
    environment: Optional[Dict[str, str]] = None
    command: Optional[Union[str, List[str]]] = None
    
    # Container behavior
    remove: bool = True
    detach: bool = True
    
    # Qdrant-specific settings
    exit_on_error: bool = True
    
    # Additional Docker parameters
    additional_params: Optional[Dict[str, Any]] = field(default_factory=dict)
    
    def to_docker_config(self, qdrant_image: str) -> Dict[str, Any]:
        """Convert this config to Docker container.run() parameters."""
        config = {
            "image": qdrant_image,
            "detach": self.detach,
            "remove": self.remove,
        }
        
        # Add port bindings unless host network mode
        if self.network_mode != 'host':
            config["ports"] = {'6333/tcp': ('127.0.0.1', None), '6334/tcp': ('127.0.0.1', None)}
        
        # Add optional parameters
        if self.name:
            config["name"] = self.name
        if self.mem_limit:
            config["mem_limit"] = self.mem_limit
        if self.cpu_limit:
            config["cpu_limit"] = self.cpu_limit
        if self.network:
            config["network"] = self.network
        if self.network_mode:
            config["network_mode"] = self.network_mode
        if self.volumes:
            config["volumes"] = self.volumes
        if self.mounts:
            config["mounts"] = self.mounts
        if self.environment:
            config["environment"] = self.environment
        if self.command:
            config["command"] = self.command
        
        # Add any additional parameters
        if self.additional_params:
            config.update(self.additional_params)
        
        return config


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


def _create_qdrant_container(docker_client: docker.DockerClient, qdrant_image: str, config: Optional[Union[Dict[str, Any], QdrantContainerConfig]] = None) -> QdrantContainer:
    """Core function to create a Qdrant container with given configuration.
    
    Args:
        docker_client: Docker client instance
        qdrant_image: Qdrant Docker image to use
        config: Optional configuration (dict or QdrantContainerConfig). Special parameters:
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
    
    # Handle both dict and QdrantContainerConfig inputs
    if isinstance(config, QdrantContainerConfig):
        exit_on_error = config.exit_on_error
        merged_config = config.to_docker_config(qdrant_image)
    else:
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
        Path: Absolute path to tests/e2e_tests/test_data directory
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
def qdrant_container_factory(docker_client, qdrant_image, request):
    """
    Fixture for creating Qdrant containers with provided configuration.
    For a simple use case with default configuration, use qdrant fixture instead.

    Can be used as a factory or with indirect parametrization:
    
    Factory usage (returns a callable):
        def test_something(qdrant_container_factory):
            container_info = qdrant_container_factory(mem_limit="128m", environment={...})
            
    Indirect parametrization (returns container info directly):
        @pytest.mark.parametrize("qdrant_container_factory", [
            {"mem_limit": "256m", "environment": {"KEY": "value"}}
        ], indirect=True)
        def test_something(qdrant_container_factory):
            # qdrant_container_factory is already the container info object
            host = qdrant_container_factory.host
            port = qdrant_container_factory.http_port
    
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
    
    def _create_container(*args, **kwargs):
        # Handle both QdrantContainerConfig objects and keyword arguments
        if len(args) == 1 and isinstance(args[0], QdrantContainerConfig):
            config = args[0]
        else:
            # Convert kwargs to QdrantContainerConfig for better type safety
            config = QdrantContainerConfig(**kwargs)
        container_info = _create_qdrant_container(docker_client, qdrant_image, config)
        containers.append(container_info.container)
        return container_info
    
    # Check if this is being used with indirect parametrization
    if hasattr(request, "param"):
        if isinstance(request.param, dict):
            # Indirect parametrization mode with dict - create container directly with provided config
            config = QdrantContainerConfig(**request.param)
        elif isinstance(request.param, QdrantContainerConfig):
            # Indirect parametrization mode with QdrantContainerConfig - use directly
            config = request.param
        else:
            raise ValueError(f"Unsupported parameter type for qdrant_container_factory: {type(request.param)}")
        
        container_info = _create_qdrant_container(docker_client, qdrant_image, config)
        containers.append(container_info.container)
        yield container_info
    else:
        # Factory mode - return the factory function
        yield _create_container
    
    # Cleanup all containers
    for container in containers:
        _cleanup_container(container)
