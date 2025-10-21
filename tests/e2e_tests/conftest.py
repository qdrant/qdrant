import shutil
import subprocess
from pathlib import Path
from typing import Generator

import pytest
import docker
from docker.errors import ImageNotFound

from .models import QdrantContainerConfig
from .utils import (
    create_qdrant_container,
    cleanup_container, extract_archive, run_docker_compose
)


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Hook to capture test outcome for logging purposes"""
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)


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
    image_tag = config.get("tag", "qdrant/qdrant:e2e-tests")
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
        container_info = create_qdrant_container(docker_client, qdrant_image, config)
        containers.append(container_info.container)
        return container_info

    def _log_containers_on_failure():
        """Output container logs if the test failed"""
        if request.node.rep_call.failed if hasattr(request.node, 'rep_call') else False:
            print("\n" + "="*50)
            print("TEST FAILED - DUMPING CONTAINER LOGS")
            print("="*50)
            for docker_container in containers:
                try:
                    docker_container.reload()
                    logs = docker_container.logs(tail=50).decode('utf-8', errors='ignore')
                    print(f"\nLogs for container {docker_container.name}:")
                    print("-" * 30)
                    print(logs)
                    print("-" * 30)
                except Exception as e:
                    print(f"Failed to get logs for container {docker_container.name}: {e}")
            print("="*50)

    def _cleanup_containers():
        """Clean up containers after potentially logging them"""
        # First log containers if test failed (before cleanup)
        _log_containers_on_failure()

        # Then cleanup all containers
        for container in containers:
            cleanup_container(container)

    # Register the finalizer to handle logging and cleanup
    request.addfinalizer(_cleanup_containers)

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

        container_info = create_qdrant_container(docker_client, qdrant_image, config)
        containers.append(container_info.container)
        yield container_info
    else:
        # Factory mode - return the factory function
        yield _create_container


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

    archive_file = test_data_dir / archive_name
    extract_to = temp_storage_dir.parent

    extract_archive(archive_file, extract_to)

    return temp_storage_dir


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
        - service_name (str, optional): Specific service name for multiservice compose files.
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
    cluster = run_docker_compose(docker_client, qdrant_image, test_data_dir, config)

    try:
        yield cluster
    finally:
        cluster.cleanup()
