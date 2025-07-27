"""Helper functions for pytest fixtures and container management."""
import gzip
import shutil
import subprocess
import tarfile
import time
import zipfile
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, Union

import docker.models.containers
import requests
from docker.errors import NotFound

from .models import QdrantContainer, QdrantContainerConfig


def wait_for_qdrant_ready(port: int = 6333, timeout: int = 30) -> bool:
    """Wait for Qdrant service to be ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"http://localhost:{port}/readyz")
            if response.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(0.2)
    return False


def get_default_qdrant_config(qdrant_image: str) -> Dict[str, Any]:
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


def extract_container_ports(container: docker.models.containers.Container) -> Tuple[int, int]:
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


def create_container_info(container: docker.models.containers.Container, http_port: int, grpc_port: int) -> QdrantContainer:
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


def cleanup_container(container: docker.models.containers.Container) -> None:
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


def create_qdrant_container(docker_client: docker.DockerClient, qdrant_image: str, config: Optional[Union[Dict[str, Any], QdrantContainerConfig]] = None) -> QdrantContainer:
    """Core function to create a Qdrant container with given configuration.
    
    Args:
        docker_client: Docker client instance
        qdrant_image: Qdrant Docker image to use
        config: Optional configuration (dict or QdrantContainerConfig). Special parameters:
            - exit_on_error (bool): If True (default), raises RuntimeError when Qdrant fails to start.
                                   If False, returns container info even if Qdrant doesn't start.
            All other parameters are passed to docker_client.containers.run()
            
    Returns:
        QdrantContainer: Container info object (see create_container_info)
        
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
        
        default_config = get_default_qdrant_config(qdrant_image)
        merged_config = {**default_config, **config}
        
        # If using host network mode, remove port bindings
        if merged_config.get('network_mode') == 'host':
            merged_config.pop('ports', None)
    
    container = docker_client.containers.run(**merged_config)
    
    try:
        http_port, grpc_port = extract_container_ports(container)
        
        if not wait_for_qdrant_ready(port=http_port, timeout=30):
            if exit_on_error:
                raise RuntimeError("Qdrant failed to start within 30 seconds")
        
        return create_container_info(container, http_port, grpc_port)
    except Exception:
        cleanup_container(container)
        raise

def extract_archive(archive_file: Path, extract_to: Path, cleanup_archive: bool = False) -> Path:
    """General utility function to extract various archive formats.

    Args:
        archive_file: Path to the archive file to extract
        extract_to: Directory where to extract the archive contents
        cleanup_archive: Whether to delete the archive file after extraction

    Returns:
        Path: The extraction directory path

    Raises:
        FileNotFoundError: If archive file doesn't exist
        ValueError: If archive format is unsupported
        RuntimeError: If extraction fails

    Supported formats: .tar.xz, .tar.gz, .tar.bz2, .tgz, .tbz2, .tar, .zip, .gz
    """
    if not archive_file.exists():
        raise FileNotFoundError(f"Archive not found: {archive_file}")

    extract_to.mkdir(parents=True, exist_ok=True)
    file_name = archive_file.name.lower()

    try:
        if file_name.endswith('.gz') and not file_name.endswith(('.tar.gz', '.tgz')):
            # Handle standalone gzip files (like snapshots)
            output_file = extract_to / archive_file.stem
            with gzip.open(archive_file, 'rb') as gz_file, \
                open(output_file, 'wb') as out_file:
                    shutil.copyfileobj(gz_file, out_file)
            print(f"Extracted {archive_file} to {output_file}")

        elif file_name.endswith(('.tar.xz', '.tar.gz', '.tar.bz2', '.tgz', '.tbz2')):
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


    except (OSError, ValueError, tarfile.TarError, zipfile.BadZipFile) as e:
        print(f"Failed to extract archive {archive_file}: {e}")
        # Try fallback to subprocess for tar files
        if file_name.endswith(('.tar.xz', '.tar.gz', '.tar.bz2', '.tgz', '.tbz2', '.tar')):
            try:
                print("Trying fallback extraction with tar command...")
                subprocess.run(["tar", "-xf", str(archive_file)], cwd=str(extract_to), check=True)
                print(f"Successfully extracted {archive_file} using tar command")
            except subprocess.CalledProcessError as tar_error:
                raise RuntimeError(f"Failed to extract archive: {tar_error}") from tar_error
        else:
            raise

    if cleanup_archive:
        archive_file.unlink(missing_ok=True)
        print(f"Cleaned up archive: {archive_file}")

    return extract_to
