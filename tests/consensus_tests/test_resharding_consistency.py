"""
Test resharding with concurrent updates, restarts, and consistency verification.

This test verifies that during resharding operations, concurrent updates to points
(vectors and payloads) are properly replicated and all nodes maintain consistency,
even when nodes are being restarted. Note that a running Cluster Manager instance (port 7333) is required.

Usage:
    TEST_DURATION_MINUTES=10 pytest tests/consensus_tests/test_resharding_consistency.py -v
"""
import os
import pathlib
import random
import shutil
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Callable, Literal, Set

import requests

# Directory to preserve peer storage on failure (for investigation)
PRESERVED_STORAGE_DIR = Path("/tmp/qdrant_test_failure_storage")

from .assertions import assert_http_ok
from .fixtures import create_collection, random_dense_vector
from .test_resharding import start_resharding
from .utils import (
    assert_project_root,
    check_all_replicas_active,
    every_test,
    get_collection_point_count,
    get_uri,
    processes,
    start_cluster,
    start_peer,
    wait_collection_exists_and_active_on_all_peers,
    wait_for,
    wait_for_peer_online,
)

# =============================================================================
# Constants
# =============================================================================

N_PEERS = 3
N_SHARDS = 3
N_REPLICAS = 2
COLLECTION_NAME = "test_collection"
N_POINTS = 1000

# Port configuration (http_port = port_seed + PORT_OFFSET)
PORT_OFFSET = 2

# Test duration
DEFAULT_TEST_DURATION_MINUTES = 5

# Timeouts (seconds)
DEFAULT_REQUEST_TIMEOUT = 30.0
DEFAULT_UPDATE_TIMEOUT = 5.0
CONSISTENCY_CHECK_TIMEOUT = 60.0

# Consistency check configuration
CONSISTENCY_MAX_RETRIES = 5
CONSISTENCY_RETRY_DELAY = 1.0
CONSISTENCY_CHECK_INTERVAL = 2.0

# Worker intervals (seconds)
RESHARDING_MANAGER_INTERVAL = 5.0
RESTART_MIN_INTERVAL = 2.0
RESTART_MAX_INTERVAL = 7.0
SNAPSHOT_MIN_INTERVAL = 30.0
SNAPSHOT_MAX_INTERVAL = 120.0

# Errors that are expected during resharding/restarts (don't indicate data inconsistency)
IGNORABLE_ERRORS = frozenset([
    "does not have enough active replicas",
    "No active replica",
    "Replica set is not available",
    "Forward proxy timeout",
    "Connection refused",
    "Connection reset",
    "transport error",
    "Read timed out",
    "timed out",
])

# Type aliases
PointId = int
PeerUrlGetter = Callable[[], list[str]]


# =============================================================================
# Logging
# =============================================================================

def log(level: str, message: str, **kwargs) -> None:
    """Log a message with structured key-value pairs."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    kv_pairs = " ".join(f'{k}="{v}"' for k, v in kwargs.items())
    print(f'ts={ts} level={level} msg="{message}" {kv_pairs}')


# =============================================================================
# Point Operations
# =============================================================================

def generate_timestamp() -> str:
    """Generate an ISO 8601 timestamp string with timezone."""
    return datetime.now(timezone.utc).isoformat()


def create_points_with_timestamps(num: int) -> list[dict]:
    """Create points with text payload containing timestamp."""
    return [
        {
            "id": i,
            "vector": {"": random_dense_vector()},
            "payload": {"timestamp": generate_timestamp()},
        }
        for i in range(num)
    ]


def _make_error_response(error: Exception) -> requests.Response:
    """Create a fake 503 response for connection errors."""
    resp = requests.Response()
    resp.status_code = 503
    resp._content = str(error).encode()
    return resp


def upsert_points(
    peer_url: str,
    points: list[dict],
    collection_name: str = COLLECTION_NAME,
    timeout: float = DEFAULT_REQUEST_TIMEOUT,
) -> requests.Response:
    """Upsert points to the collection with timeout handling."""
    try:
        return requests.put(
            f"{peer_url}/collections/{collection_name}/points?wait=true&ordering=weak",
            json={"points": points},
            timeout=timeout,
        )
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        return _make_error_response(e)


def update_point(
    peer_url: str,
    point_id: int,
    collection_name: str = COLLECTION_NAME,
    timeout: float = DEFAULT_UPDATE_TIMEOUT,
) -> requests.Response:
    """Update a single point with new vector and timestamp payload."""
    point = {
        "id": point_id,
        "vector": {"": random_dense_vector()},
        "payload": {"timestamp": generate_timestamp()},
    }
    try:
        return requests.put(
            f"{peer_url}/collections/{collection_name}/points?wait=true",
            json={"points": [point]},
            timeout=timeout,
        )
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        return _make_error_response(e)


def update_points_batch(
    peer_url: str,
    point_ids: list[int],
    collection_name: str = COLLECTION_NAME,
    timeout: float = DEFAULT_REQUEST_TIMEOUT,
) -> requests.Response:
    """Update multiple points with new vectors and timestamp payloads."""
    points = [
        {
            "id": point_id,
            "vector": {"": random_dense_vector()},
            "payload": {"timestamp": generate_timestamp()},
        }
        for point_id in point_ids
    ]
    try:
        return requests.put(
            f"{peer_url}/collections/{collection_name}/points?wait=true",
            json={"points": points},
            timeout=timeout,
        )
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        return _make_error_response(e)


# =============================================================================
# Consistency Checking
# =============================================================================

def get_inconsistent_point_ids(
    peer_url: str,
    point_ids: Set[PointId],
    collection_name: str = COLLECTION_NAME,
    timeout: float = DEFAULT_REQUEST_TIMEOUT,
) -> Set[PointId]:
    """
    Return set of point IDs that are inconsistent across replicas.

    Uses consistency=all parameter - points not returned are inconsistent.
    """
    start_time = time.time()

    # Use scroll for checking all points
    all_points = []
    offset = None

    while True:
        resp = requests.post(
            f"{peer_url}/collections/{collection_name}/points/scroll?consistency=all",
            json={
                "limit": len(point_ids),
                "offset": offset,
                "with_payload": ["timestamp"],
                "with_vector": False,
            },
            timeout=timeout,
        )
        assert_http_ok(resp)
        result = resp.json()["result"]
        all_points.extend(result["points"])

        offset = result.get("next_page_offset")
        if offset is None:
            break

    duration = time.time() - start_time
    consistent_points = {int(point["id"]) for point in all_points}
    inconsistent_points = point_ids - consistent_points

    log(
        "debug",
        "Consistency check completed",
        duration_seconds=f"{duration:.3f}",
        points_checked=len(point_ids),
        points_consistent=len(consistent_points),
        points_inconsistent=len(inconsistent_points),
    )

    return inconsistent_points


def get_points_from_all_nodes(
    peer_urls: list[str],
    point_ids: Set[PointId],
    collection_name: str = COLLECTION_NAME,
    timeout: float = DEFAULT_REQUEST_TIMEOUT,
) -> dict[str, list[dict]]:
    """Retrieve points from each node individually."""
    points_per_node = {}

    for peer_url in peer_urls:
        try:
            resp = requests.post(
                f"{peer_url}/collections/{collection_name}/points",
                json={
                    "ids": list(point_ids),
                    "with_payload": True,
                    "with_vector": True,
                },
                timeout=timeout,
            )
            if resp.ok:
                points_per_node[peer_url] = resp.json()["result"]
        except Exception as e:
            log("warn", "Failed to retrieve points from node", node_url=peer_url, error=str(e)[:100])

    return points_per_node


def find_point_differences(points_per_node: dict[str, list[dict]]) -> dict:
    """Identify points that have different values across nodes.

    Returns dict with:
    - 'differences': points with actual differences found
    - 'all_values': all point values by node (for debugging when differences is empty)
    """
    point_values_by_node: dict[int, dict[str, dict]] = {}

    for node_url, points in points_per_node.items():
        for point in points:
            point_id = point["id"]
            if point_id not in point_values_by_node:
                point_values_by_node[point_id] = {}

            payload = point.get("payload")
            timestamp = payload.get("timestamp") if isinstance(payload, dict) else None

            vector = point.get("vector")
            if isinstance(vector, dict):
                vector_value = vector.get("")
            elif isinstance(vector, list):
                vector_value = vector
            else:
                vector_value = None

            point_values_by_node[point_id][node_url] = {
                "timestamp": timestamp,
                "vector_sum": sum(vector_value) if vector_value else None,
            }

    # Find differences
    differences = {}
    for point_id, node_values in point_values_by_node.items():
        if len(node_values) < len(points_per_node):
            differences[point_id] = {
                "issue": "missing_from_nodes",
                "present_in": list(node_values.keys()),
            }
            continue

        timestamps = {url: v["timestamp"] for url, v in node_values.items()}
        if len(set(timestamps.values())) > 1:
            differences[point_id] = {"timestamps": timestamps}

    # Always return all values for debugging - if differences is empty,
    # points may have converged between consistency check and individual fetch
    return {
        "differences": differences,
        "all_values": point_values_by_node,
        "note": "If differences is empty, points may have converged after consistency check"
    }


# =============================================================================
# Cluster State Management
# =============================================================================

@dataclass
class ClusterState:
    """Manages cluster state including peer URLs and restart functionality."""

    peer_urls: list[str]
    peer_dirs: list[Path]
    bootstrap_uri: str
    env: dict[str, str]
    lock: threading.Lock = field(default_factory=threading.Lock)
    _current_urls: list[str | None] = field(default_factory=list)
    _port_seeds: list[int] = field(default_factory=list)
    _restart_count: int = 0

    def __post_init__(self):
        self._current_urls = list(self.peer_urls)
        self._port_seeds = [
            int(url.rsplit(":", 1)[-1]) - PORT_OFFSET
            for url in self.peer_urls
        ]

    def get_available_urls(self) -> list[str]:
        """Get list of currently available peer URLs."""
        with self.lock:
            return [url for url in self._current_urls if url is not None]

    def get_all_urls(self) -> list[str]:
        """Get list of all peer URLs (for cluster manager)."""
        return self.peer_urls

    def get_restartable_peer_indices(self, skip_bootstrap: bool = True) -> list[int]:
        """Get indices of peers that can be restarted (are currently online)."""
        with self.lock:
            start_idx = 1 if skip_bootstrap else 0
            return [
                i for i in range(start_idx, len(self._current_urls))
                if self._current_urls[i] is not None
            ]

    @property
    def restart_count(self) -> int:
        with self.lock:
            return self._restart_count

    def preserve_storage(self, dest_dir: Path = PRESERVED_STORAGE_DIR) -> Path | None:
        """
        Copy all peer storage directories to a preserved location for investigation.

        Returns the path where storage was preserved, or None if failed.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        preserve_path = dest_dir / f"failure_{timestamp}"

        try:
            # Remove old preserved storage if it exists
            if dest_dir.exists():
                shutil.rmtree(dest_dir)

            preserve_path.mkdir(parents=True, exist_ok=True)

            for idx, peer_dir in enumerate(self.peer_dirs):
                if peer_dir.exists():
                    dest = preserve_path / f"peer_{idx}"
                    log("info", f"Preserving peer {idx} storage", src=str(peer_dir), dest=str(dest))
                    shutil.copytree(peer_dir, dest)

            log("info", "Storage preserved for investigation", path=str(preserve_path))
            return preserve_path

        except Exception as e:
            log("error", "Failed to preserve storage", error=str(e))
            return None

    def restart_peer(self, peer_idx: int) -> str | None:
        """
        Restart a specific peer. Returns new URL or None if failed.

        Thread-safe: marks peer as unavailable during restart.
        """
        with self.lock:
            old_url = self._current_urls[peer_idx]
            if old_url is None:
                return None
            self._current_urls[peer_idx] = None
            self._restart_count += 1
            restart_id = self._restart_count

        log("info", f"Restarting peer {peer_idx}", old_url=old_url, restart_id=restart_id)

        # Find and kill the process
        process_to_kill = None
        for proc in processes:
            if get_uri(proc.http_port) == old_url:
                process_to_kill = proc
                break

        if not process_to_kill:
            log("warn", f"Could not find process for peer {peer_idx}")
            with self.lock:
                self._current_urls[peer_idx] = old_url
            return None

        try:
            process_to_kill.kill()
            processes.remove(process_to_kill)
        except Exception as e:
            log("warn", f"Error killing process: {e}")

        sleep(0.5)

        try:
            new_url = start_peer(
                self.peer_dirs[peer_idx],
                f"peer_{peer_idx}_restart_{restart_id}.log",
                self.bootstrap_uri,
                port=self._port_seeds[peer_idx],
                extra_env=self.env,
            )
            wait_for_peer_online(new_url)

            with self.lock:
                self._current_urls[peer_idx] = new_url

            log("info", f"Peer {peer_idx} restarted", new_url=new_url)
            return new_url

        except Exception as e:
            log("error", f"Failed to restart peer {peer_idx}", error=str(e))
            return None


# =============================================================================
# Base Worker Class
# =============================================================================

@dataclass
class WorkerStats:
    """Statistics for a worker."""
    count: int = 0
    success_count: int = 0
    failure_count: int = 0
    errors: list[str] = field(default_factory=list)

    def record_success(self):
        self.count += 1
        self.success_count += 1

    def record_failure(self, error: str | None = None):
        self.count += 1
        self.failure_count += 1
        if error:
            self.errors.append(error)


class BaseWorker(ABC):
    """Abstract base class for all worker threads."""

    def __init__(self):
        self.stop_event = threading.Event()
        self.failure_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.stats = WorkerStats()
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the worker thread."""
        self.thread = threading.Thread(target=self._run_wrapper, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        """Stop the worker thread."""
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=30)

    def _run_wrapper(self) -> None:
        """Wrapper that catches exceptions in the run loop."""
        try:
            self._run()
        except Exception as e:
            with self._lock:
                self.stats.errors.append(f"Fatal error: {e}")
            log("error", f"Worker crashed: {e}")

    @abstractmethod
    def _run(self) -> None:
        """Main worker loop. Must be implemented by subclasses."""
        pass

    def _wait_or_stop(self, timeout: float) -> bool:
        """Wait for timeout or stop event. Returns True if should stop."""
        return self.stop_event.wait(timeout=timeout)


# =============================================================================
# Worker Implementations
# =============================================================================

class UpdateWorker(BaseWorker):
    """Worker that continuously sends batch point updates to random nodes."""

    def __init__(
        self,
        peer_url_getter: PeerUrlGetter,
        num_points: int,
        collection_name: str = COLLECTION_NAME,
        batch_percentage: float = 0.10,  # 10% of points per batch
    ):
        super().__init__()
        self.peer_url_getter = peer_url_getter
        self.num_points = num_points
        self.collection_name = collection_name
        self.batch_size = max(1, int(num_points * batch_percentage))
        self.pause_event = threading.Event()  # When set, updates are paused
        self.all_point_ids = list(range(num_points))

    def pause(self) -> None:
        """Pause updates."""
        self.pause_event.set()
        log("info", "Update worker paused")

    def resume(self) -> None:
        """Resume updates."""
        self.pause_event.clear()
        log("info", "Update worker resumed")

    def _run(self) -> None:
        while not self.stop_event.is_set():
            # Check if paused
            if self.pause_event.is_set():
                sleep(0.1)
                continue

            peer_urls = self.peer_url_getter()
            if not peer_urls:
                sleep(0.1)
                continue

            peer_url = random.choice(peer_urls)

            # Select random batch of point IDs (10% of points)
            batch_point_ids = random.sample(self.all_point_ids, self.batch_size)

            resp = update_points_batch(peer_url, batch_point_ids, self.collection_name)

            with self._lock:
                if resp.ok:
                    self.stats.record_success()
                else:
                    if resp.status_code != 503:  # 503 expected during restarts
                        self.stats.record_failure(f"Update failed: {resp.status_code}")
                    else:
                        self.stats.count += 1

            sleep(0.01)


class ConsistencyCheckWorker(BaseWorker):
    """Worker that periodically checks data consistency across nodes.

    When inconsistency is detected:
    1. Immediately pauses updates
    2. Continues retry attempts with updates paused
    3. If resolved, resumes updates and continues
    4. If still inconsistent after all retries, fails the test
    """

    def __init__(
        self,
        peer_url_getter: PeerUrlGetter,
        point_ids: Set[PointId],
        update_worker: UpdateWorker | None = None,
        collection_name: str = COLLECTION_NAME,
        interval: float = CONSISTENCY_CHECK_INTERVAL,
        max_retries: int = CONSISTENCY_MAX_RETRIES,
        retry_delay: float = CONSISTENCY_RETRY_DELAY,
    ):
        super().__init__()
        self.peer_url_getter = peer_url_getter
        self.point_ids = point_ids
        self.update_worker = update_worker
        self.collection_name = collection_name
        self.interval = interval
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.inconsistencies: list[dict] = []

    def _run(self) -> None:
        while not self.stop_event.is_set():
            peer_urls = self.peer_url_getter()
            if not peer_urls:
                if self._wait_or_stop(self.interval):
                    break
                continue

            check_id = self.stats.count + 1
            success = self._perform_check_with_retries(peer_urls, check_id)

            with self._lock:
                if success:
                    self.stats.record_success()
                else:
                    self.stats.record_failure()
                    self.failure_event.set()
                    return

            if self._wait_or_stop(self.interval):
                break

    def _perform_check_with_retries(self, peer_urls: list[str], check_id: int) -> bool:
        """Perform consistency check with retries. Returns True if consistent.

        When inconsistency is found:
        1. Immediately pause updates
        2. Continue retrying consistency checks
        3. If resolved, resume updates and return True
        4. If still inconsistent after all retries, return False
        """
        updates_paused = False

        try:
            # Initial check against all points
            try:
                inconsistent_ids = get_inconsistent_point_ids(
                    peer_url=peer_urls[0],
                    point_ids=self.point_ids,
                    collection_name=self.collection_name,
                    timeout=CONSISTENCY_CHECK_TIMEOUT,
                )
            except Exception as e:
                error_str = str(e)
                if any(err in error_str for err in IGNORABLE_ERRORS):
                    log("debug", "Consistency check skipped (expected error)", check_id=check_id)
                    return True  # Skip this check, not a real failure
                raise

            if not inconsistent_ids:
                log("info", "Consistency check passed", check_id=check_id, attempts=1)
                return True

            # Inconsistency found - immediately pause updates
            if self.update_worker:
                self.update_worker.pause()
                updates_paused = True

            log(
                "warn",
                "Inconsistency detected, updates paused, starting retries",
                check_id=check_id,
                inconsistent_count=len(inconsistent_ids),
            )

            # Retry loop with updates paused
            retry_count = 0
            while retry_count < self.max_retries:
                sleep(self.retry_delay)
                retry_count += 1

                try:
                    # Re-check only the inconsistent points
                    inconsistent_ids = get_inconsistent_point_ids(
                        peer_url=peer_urls[0],
                        point_ids=inconsistent_ids,
                        collection_name=self.collection_name,
                        timeout=CONSISTENCY_CHECK_TIMEOUT,
                    )

                    if not inconsistent_ids:
                        log(
                            "info",
                            "Consistency restored after retries, resuming updates",
                            check_id=check_id,
                            attempts=retry_count + 1,
                        )
                        return True  # Finally block will resume updates

                    log(
                        "warn",
                        "Still inconsistent, retrying",
                        check_id=check_id,
                        attempt=retry_count,
                        inconsistent_count=len(inconsistent_ids),
                    )

                except Exception as e:
                    error_str = str(e)
                    if any(err in error_str for err in IGNORABLE_ERRORS):
                        log("debug", "Consistency check skipped (expected error)", check_id=check_id)
                        continue

                    log("warn", "Consistency check error", check_id=check_id, error=error_str[:100])

            # Failed after all retries - collect details and fail
            sample_ids = set(sorted(inconsistent_ids)[:10])
            points_per_node = get_points_from_all_nodes(peer_urls, sample_ids, self.collection_name)
            details = find_point_differences(points_per_node)

            with self._lock:
                self.inconsistencies.append({
                    "check_id": check_id,
                    "inconsistent_count": len(inconsistent_ids),
                    "sample_ids": sorted(sample_ids),
                    "details": details,
                })

            log(
                "error",
                "Consistency check FAILED after all retries",
                check_id=check_id,
                inconsistent_count=len(inconsistent_ids),
                details=str(details),
            )
            return False

        finally:
            # Resume updates if they were paused (whether success or failure)
            if updates_paused and self.update_worker:
                self.update_worker.resume()


class RestartWorker(BaseWorker):
    """Worker that continuously restarts random non-bootstrap peers."""

    def __init__(
        self,
        cluster_state: ClusterState,
        min_interval: float = RESTART_MIN_INTERVAL,
        max_interval: float = RESTART_MAX_INTERVAL,
    ):
        super().__init__()
        self.cluster_state = cluster_state
        self.min_interval = min_interval
        self.max_interval = max_interval

    def _run(self) -> None:
        while not self.stop_event.is_set():
            interval = random.uniform(self.min_interval, self.max_interval)
            if self._wait_or_stop(interval):
                break

            # Pick a random non-bootstrap peer (skip peer 0)
            restartable_indices = self.cluster_state.get_restartable_peer_indices()
            if not restartable_indices:
                continue

            peer_idx = random.choice(restartable_indices)
            new_url = self.cluster_state.restart_peer(peer_idx)

            with self._lock:
                if new_url:
                    self.stats.record_success()
                else:
                    self.stats.record_failure(f"Failed to restart peer {peer_idx}")


class SnapshotWorker(BaseWorker):
    """Worker that periodically creates and deletes snapshots on all nodes."""

    def __init__(
        self,
        peer_url_getter: PeerUrlGetter,
        collection_name: str = COLLECTION_NAME,
        min_interval: float = SNAPSHOT_MIN_INTERVAL,
        max_interval: float = SNAPSHOT_MAX_INTERVAL,
    ):
        super().__init__()
        self.peer_url_getter = peer_url_getter
        self.collection_name = collection_name
        self.min_interval = min_interval
        self.max_interval = max_interval

    def _get_snapshots(self, peer_url: str) -> list[str]:
        """Get list of snapshot names from a node."""
        try:
            resp = requests.get(
                f"{peer_url}/collections/{self.collection_name}/snapshots",
                timeout=DEFAULT_REQUEST_TIMEOUT,
            )
            if resp.ok:
                return [s["name"] for s in resp.json().get("result", [])]
        except Exception as e:
            log("debug", "Failed to get snapshots", url=peer_url, error=str(e)[:100])
        return []

    def _create_snapshot(self, peer_url: str) -> str | None:
        """Create a snapshot on a node. Returns snapshot name or None on failure."""
        try:
            # Check if snapshots already exist (avoid OOD errors)
            existing = self._get_snapshots(peer_url)
            if existing:
                log("debug", "Snapshots already exist, skipping create", url=peer_url, count=len(existing))
                return None

            resp = requests.post(
                f"{peer_url}/collections/{self.collection_name}/snapshots",
                timeout=DEFAULT_REQUEST_TIMEOUT,
            )
            if resp.ok:
                snapshot_name = resp.json().get("result", {}).get("name")
                log("debug", "Created snapshot", url=peer_url, snapshot=snapshot_name)
                return snapshot_name
            else:
                log("debug", "Failed to create snapshot", url=peer_url, status=resp.status_code)
        except Exception as e:
            log("debug", "Exception creating snapshot", url=peer_url, error=str(e)[:100])
        return None

    def _delete_snapshot(self, peer_url: str, snapshot_name: str) -> bool:
        """Delete a snapshot from a node."""
        try:
            resp = requests.delete(
                f"{peer_url}/collections/{self.collection_name}/snapshots/{snapshot_name}",
                timeout=DEFAULT_REQUEST_TIMEOUT,
            )
            if resp.ok:
                log("debug", "Deleted snapshot", url=peer_url, snapshot=snapshot_name)
                return True
            else:
                log("debug", "Failed to delete snapshot", url=peer_url, snapshot=snapshot_name, status=resp.status_code)
        except Exception as e:
            log("debug", "Exception deleting snapshot", url=peer_url, snapshot=snapshot_name, error=str(e)[:100])
        return False

    def _delete_all_snapshots(self, peer_url: str) -> None:
        """Delete all snapshots from a node."""
        snapshots = self._get_snapshots(peer_url)
        for snapshot_name in snapshots:
            self._delete_snapshot(peer_url, snapshot_name)

    def _run(self) -> None:
        while not self.stop_event.is_set():
            interval = random.uniform(self.min_interval, self.max_interval)
            if self._wait_or_stop(interval):
                break

            peer_urls = self.peer_url_getter()
            if not peer_urls:
                continue

            log("info", "Starting snapshot cycle", node_count=len(peer_urls))

            # Create snapshots on all nodes
            created_count = 0
            for peer_url in peer_urls:
                if self._create_snapshot(peer_url):
                    created_count += 1

            log("debug", "Snapshots created", count=created_count)

            # Wait before deleting
            if self._wait_or_stop(5.0):
                break

            # Delete snapshots on all nodes
            for peer_url in peer_urls:
                self._delete_all_snapshots(peer_url)

            # Wait and verify deletion
            if self._wait_or_stop(5.0):
                break

            # Verify snapshots were deleted
            leftover_count = 0
            for peer_url in peer_urls:
                remaining = self._get_snapshots(peer_url)
                if remaining:
                    leftover_count += len(remaining)
                    log("warn", "Snapshots remain after deletion", url=peer_url, snapshots=remaining)

            with self._lock:
                if leftover_count == 0:
                    self.stats.record_success()
                else:
                    self.stats.record_failure(f"Leftover snapshots: {leftover_count}")

            log("info", "Snapshot cycle completed", created=created_count, leftover=leftover_count)


class ReshardingWorker(BaseWorker):
    """Worker that continuously triggers resharding operations (alternating up/down)."""

    def __init__(
        self,
        cluster_state: ClusterState,
        collection_name: str = COLLECTION_NAME,
        manager_url: str = "http://localhost:7333/manage",
        manager_interval: float = RESHARDING_MANAGER_INTERVAL,
    ):
        super().__init__()
        self.cluster_state = cluster_state
        self.collection_name = collection_name
        self.manager_url = manager_url
        self.manager_interval = manager_interval
        self.current_shard_count = N_SHARDS
        self.direction: Literal["up", "down"] = "down"

    def _run(self) -> None:
        while not self.stop_event.is_set():
            # Determine direction based on shard count
            self._update_direction()

            # Start resharding
            available_urls = self.cluster_state.get_available_urls()
            if not available_urls:
                if self._wait_or_stop(1.0):
                    break
                continue

            log(
                "info",
                "Starting resharding",
                direction=self.direction,
                current_shards=self.current_shard_count,
            )

            try:
                resp = start_resharding(available_urls[0], self.collection_name, direction=self.direction)
                if not resp.ok:
                    log("warn", f"Failed to start resharding: {resp.status_code}")
                    if self._wait_or_stop(random.uniform(1, 5)):
                        break
                    continue
            except Exception as e:
                log("warn", f"Exception starting resharding: {e}")
                if self._wait_or_stop(random.uniform(1, 5)):
                    break
                continue

            # Wait for resharding to complete via manager
            if self._wait_for_resharding_completion():
                with self._lock:
                    self.stats.record_success()
                if self.direction == "down":
                    self.current_shard_count -= 1
                else:
                    self.current_shard_count += 1
                log("info", "Resharding completed", new_shard_count=self.current_shard_count)
            else:
                with self._lock:
                    self.stats.record_failure("Resharding did not complete")

            # Random delay before next resharding
            if self._wait_or_stop(random.uniform(1, 5)):
                break

    def _update_direction(self) -> None:
        """Update resharding direction based on current shard count."""
        if self.current_shard_count <= 1:
            self.direction = "up"
        elif self.current_shard_count >= N_SHARDS + 2:
            self.direction = "down"
        elif random.random() < 0.3:  # 30% chance to flip
            self.direction = "up" if self.direction == "down" else "down"

    def _wait_for_resharding_completion(self) -> bool:
        """Send requests to cluster manager until resharding completes."""
        while not self.stop_event.is_set():
            try:
                resp = requests.post(
                    self.manager_url,
                    json={
                        "urls": self.cluster_state.get_all_urls(),
                        "rules": {"dry_run": False},
                    },
                    timeout=30.0,
                )

                if resp.ok:
                    data = resp.json()
                    actions = data.get("actions", {})
                    description = data.get("description")

                    if (actions == {} or actions is None) and description is None:
                        return True

                    log(
                        "debug",
                        "Resharding in progress",
                        description=description,
                        actions_count=len(actions) if actions else 0,
                    )

            except Exception as e:
                log("debug", f"Manager request failed: {e}")

            if self._wait_or_stop(self.manager_interval):
                return False

        return False


# =============================================================================
# Test Orchestration
# =============================================================================

@dataclass
class TestResult:
    """Results from a test run."""
    duration_seconds: float
    update_stats: WorkerStats
    consistency_stats: WorkerStats
    restart_stats: WorkerStats
    resharding_stats: WorkerStats
    snapshot_stats: WorkerStats
    inconsistencies: list[dict]
    final_shard_count: int

    def print_summary(self) -> None:
        print(f"\n{'=' * 60}")
        print(f"Test completed in {self.duration_seconds:.1f}s ({self.duration_seconds/60:.1f} min)")
        print(f"{'=' * 60}")
        print(f"Updates: {self.update_stats.success_count}/{self.update_stats.count} successful")
        print(f"Consistency checks: {self.consistency_stats.success_count}/{self.consistency_stats.count} passed")
        print(f"Restarts: {self.restart_stats.success_count} completed")
        print(f"Resharding ops: {self.resharding_stats.success_count} completed")
        print(f"Snapshot cycles: {self.snapshot_stats.success_count} completed")
        print(f"Final shard count: {self.final_shard_count}")

        if self.consistency_stats.errors:
            print(f"Consistency errors: {self.consistency_stats.errors[:5]}")
        if self.inconsistencies:
            print(f"Inconsistencies found: {self.inconsistencies}")


def run_continuous_test(
    cluster_state: ClusterState,
    point_ids: Set[PointId],
    duration_seconds: float,
    enable_restarts: bool = True,
    enable_resharding: bool = True,
    enable_snapshots: bool = True,
) -> TestResult:
    """
    Run the continuous test with all workers.

    Args:
        cluster_state: The cluster state manager.
        point_ids: Set of point IDs to check for consistency.
        duration_seconds: How long to run the test.
        enable_restarts: Whether to enable the restart worker.
        enable_resharding: Whether to enable the resharding worker.
        enable_snapshots: Whether to enable the snapshot worker.

    Returns:
        TestResult with statistics from the run.
    """
    start_time = time.time()
    end_time = start_time + duration_seconds

    log("info", "Starting continuous test", duration_seconds=duration_seconds)

    # Create workers
    update_worker = UpdateWorker(
        peer_url_getter=cluster_state.get_available_urls,
        num_points=len(point_ids),
    )

    consistency_worker = ConsistencyCheckWorker(
        peer_url_getter=cluster_state.get_available_urls,
        point_ids=point_ids,
        update_worker=update_worker,  # Pass reference so it can pause/resume updates
    )

    restart_worker = RestartWorker(cluster_state) if enable_restarts else None
    resharding_worker = ReshardingWorker(cluster_state) if enable_resharding else None
    snapshot_worker = SnapshotWorker(
        peer_url_getter=cluster_state.get_available_urls,
    ) if enable_snapshots else None

    # Start all workers
    workers: list[BaseWorker] = [update_worker, consistency_worker]
    if restart_worker:
        workers.append(restart_worker)
    if resharding_worker:
        workers.append(resharding_worker)
    if snapshot_worker:
        workers.append(snapshot_worker)

    for worker in workers:
        worker.start()

    # Main loop - wait for duration or consistency failure
    try:
        while time.time() < end_time:
            if consistency_worker.failure_event.is_set():
                log("error", "Consistency check failed - stopping test")
                break
            sleep(1.0)
    finally:
        # Stop all workers
        for worker in workers:
            worker.stop()

    elapsed = time.time() - start_time

    return TestResult(
        duration_seconds=elapsed,
        update_stats=update_worker.stats,
        consistency_stats=consistency_worker.stats,
        restart_stats=restart_worker.stats if restart_worker else WorkerStats(),
        resharding_stats=resharding_worker.stats if resharding_worker else WorkerStats(),
        snapshot_stats=snapshot_worker.stats if snapshot_worker else WorkerStats(),
        inconsistencies=consistency_worker.inconsistencies,
        final_shard_count=resharding_worker.current_shard_count if resharding_worker else N_SHARDS,
    )


# =============================================================================
# Test Function
# =============================================================================

def test_resharding_continuous(tmp_path: pathlib.Path, every_test):
    """
    Duration-based test with continuous updates, consistency checks,
    node restarts, and resharding operations.

    Set TEST_DURATION_MINUTES environment variable to control duration.
    Default: 5 minutes.
    """
    assert_project_root()

    # Configuration
    duration_minutes = float(os.environ.get("TEST_DURATION_MINUTES", DEFAULT_TEST_DURATION_MINUTES))
    duration_seconds = duration_minutes * 60

    env = {"QDRANT__CLUSTER__RESHARDING_ENABLED": "true"}

    log("info", "Starting test", duration_minutes=duration_minutes)

    # Create cluster
    peer_urls, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path, N_PEERS, port_seed=6333, extra_env=env
    )

    cluster_state = ClusterState(
        peer_urls=peer_urls,
        peer_dirs=peer_dirs,
        bootstrap_uri=bootstrap_uri,
        env=env,
    )

    # Create collection
    create_collection(
        peer_urls[0],
        collection=COLLECTION_NAME,
        shard_number=N_SHARDS,
        replication_factor=N_REPLICAS,
    )

    wait_collection_exists_and_active_on_all_peers(
        collection_name=COLLECTION_NAME,
        peer_api_uris=peer_urls,
    )

    # Insert initial points
    points = create_points_with_timestamps(N_POINTS)
    resp = upsert_points(peer_urls[0], points)
    assert_http_ok(resp)

    all_point_ids: Set[PointId] = set(range(N_POINTS))

    # Wait for replication
    for peer_url in peer_urls:
        wait_for(
            lambda url=peer_url: get_collection_point_count(url, COLLECTION_NAME) == N_POINTS,
            wait_for_timeout=30,
        )

    # Verify initial consistency
    inconsistent = get_inconsistent_point_ids(peer_urls[0], all_point_ids)
    assert not inconsistent, f"Initial consistency check failed: {inconsistent}"

    log("info", "Initial setup complete, starting workers")

    # Run the test
    result = run_continuous_test(
        cluster_state=cluster_state,
        point_ids=all_point_ids,
        duration_seconds=duration_seconds,
        enable_restarts=True,
        enable_resharding=True,
    )

    result.print_summary()

    # Fail if consistency check failed
    if result.inconsistencies:
        # Preserve storage for investigation before cleanup
        preserved_path = cluster_state.preserve_storage()
        if preserved_path:
            print(f"\n*** Storage preserved at: {preserved_path} ***")
        assert False, f"Consistency check failed: {result.inconsistencies}"

    # Final verification
    log("info", "Waiting for cluster to stabilize...")
    sleep(5)

    final_urls = cluster_state.get_available_urls()
    for peer_url in final_urls:
        try:
            wait_for(
                check_all_replicas_active,
                peer_url,
                COLLECTION_NAME,
                wait_for_timeout=60,
            )
        except Exception as e:
            log("warn", f"Not all replicas active on {peer_url}: {e}")

    # Final consistency check
    log("info", "Running final consistency check...")
    inconsistent = get_inconsistent_point_ids(final_urls[0], all_point_ids)
    assert not inconsistent, f"Final consistency check failed: {inconsistent}"

    log("info", "Test PASSED")
    print(f"\nAll {N_POINTS} points consistent across {len(final_urls)} peers")