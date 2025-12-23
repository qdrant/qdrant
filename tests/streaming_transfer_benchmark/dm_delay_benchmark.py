#!/usr/bin/env python3
"""
Disk Latency Benchmark using dm-delay.

Tests streaming transfer throughput with various disk latencies using
the Linux device-mapper 'delay' target.

Usage:
    python dm_delay_benchmark.py --points 100000 --runs 3
    python dm_delay_benchmark.py --points 50000 --latencies "0:0,10:10,20:20"
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

_TESTS_DIR = Path(__file__).parent.parent
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))

if 'PYTEST_CURRENT_TEST' not in os.environ:
    os.environ['PYTEST_CURRENT_TEST'] = 'benchmark'

from consensus_tests.utils import start_cluster, kill_all_processes

from benchmark import (
    Result, create_collection, upsert_points, run_transfer, cleanup_replica, drop_caches
)

# --- Constants ---

# Time to wait for cluster/processes to settle after operations
SETTLE_TIME_S = 2

# Time between transfer runs to allow cleanup to complete
INTER_RUN_DELAY_S = 1

# Default disk image size (must fit test data + Qdrant overhead)
DEFAULT_DISK_SIZE_GB = 15


# --- Shell Command Helper ---

def run_cmd(cmd: str, sudo: bool = False, check: bool = True) -> str:
    """
    Run shell command and return stdout.

    Args:
        cmd: Command to run
        sudo: Prefix with sudo
        check: Raise on non-zero exit

    Returns:
        Stripped stdout
    """
    if sudo:
        cmd = f"sudo {cmd}"
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=check)
    return r.stdout.strip()


# --- dm-delay Device Management ---

class DmDelay:
    """
    Manages a dm-delay device for disk latency injection.

    Creates a loopback device with configurable read/write latency using
    the device-mapper 'delay' target.
    """

    def __init__(self, size_gb: int = DEFAULT_DISK_SIZE_GB,
                 mount_point: str = "/mnt/qdrant_delayed"):
        self.size_gb = size_gb
        self.mount_point = Path(mount_point)
        self.img_path = Path("/tmp/qdrant_delayed_disk.img")
        self.loop_dev: Optional[str] = None
        self.dm_name = "qdrant_delayed"
        self.is_setup = False

    def is_available(self) -> bool:
        """Check if dm-delay kernel module is available, try to load if not."""
        result = run_cmd("dmsetup targets | grep -q delay", sudo=True, check=False)
        if result == "":
            return True
        # Try to load the module
        run_cmd("modprobe dm-delay", sudo=True, check=False)
        return run_cmd("dmsetup targets | grep -q delay", sudo=True, check=False) == ""

    def setup(self, read_delay_ms: int, write_delay_ms: int) -> Optional[Path]:
        """
        Setup dm-delay device with specified latencies.

        Args:
            read_delay_ms: Latency to add to read operations (milliseconds)
            write_delay_ms: Latency to add to write operations (milliseconds)

        Returns:
            Path to mounted filesystem, or None on failure
        """
        try:
            print(f"  Setting up dm-delay: read={read_delay_ms}ms, write={write_delay_ms}ms")

            # Clean up any previous setup
            self.teardown()

            # Create backing file if needed
            expected_size = self.size_gb * 1024 * 1024 * 1024
            if not self.img_path.exists() or self.img_path.stat().st_size != expected_size:
                print(f"  Creating {self.size_gb}GB disk image...")
                run_cmd(f"dd if=/dev/zero of={self.img_path} bs=1M count={self.size_gb * 1024} status=progress")

            # Setup loopback device
            self.loop_dev = run_cmd(f"losetup --find --show {self.img_path}", sudo=True)
            print(f"  Loop device: {self.loop_dev}")

            # Format if needed
            if 'ext4' not in run_cmd(f"blkid {self.loop_dev}", sudo=True, check=False):
                print(f"  Formatting as ext4...")
                run_cmd(f"mkfs.ext4 -q {self.loop_dev}", sudo=True)

            # Get device size in sectors
            sectors = run_cmd(f"blockdev --getsz {self.loop_dev}", sudo=True)

            # Create dm-delay device
            dm_table = f"0 {sectors} delay {self.loop_dev} 0 {read_delay_ms} {self.loop_dev} 0 {write_delay_ms}"
            run_cmd(f'echo "{dm_table}" | sudo dmsetup create {self.dm_name}')
            print(f"  Created dm-delay device: /dev/mapper/{self.dm_name}")

            # Mount
            run_cmd(f"mkdir -p {self.mount_point}", sudo=True)
            run_cmd(f"mount /dev/mapper/{self.dm_name} {self.mount_point}", sudo=True)

            # Set permissions
            user = os.getenv('USER', 'root')
            run_cmd(f"chown -R {user}:{user} {self.mount_point}", sudo=True)

            self.is_setup = True
            print(f"  Mounted at: {self.mount_point}")
            return self.mount_point

        except subprocess.CalledProcessError as e:
            print(f"  Error setting up dm-delay: {e}")
            self.teardown()
            return None
        except Exception as e:
            print(f"  Unexpected error: {e}")
            self.teardown()
            return None

    def teardown(self):
        """Clean up dm-delay device, unmount, and detach loop device."""
        try:
            run_cmd(f"umount {self.mount_point} 2>/dev/null", sudo=True, check=False)
            run_cmd(f"dmsetup remove {self.dm_name} 2>/dev/null", sudo=True, check=False)
            if self.loop_dev:
                run_cmd(f"losetup -d {self.loop_dev} 2>/dev/null", sudo=True, check=False)
                self.loop_dev = None
            self.is_setup = False
            print(f"  dm-delay cleaned up")
        except Exception as e:
            print(f"  Warning during cleanup: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.teardown()


# --- Single Configuration Test ---

def run_single_config(
    dm: DmDelay,
    read_ms: int,
    write_ms: int,
    points: int,
    dims: int,
    runs: int
) -> Optional[Result]:
    """
    Run benchmark for a single latency configuration.

    Args:
        dm: DmDelay instance
        read_ms: Read delay in milliseconds
        write_ms: Write delay in milliseconds
        points: Number of points to transfer
        dims: Vector dimensions
        runs: Number of runs

    Returns:
        Result object or None on failure
    """
    print(f"\n{'='*50}")
    print(f"LATENCY: read={read_ms}ms, write={write_ms}ms")
    print(f"{'='*50}")

    delayed_path = dm.setup(read_ms, write_ms)
    if not delayed_path:
        print(f"  SKIP: Failed to setup dm-delay")
        return None

    cluster_dir = delayed_path / f"cluster_{read_ms}_{write_ms}"
    if cluster_dir.exists():
        shutil.rmtree(cluster_dir)
    cluster_dir.mkdir(parents=True)

    try:
        print(f"  Starting cluster on delayed storage...")
        uris, dirs, _ = start_cluster(cluster_dir, num_peers=2, port_seed=None)
        print(f"    Peer 0: {uris[0]}")
        print(f"    Peer 1: {uris[1]}")

        create_collection(uris[0], dims)
        upsert_points(uris[0], points, dims)
        time.sleep(SETTLE_TIME_S)

        result = Result(f"latency_{read_ms}_{write_ms}", {
            'read_delay_ms': read_ms,
            'write_delay_ms': write_ms,
            'points': points
        })

        for i in range(runs):
            print(f"\n  Run {i+1}/{runs}:")
            drop_caches(dirs[0] / "storage" / "collections")
            cleanup_replica(uris)
            time.sleep(INTER_RUN_DELAY_S)

            m = run_transfer(uris)
            m.num_points = points
            m.vector_dims = dims
            m.extra['read_delay_ms'] = read_ms
            m.extra['write_delay_ms'] = write_ms
            result.runs.append(m)

        s = result.stats()
        print(f"\n  Avg: {s['throughput_mean']:,.0f} pts/s, {s['duration_mean']:.1f}s")
        return result

    finally:
        print(f"  Stopping cluster...")
        kill_all_processes()
        time.sleep(SETTLE_TIME_S)
        dm.teardown()


# --- Main Benchmark ---

def run_dm_delay(
    points: int,
    dims: int,
    runs: int,
    latencies: List[Tuple[int, int]] = None
) -> Result:
    """
    Test transfer throughput with various disk latencies using dm-delay.

    Args:
        points: Number of points to transfer
        dims: Vector dimensions
        runs: Number of runs per latency configuration
        latencies: List of (read_delay_ms, write_delay_ms) tuples
    """
    if latencies is None:
        latencies = [
            (0, 0),      # Baseline (no delay)
            # (1, 1),      # Cloud Qdrant
            (5, 5),      # Light SSD
            (10, 10),    # Typical SSD
            (20, 20),    # Slow SSD / EBS gp3
            (50, 50),    # HDD-like
            (0, 20),     # Fast reads, slow writes (write bottleneck)
            (20, 0),     # Slow reads, fast writes (read bottleneck)
        ]

    print(f"\n{'='*60}")
    print(f"DM-DELAY DISK LATENCY EXPERIMENT")
    print(f"Points: {points:,}, Dims: {dims}, Runs per config: {runs}")
    print(f"Latency configs: {len(latencies)}")
    print(f"{'='*60}")

    dm = DmDelay()

    # Check availability
    if not dm.is_available():
        print("\nERROR: dm-delay kernel module not available")
        print("Try: sudo modprobe dm-delay")
        return Result("dm_delay", {'error': 'dm-delay not available'}, [])

    all_results = []
    try:
        for read_ms, write_ms in latencies:
            result = run_single_config(dm, read_ms, write_ms, points, dims, runs)
            if result:
                all_results.append(result)
    except KeyboardInterrupt:
        print("\n\nInterrupted! Cleaning up...")
    finally:
        dm.teardown()

    # Print summary
    print_summary(all_results)

    return Result("dm_delay", {
        'points': points,
        'dims': dims,
        'latencies': latencies
    }, [m for r in all_results for m in r.runs])


def print_summary(results: List[Result]):
    """Print formatted summary table."""
    print(f"\n{'='*60}")
    print("SUMMARY: Disk Latency Impact")
    print(f"{'='*60}")
    print(f"{'Read (ms)':<12} {'Write (ms)':<12} {'Throughput':<15} {'vs Baseline':<12}")
    print("-" * 55)

    if not results:
        print("No results collected.")
        return

    baseline_tp = results[0].stats()['throughput_mean']
    for r in results:
        s = r.stats()
        read_ms = r.params.get('read_delay_ms', 0)
        write_ms = r.params.get('write_delay_ms', 0)
        tp = s['throughput_mean']

        if baseline_tp > 0 and r != results[0]:
            pct = ((tp - baseline_tp) / baseline_tp) * 100
            print(f"{read_ms:<12} {write_ms:<12} {tp:>10,.0f} pts/s  {pct:>+6.1f}%")
        else:
            print(f"{read_ms:<12} {write_ms:<12} {tp:>10,.0f} pts/s  {'baseline':>8}")


# --- CLI ---

def main():
    parser = argparse.ArgumentParser(
        description="Disk Latency Benchmark using dm-delay",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default latencies
  python dm_delay_benchmark.py --points 100000 --runs 3

  # Run with custom latencies (read_ms:write_ms)
  python dm_delay_benchmark.py --latencies "0:0,10:10,20:20,50:50"

  # Quick test
  python dm_delay_benchmark.py --points 50000 --runs 1 --latencies "0:0,20:20"

Prerequisites:
  # Load dm-delay kernel module
  sudo modprobe dm-delay

  # Verify it's available
  sudo dmsetup targets | grep delay
"""
    )
    parser.add_argument('--points', '-p', type=int, default=100_000,
                        help="Number of points to transfer (default: 100000)")
    parser.add_argument('--dims', '-d', type=int, default=768,
                        help="Vector dimensions (default: 768)")
    parser.add_argument('--runs', '-r', type=int, default=3,
                        help="Number of runs per latency config (default: 3)")
    parser.add_argument('--latencies', type=str, default=None,
                        help="Comma-separated read:write latency pairs (e.g., '0:0,10:10,20:20')")
    parser.add_argument('--output', '-o', type=Path, default=Path("benchmark_results"),
                        help="Output directory for results (default: benchmark_results)")
    args = parser.parse_args()

    # Parse latencies
    latencies = None
    if args.latencies:
        latencies = []
        for pair in args.latencies.split(','):
            read_ms, write_ms = pair.split(':')
            latencies.append((int(read_ms), int(write_ms)))

    # Run benchmark
    result = run_dm_delay(args.points, args.dims, args.runs, latencies)

    # Save results
    args.output.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = args.output / f"dm-delay_{ts}.json"
    output_file.write_text(result.to_json())
    print(f"\nSaved: {output_file}")


if __name__ == '__main__':
    main()