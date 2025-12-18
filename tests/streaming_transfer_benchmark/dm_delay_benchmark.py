#!/usr/bin/env python3
"""
Disk Latency Benchmark using dm-delay.

Tests streaming transfer throughput with various disk latencies using
the Linux device-mapper 'delay' target.

Usage:
    python dm_delay_benchmark.py --points 100000 --runs 3
    python dm_delay_benchmark.py --points 50000 --latencies "0:0,20:20" --async-scorer
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

import requests

_TESTS_DIR = Path(__file__).parent.parent
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))

if 'PYTEST_CURRENT_TEST' not in os.environ:
    os.environ['PYTEST_CURRENT_TEST'] = 'benchmark'

from consensus_tests.utils import (
    start_cluster, kill_all_processes, start_first_peer, start_peer
)
from benchmark import (
    Result, create_collection, upsert_points, run_transfer,
    cleanup_replica, drop_caches, wait_for_optimization, COLLECTION
)

# Constants
SETTLE_TIME_S = 2
INTER_RUN_DELAY_S = 1
DEFAULT_DISK_SIZE_GB = 15


def run_cmd(cmd: str, sudo: bool = False, check: bool = True) -> str:
    """Run shell command and return stdout."""
    if sudo:
        cmd = f"sudo {cmd}"
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=check)
    return r.stdout.strip()


def get_storage_types(uri: str) -> dict:
    """Get segment storage type counts from telemetry."""
    try:
        r = requests.get(f"{uri}/telemetry?details_level=6", timeout=10)
        if not r.ok:
            return {}

        for coll in r.json().get('result', {}).get('collections', {}).get('collections', []):
            if coll.get('id') == COLLECTION:
                types = {}
                for shard in coll.get('shards', []):
                    for seg in shard.get('local', {}).get('segments', []):
                        for vec_cfg in seg.get('config', {}).get('vector_data', {}).values():
                            st = vec_cfg.get('storage_type', 'unknown')
                            types[st] = types.get(st, 0) + 1
                return types
        return {}
    except Exception:
        return {}


def wait_for_mmap_segments(uri: str, timeout: int = 180) -> bool:
    """Wait for segments to use Mmap storage (required for io_uring)."""
    print("  Waiting for Mmap segments...", end='', flush=True)
    start = time.time()

    while time.time() - start < timeout:
        types = get_storage_types(uri)
        mmap = types.get('Mmap', 0)
        chunked = types.get('ChunkedMmap', 0) + types.get('InRamChunkedMmap', 0)

        if mmap > 0 and mmap >= chunked:
            print(f" done ({time.time() - start:.1f}s)")
            return True

        time.sleep(2)
        print(".", end='', flush=True)

    print(f" timeout")
    return False


def restart_cluster(peer_dirs: List[Path], port_seed: int,
                    memory_limit: str = None, extra_env: dict = None):
    """Restart cluster using existing peer directories."""
    peer_uris = []

    api_uri, bootstrap_uri = start_first_peer(
        peer_dirs[0], "peer_restart.log",
        port=port_seed, extra_env=extra_env, memory_limit=memory_limit
    )
    peer_uris.append(api_uri)

    for i in range(1, len(peer_dirs)):
        api_uri = start_peer(
            peer_dirs[i], f"peer_{i}_restart.log",
            bootstrap_uri, port=port_seed + i * 100,
            extra_env=extra_env, memory_limit=memory_limit
        )
        peer_uris.append(api_uri)

    # Wait for cluster ready
    timeout = 600
    start = time.time()
    while time.time() - start < timeout:
        try:
            all_ready = all(
                requests.get(f"{uri}/cluster", timeout=5).json()
                .get('result', {}).get('raft_info', {}).get('leader')
                for uri in peer_uris
            )
            if all_ready:
                return peer_uris, peer_dirs, bootstrap_uri
        except Exception:
            pass
        time.sleep(1)

    raise Exception(f"Cluster not ready after {timeout}s")


class DmDelay:
    """Manages a dm-delay device for disk latency injection."""

    def __init__(self, size_gb: int = DEFAULT_DISK_SIZE_GB,
                 mount_point: str = "/mnt/qdrant_delayed"):
        self.size_gb = size_gb
        self.mount_point = Path(mount_point)
        self.img_path = Path("/tmp/qdrant_delayed_disk.img")
        self.loop_dev: Optional[str] = None
        self.dm_name = "qdrant_delayed"

    def is_available(self) -> bool:
        """Check if dm-delay kernel module is available."""
        if run_cmd("dmsetup targets | grep -q delay", sudo=True, check=False) == "":
            return True
        run_cmd("modprobe dm-delay", sudo=True, check=False)
        return run_cmd("dmsetup targets | grep -q delay", sudo=True, check=False) == ""

    def setup(self, read_ms: int, write_ms: int) -> Optional[Path]:
        """Setup dm-delay device with specified latencies."""
        try:
            print(f"  Setting up dm-delay: read={read_ms}ms, write={write_ms}ms")
            self.teardown()

            # Create backing file if needed
            expected_size = self.size_gb * 1024 * 1024 * 1024
            if not self.img_path.exists() or self.img_path.stat().st_size != expected_size:
                print(f"  Creating {self.size_gb}GB disk image...")
                run_cmd(f"dd if=/dev/zero of={self.img_path} bs=1M count={self.size_gb * 1024} status=progress")

            # Setup loopback and dm-delay
            self.loop_dev = run_cmd(f"losetup --find --show {self.img_path}", sudo=True)

            if 'ext4' not in run_cmd(f"blkid {self.loop_dev}", sudo=True, check=False):
                run_cmd(f"mkfs.ext4 -q {self.loop_dev}", sudo=True)

            sectors = run_cmd(f"blockdev --getsz {self.loop_dev}", sudo=True)
            dm_table = f"0 {sectors} delay {self.loop_dev} 0 {read_ms} {self.loop_dev} 0 {write_ms}"
            run_cmd(f'echo "{dm_table}" | sudo dmsetup create {self.dm_name}')

            # Mount and set permissions
            run_cmd(f"mkdir -p {self.mount_point}", sudo=True)
            run_cmd(f"mount /dev/mapper/{self.dm_name} {self.mount_point}", sudo=True)
            run_cmd(f"chown -R {os.getenv('USER', 'root')}:{os.getenv('USER', 'root')} {self.mount_point}", sudo=True)

            print(f"  Mounted at: {self.mount_point}")
            return self.mount_point

        except Exception as e:
            print(f"  Error: {e}")
            self.teardown()
            return None

    def teardown(self):
        """Clean up dm-delay device."""
        run_cmd(f"umount {self.mount_point} 2>/dev/null", sudo=True, check=False)
        run_cmd(f"dmsetup remove {self.dm_name} 2>/dev/null", sudo=True, check=False)
        if self.loop_dev:
            run_cmd(f"losetup -d {self.loop_dev} 2>/dev/null", sudo=True, check=False)
            self.loop_dev = None


def run_single_config(
    dm: DmDelay, read_ms: int, write_ms: int,
    points: int, dims: int, runs: int,
    memory_limit: str = None, force_cold: bool = False, async_scorer: bool = False
) -> Optional[Result]:
    """Run benchmark for a single latency configuration."""
    print(f"\n{'='*50}")
    print(f"LATENCY: read={read_ms}ms, write={write_ms}ms")
    print(f"{'='*50}")

    delayed_path = dm.setup(read_ms, write_ms)
    if not delayed_path:
        return None

    cluster_dir = delayed_path / f"cluster_{read_ms}_{write_ms}"
    if cluster_dir.exists():
        shutil.rmtree(cluster_dir)
    cluster_dir.mkdir(parents=True)

    port_seed = 21000 + (read_ms * 100 + write_ms) * 10

    result = Result(f"latency_{read_ms}_{write_ms}", {
        'read_delay_ms': read_ms, 'write_delay_ms': write_ms,
        'points': points, 'force_cold': force_cold, 'async_scorer': async_scorer
    })

    extra_env = {"QDRANT__SERVICE__MAX_REQUEST_SIZE_MB": "256"}
    if async_scorer:
        extra_env["QDRANT__STORAGE__PERFORMANCE__ASYNC_SCORER"] = "true"

    uris = dirs = None

    try:
        for i in range(runs):
            print(f"\n  Run {i+1}/{runs}:")

            if i == 0:
                # First run: create cluster and data
                uris, dirs, _ = start_cluster(
                    cluster_dir, num_peers=2, port_seed=port_seed,
                    extra_env=extra_env, memory_limit=memory_limit
                )
                print(f"    Peers: {uris[0]}, {uris[1]}")

                create_collection(uris[0], dims)
                upsert_points(uris[0], points, dims)
                wait_for_optimization(uris[0])

                if async_scorer and not wait_for_mmap_segments(uris[0]):
                    print("  WARNING: io_uring requires Mmap storage")

                if force_cold:
                    kill_all_processes()
                    time.sleep(SETTLE_TIME_S)
                    drop_caches(cluster_dir, verbose=True)
                    uris, dirs, _ = restart_cluster(dirs, port_seed, memory_limit, extra_env)
                    time.sleep(SETTLE_TIME_S)
                else:
                    drop_caches(dirs[0] / "storage" / "collections")
                    time.sleep(SETTLE_TIME_S)

            elif force_cold:
                kill_all_processes()
                time.sleep(SETTLE_TIME_S)
                drop_caches(cluster_dir, verbose=True)
                uris, dirs, _ = restart_cluster(dirs, port_seed, memory_limit, extra_env)
                time.sleep(SETTLE_TIME_S)

            else:
                drop_caches(dirs[0] / "storage" / "collections", verbose=True)
                cleanup_replica(uris)
                time.sleep(INTER_RUN_DELAY_S)

            m = run_transfer(uris)
            m.num_points = points
            m.vector_dims = dims
            m.extra['read_delay_ms'] = read_ms
            m.extra['write_delay_ms'] = write_ms
            m.extra['run_type'] = 'cold_restart' if (force_cold or i == 0) else 'warm'
            result.runs.append(m)

        s = result.stats()
        print(f"\n  Avg: {s['throughput_mean']:,.0f} pts/s, {s['duration_mean']:.1f}s")
        return result

    finally:
        kill_all_processes()
        time.sleep(SETTLE_TIME_S)
        dm.teardown()


def run_dm_delay(
    points: int, dims: int, runs: int,
    latencies: List[Tuple[int, int]] = None,
    memory_limit: str = None, force_cold: bool = False, async_scorer: bool = False
) -> Result:
    """Run transfer benchmark with various disk latencies."""
    if latencies is None:
        latencies = [
            (0, 0), (1, 1), (5, 5), (10, 10), (20, 20),
            (0, 20), (20, 0),
        ]

    print(f"\n{'='*60}")
    print(f"DM-DELAY BENCHMARK: {points:,} pts, {dims}d, {runs} runs")
    if async_scorer:
        print(f"io_uring: ENABLED")
    print(f"{'='*60}")

    dm = DmDelay()
    if not dm.is_available():
        print("\nERROR: dm-delay not available. Try: sudo modprobe dm-delay")
        return Result("dm_delay", {'error': 'dm-delay not available'}, [])

    all_results = []
    try:
        for read_ms, write_ms in latencies:
            result = run_single_config(
                dm, read_ms, write_ms, points, dims, runs,
                memory_limit, force_cold, async_scorer
            )
            if result:
                all_results.append(result)
    except KeyboardInterrupt:
        print("\n\nInterrupted!")
    finally:
        dm.teardown()

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"{'Read':<8} {'Write':<8} {'Throughput':<15} {'vs Baseline':<12}")
    print("-" * 50)

    baseline = all_results[0].stats()['throughput_mean'] if all_results else 0
    for r in all_results:
        s = r.stats()
        read_ms = r.params.get('read_delay_ms', 0)
        write_ms = r.params.get('write_delay_ms', 0)
        tp = s['throughput_mean']
        if r == all_results[0]:
            print(f"{read_ms:<8} {write_ms:<8} {tp:>10,.0f} pts/s  baseline")
        else:
            pct = ((tp - baseline) / baseline) * 100 if baseline else 0
            print(f"{read_ms:<8} {write_ms:<8} {tp:>10,.0f} pts/s  {pct:>+.1f}%")

    return Result("dm_delay", {
        'points': points, 'dims': dims, 'latencies': latencies
    }, [m for r in all_results for m in r.runs])


def main():
    parser = argparse.ArgumentParser(
        description="Disk Latency Benchmark using dm-delay",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python dm_delay_benchmark.py --points 100000 --runs 3
  python dm_delay_benchmark.py --latencies "0:0,20:20" --async-scorer
  python dm_delay_benchmark.py --points 50000 --runs 2 --force-cold

Prerequisites:
  sudo modprobe dm-delay
  sudo dmsetup targets | grep delay
"""
    )
    parser.add_argument('--points', '-p', type=int, default=100_000)
    parser.add_argument('--dims', '-d', type=int, default=768)
    parser.add_argument('--runs', '-r', type=int, default=3)
    parser.add_argument('--latencies', type=str, default=None,
                        help="Comma-separated read:write pairs (e.g., '0:0,20:20')")
    parser.add_argument('--output', '-o', type=Path, default=Path("benchmark_results"))
    parser.add_argument('--memory-limit', '-m', type=str, default=None,
                        help="Memory limit per peer (e.g., '512M')")
    parser.add_argument('--force-cold', action='store_true',
                        help="Restart between runs for cold cache")
    parser.add_argument('--async-scorer', '--io-uring', action='store_true',
                        help="Enable io_uring async reads")
    args = parser.parse_args()

    latencies = None
    if args.latencies:
        latencies = [(int(r), int(w)) for r, w in
                     (p.split(':') for p in args.latencies.split(','))]

    result = run_dm_delay(
        args.points, args.dims, args.runs, latencies,
        args.memory_limit, args.force_cold, args.async_scorer
    )

    args.output.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = args.output / f"dm-delay_{ts}.json"
    output_file.write_text(result.to_json())
    print(f"\nSaved: {output_file}")


if __name__ == '__main__':
    main()