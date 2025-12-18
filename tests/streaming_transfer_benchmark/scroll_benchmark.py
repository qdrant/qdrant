#!/usr/bin/env python3
"""
Scroll Benchmark - Tests read performance to isolate transfer bottleneck.

Usage:
    python scroll_benchmark.py --points 100000 --runs 3
    sudo python scroll_benchmark.py --points 50000 --read-latency 20 --compare
"""

import argparse
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

_TESTS_DIR = Path(__file__).parent.parent
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))

if 'PYTEST_CURRENT_TEST' not in os.environ:
    os.environ['PYTEST_CURRENT_TEST'] = 'benchmark'

from consensus_tests.utils import start_cluster, kill_all_processes, start_first_peer
from consensus_tests.assertions import assert_http_ok

COLLECTION = "scroll_bench"


def run_cmd(cmd: str, sudo: bool = False, check: bool = True) -> str:
    if sudo:
        cmd = f"sudo {cmd}"
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, check=check).stdout.strip()


def get_storage_types(uri: str) -> dict:
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


def wait_for_mmap_segments(uri: str, timeout: int = 120) -> bool:
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
    print(" timeout")
    return False


class DmDelay:
    def __init__(self, size_gb: int = 10, mount_point: str = "/mnt/qdrant_scroll_bench"):
        self.size_gb = size_gb
        self.mount_point = Path(mount_point)
        self.img_path = Path("/tmp/qdrant_scroll_bench.img")
        self.loop_dev: Optional[str] = None
        self.dm_name = "qdrant_scroll_bench"

    def is_available(self) -> bool:
        if run_cmd("dmsetup targets | grep -q delay", sudo=True, check=False) == "":
            return True
        run_cmd("modprobe dm-delay", sudo=True, check=False)
        return run_cmd("dmsetup targets | grep -q delay", sudo=True, check=False) == ""

    def setup(self, read_ms: int, write_ms: int) -> Optional[Path]:
        try:
            print(f"  Setting up dm-delay: read={read_ms}ms, write={write_ms}ms")
            self.teardown()

            expected_size = self.size_gb * 1024 * 1024 * 1024
            if not self.img_path.exists() or self.img_path.stat().st_size != expected_size:
                print(f"  Creating {self.size_gb}GB disk image...")
                run_cmd(f"dd if=/dev/zero of={self.img_path} bs=1M count={self.size_gb * 1024} status=progress")

            self.loop_dev = run_cmd(f"losetup --find --show {self.img_path}", sudo=True)
            if 'ext4' not in run_cmd(f"blkid {self.loop_dev}", sudo=True, check=False):
                run_cmd(f"mkfs.ext4 -q {self.loop_dev}", sudo=True)

            sectors = run_cmd(f"blockdev --getsz {self.loop_dev}", sudo=True)
            run_cmd(f'echo "0 {sectors} delay {self.loop_dev} 0 {read_ms} {self.loop_dev} 0 {write_ms}" | sudo dmsetup create {self.dm_name}')

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
        run_cmd(f"umount {self.mount_point} 2>/dev/null", sudo=True, check=False)
        run_cmd(f"dmsetup remove {self.dm_name} 2>/dev/null", sudo=True, check=False)
        if self.loop_dev:
            run_cmd(f"losetup -d {self.loop_dev} 2>/dev/null", sudo=True, check=False)
            self.loop_dev = None


def create_collection(uri: str, dims: int = 768):
    requests.delete(f"{uri}/collections/{COLLECTION}?timeout=60")
    r = requests.put(f"{uri}/collections/{COLLECTION}?timeout=60", json={
        "vectors": {"size": dims, "distance": "Cosine", "on_disk": True},
        "shard_number": 1, "replication_factor": 1,
        "optimizers_config": {"indexing_threshold": 1},
    })
    assert_http_ok(r)


def upsert_points(uri: str, num: int, dims: int, batch: int = 1000):
    for offset in range(0, num, batch):
        n = min(batch, num - offset)
        points = [{"id": offset + i, "vector": [random.random() for _ in range(dims)]} for i in range(n)]
        r = requests.put(f"{uri}/collections/{COLLECTION}/points?wait=true", json={"points": points})
        assert_http_ok(r)
        print(f"\r  Upsert: {offset + n:,}/{num:,}", end='', flush=True)
    print()


def wait_for_optimization(uri: str, timeout: int = 120):
    print("  Waiting for optimization...", end='', flush=True)
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"{uri}/collections/{COLLECTION}", timeout=5)
            if r.ok and r.json().get('result', {}).get('status') == 'green':
                print(f" done ({time.time() - start:.1f}s)")
                return True
        except Exception:
            pass
        time.sleep(1)
        print(".", end='', flush=True)
    print(" timeout")
    return False


def drop_caches():
    try:
        subprocess.run(["sudo", "-n", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"],
                       capture_output=True, timeout=10)
    except Exception:
        pass


def restart_peer(peer_dir: Path, port: int, extra_env: dict = None, memory_limit: str = None) -> str:
    api_uri, _ = start_first_peer(peer_dir, "peer_restart.log", port=port,
                                   extra_env=extra_env, memory_limit=memory_limit)
    start = time.time()
    while time.time() - start < 120:
        try:
            if requests.get(f"{api_uri}/collections", timeout=5).ok:
                return api_uri
        except Exception:
            pass
        time.sleep(1)
    raise Exception("Timeout waiting for peer")


def scroll_all(uri: str, batch_size: int, with_vectors: bool) -> dict:
    offset = None
    total_points = total_batches = 0
    start = time.time()

    while True:
        payload = {"limit": batch_size, "with_vector": with_vectors, "with_payload": False}
        if offset:
            payload["offset"] = offset
        r = requests.post(f"{uri}/collections/{COLLECTION}/points/scroll", json=payload)
        assert_http_ok(r)
        result = r.json()["result"]
        points = result.get("points", [])
        total_points += len(points)
        total_batches += 1
        offset = result.get("next_page_offset")
        if not offset or not points:
            break

    duration = time.time() - start
    return {
        "total_points": total_points, "total_batches": total_batches,
        "duration_s": round(duration, 3),
        "throughput_pts_s": round(total_points / duration, 1) if duration > 0 else 0,
    }


def run_experiment(points: int, dims: int, runs: int, batch_size: int, data_dir: Optional[Path],
                   memory_limit: Optional[str], async_scorer: bool,
                   read_latency_ms: int, write_latency_ms: int, force_cold: bool) -> dict:
    print(f"\n{'='*60}")
    print(f"SCROLL BENCHMARK: {points:,} pts, {dims}d, batch={batch_size}")
    if async_scorer:
        print(f"io_uring: ENABLED")
    if read_latency_ms:
        print(f"Latency: read={read_latency_ms}ms, write={write_latency_ms}ms")
    print(f"{'='*60}")

    results = {"name": "scroll_benchmark", "params": {
        "points": points, "dims": dims, "batch_size": batch_size,
        "async_scorer": async_scorer, "read_latency_ms": read_latency_ms,
    }, "configs": []}

    dm = None
    if read_latency_ms > 0 or write_latency_ms > 0:
        dm = DmDelay()
        if not dm.is_available():
            print("ERROR: dm-delay not available")
            return results
        delayed_path = dm.setup(read_latency_ms, write_latency_ms)
        if not delayed_path:
            return results
        data_dir = delayed_path / "scroll_bench"
        if data_dir.exists():
            shutil.rmtree(data_dir)
        data_dir.mkdir(parents=True)

    tmp = data_dir if data_dir else Path(tempfile.mkdtemp(prefix="scroll_bench_"))
    if data_dir:
        tmp.mkdir(parents=True, exist_ok=True)

    try:
        extra_env = {}
        if async_scorer:
            extra_env["QDRANT__STORAGE__PERFORMANCE__ASYNC_SCORER"] = "true"

        port_seed = 16100
        uris, dirs, _ = start_cluster(tmp, num_peers=1, port_seed=port_seed,
                                       extra_env=extra_env or None, memory_limit=memory_limit)
        uri, peer_dir = uris[0], dirs[0]
        print(f"  Peer: {uri}")

        create_collection(uri, dims)
        upsert_points(uri, points, dims)
        wait_for_optimization(uri)
        time.sleep(2)

        if async_scorer and not wait_for_mmap_segments(uri, timeout=180):
            print("  WARNING: io_uring requires Mmap storage")

        configs = [{"with_vectors": True, "label": "With vectors"}] if read_latency_ms else [
            {"with_vectors": False, "label": "IDs only"},
            {"with_vectors": True, "label": "With vectors"},
        ]

        for config in configs:
            print(f"\n{'='*50}")
            print(f"CONFIG: {config['label']}")
            print(f"{'='*50}")

            config_results = {"label": config["label"], "with_vectors": config["with_vectors"], "runs": []}

            for i in range(runs):
                print(f"\n  Run {i+1}/{runs}:")
                if force_cold and i > 0:
                    kill_all_processes()
                    time.sleep(2)
                    drop_caches()
                    uri = restart_peer(peer_dir, port_seed, extra_env or None, memory_limit)
                else:
                    drop_caches()
                time.sleep(1)

                metrics = scroll_all(uri, batch_size, config["with_vectors"])
                metrics["run"] = i + 1
                config_results["runs"].append(metrics)
                print(f"    {metrics['total_points']:,} pts, {metrics['throughput_pts_s']:,.0f} pts/s")

            throughputs = [r["throughput_pts_s"] for r in config_results["runs"]]
            config_results["mean_throughput"] = round(sum(throughputs) / len(throughputs), 1)
            results["configs"].append(config_results)

    finally:
        kill_all_processes()
        time.sleep(2)
        if dm:
            dm.teardown()

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    for c in results["configs"]:
        print(f"{c['label']:<20} {c['mean_throughput']:>10,.0f} pts/s")

    return results


def run_comparison(points: int, dims: int, runs: int, batch_size: int,
                   memory_limit: Optional[str], read_latency_ms: int,
                   write_latency_ms: int, force_cold: bool) -> dict:
    all_results = []

    print("\n" + "="*70)
    print("BASELINE (async_scorer=false)")
    print("="*70)
    baseline = run_experiment(points, dims, runs, batch_size, None, memory_limit,
                              False, read_latency_ms, write_latency_ms, force_cold)
    baseline["label"] = "baseline"
    all_results.append(baseline)

    time.sleep(5)

    print("\n" + "="*70)
    print("IO_URING (async_scorer=true)")
    print("="*70)
    io_uring = run_experiment(points, dims, runs, batch_size, None, memory_limit,
                              True, read_latency_ms, write_latency_ms, force_cold)
    io_uring["label"] = "io_uring"
    all_results.append(io_uring)

    # Comparison
    print("\n" + "="*70)
    print("COMPARISON")
    print("="*70)

    baseline_tp = next((c["mean_throughput"] for c in baseline.get("configs", [])
                        if c["label"] == "With vectors"), None)
    for r in all_results:
        tp = next((c["mean_throughput"] for c in r.get("configs", [])
                   if c["label"] == "With vectors"), None)
        if tp and baseline_tp:
            if r["label"] == "baseline":
                print(f"{r['label']:<15} {tp:>10,.0f} pts/s  baseline")
            else:
                pct = ((tp - baseline_tp) / baseline_tp) * 100
                print(f"{r['label']:<15} {tp:>10,.0f} pts/s  {pct:>+.1f}%")

    return {"name": "scroll_comparison", "params": {
        "points": points, "dims": dims, "read_latency_ms": read_latency_ms,
    }, "results": all_results}


def main():
    parser = argparse.ArgumentParser(description="Scroll Benchmark")
    parser.add_argument('--points', '-p', type=int, default=100_000)
    parser.add_argument('--dims', '-d', type=int, default=768)
    parser.add_argument('--runs', '-r', type=int, default=3)
    parser.add_argument('--batch-size', '-b', type=int, default=100)
    parser.add_argument('--data-dir', type=Path, default=None)
    parser.add_argument('--memory-limit', '-m', type=str, default=None)
    parser.add_argument('--async-scorer', '--io-uring', action='store_true')
    parser.add_argument('--read-latency', type=int, default=0)
    parser.add_argument('--write-latency', type=int, default=0)
    parser.add_argument('--compare', action='store_true')
    parser.add_argument('--force-cold', action='store_true')
    parser.add_argument('--output', '-o', type=Path, default=Path("benchmark_results"))
    args = parser.parse_args()

    if args.compare:
        results = run_comparison(args.points, args.dims, args.runs, args.batch_size,
                                 args.memory_limit, args.read_latency, args.write_latency, args.force_cold)
    else:
        results = run_experiment(args.points, args.dims, args.runs, args.batch_size, args.data_dir,
                                 args.memory_limit, args.async_scorer, args.read_latency,
                                 args.write_latency, args.force_cold)

    args.output.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "_comparison" if args.compare else ""
    latency = f"_lat{args.read_latency}" if args.read_latency else ""
    output_file = args.output / f"scroll{suffix}{latency}_{ts}.json"
    output_file.write_text(json.dumps(results, indent=2))
    print(f"\nSaved: {output_file}")


if __name__ == '__main__':
    main()