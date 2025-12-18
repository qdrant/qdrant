#!/usr/bin/env python3
"""Streaming Transfer Benchmark - measures shard transfer performance."""

import argparse
import json
import os
import random
import re
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests

_TESTS_DIR = Path(__file__).parent.parent
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))

if 'PYTEST_CURRENT_TEST' not in os.environ:
    os.environ['PYTEST_CURRENT_TEST'] = 'benchmark'

from consensus_tests.utils import (
    get_collection_cluster_info,
    start_cluster,
    kill_all_processes,
)
from consensus_tests.assertions import assert_http_ok

COLLECTION = "bench"


# --- Metrics ---

@dataclass
class Metrics:
    num_points: int = 0
    vector_dims: int = 768
    batch_size: int = 100
    start_time: float = 0.0
    end_time: float = 0.0
    progress: List[Dict] = field(default_factory=list)

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time

    @property
    def throughput(self) -> float:
        return self.num_points / self.duration if self.duration > 0 else 0

    @property
    def mbps(self) -> float:
        bytes_tx = self.num_points * self.vector_dims * 4
        return (bytes_tx / 1024 / 1024) / self.duration if self.duration > 0 else 0

    def to_dict(self) -> Dict:
        return {
            'num_points': self.num_points,
            'vector_dims': self.vector_dims,
            'duration_s': round(self.duration, 3),
            'throughput_pts_s': round(self.throughput, 1),
            'mbps': round(self.mbps, 2),
        }


@dataclass
class Result:
    name: str
    params: Dict[str, Any] = field(default_factory=dict)
    runs: List[Metrics] = field(default_factory=list)

    def stats(self) -> Dict:
        if not self.runs:
            return {}
        durations = [r.duration for r in self.runs]
        throughputs = [r.throughput for r in self.runs]
        return {
            'runs': len(self.runs),
            'duration_mean': round(statistics.mean(durations), 3),
            'duration_std': round(statistics.stdev(durations), 4) if len(durations) > 1 else 0,
            'throughput_mean': round(statistics.mean(throughputs), 1),
            'throughput_std': round(statistics.stdev(throughputs), 1) if len(throughputs) > 1 else 0,
        }

    def to_dict(self) -> Dict:
        return {'name': self.name, 'params': self.params, 'stats': self.stats(),
                'runs': [r.to_dict() for r in self.runs]}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


# --- Transfer Operations ---

def create_collection(uri: str, dims: int = 768, shards: int = 3, rf: int = 2):
    requests.delete(f"{uri}/collections/{COLLECTION}?timeout=60")
    r = requests.put(f"{uri}/collections/{COLLECTION}?timeout=60", json={
        "vectors": {"size": dims, "distance": "Cosine"},
        "shard_number": shards,
        "replication_factor": rf,
        "optimizers_config": {"indexing_threshold": 0},
    })
    assert_http_ok(r)


def upsert_points(uri: str, num: int, dims: int = 768, batch: int = 1000):
    for offset in range(0, num, batch):
        n = min(batch, num - offset)
        points = [{"id": offset + i, "vector": [random.random() for _ in range(dims)], "payload": {}}
                  for i in range(n)]
        r = requests.put(f"{uri}/collections/{COLLECTION}/points?wait=true", json={"points": points})
        assert_http_ok(r)
        print(f"\r  Upsert: {offset + n:,}/{num:,}", end='')
    print()


def get_transfer_progress(uri: str) -> Optional[Dict]:
    try:
        info = get_collection_cluster_info(uri, COLLECTION)
        transfers = info.get('shard_transfers', [])
        if not transfers:
            return None
        t = transfers[0]
        result = {'method': t.get('method'), 'from': t.get('from'), 'to': t.get('to')}
        match = re.search(r'\((\d+)/(\d+)\)', t.get('comment', ''))
        if match:
            result['done'] = int(match.group(1))
            result['total'] = int(match.group(2))
        return result
    except:
        return None


def run_transfer(uris: List[str], method: str = "stream_records") -> Metrics:
    src = get_collection_cluster_info(uris[0], COLLECTION)
    dst = get_collection_cluster_info(uris[1], COLLECTION)

    shard_id = src['local_shards'][0]['shard_id']
    from_id, to_id = src['peer_id'], dst['peer_id']

    m = Metrics()
    r = requests.post(f"{uris[0]}/collections/{COLLECTION}/points/count", json={"exact": True})
    assert_http_ok(r)
    m.num_points = r.json()['result']['count']

    print(f"  Transfer shard {shard_id}: {from_id} -> {to_id}")
    m.start_time = time.time()

    r = requests.post(f"{uris[0]}/collections/{COLLECTION}/cluster", json={
        "replicate_shard": {"shard_id": shard_id, "from_peer_id": from_id, "to_peer_id": to_id, "method": method}
    })
    assert_http_ok(r)

    while time.time() - m.start_time < 600:
        info = get_collection_cluster_info(uris[0], COLLECTION)
        if not info.get('shard_transfers'):
            break
        p = get_transfer_progress(uris[0])
        if p:
            m.progress.append({**p, 't': time.time()})
        time.sleep(0.5)

    m.end_time = time.time()
    print(f"  Done: {m.duration:.2f}s, {m.throughput:,.0f} pts/s")
    return m


def cleanup_replica(uris: List[str], shard_id: int = 0):
    try:
        info = get_collection_cluster_info(uris[1], COLLECTION)
        if any(s['shard_id'] == shard_id for s in info.get('local_shards', [])):
            r = requests.post(f"{uris[0]}/collections/{COLLECTION}/cluster",
                json={"drop_replica": {"shard_id": shard_id, "peer_id": info['peer_id']}})
            assert_http_ok(r)
            time.sleep(2)
    except:
        pass


def drop_caches():
    try:
        subprocess.run(["sudo", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"],
                       check=True, capture_output=True)
        print("  Dropped caches")
        return True
    except:
        print("  Warning: couldn't drop caches")
        return False


def warm_cache(data_dir: Path):
    if not data_dir.exists():
        return
    for f in data_dir.rglob("*"):
        if f.is_file() and f.suffix in ['.dat', '.bin', '']:
            try:
                with open(f, 'rb') as fh:
                    while fh.read(1024 * 1024):
                        pass
            except:
                pass
    print("  Warmed cache")


# --- Experiments ---

def run_baseline(uris: List[str], dirs: List[Path], points: int, dims: int, runs: int) -> Result:
    print(f"\n{'='*50}\nBASELINE: {points:,} points, {dims} dims\n{'='*50}")

    create_collection(uris[0], dims)
    upsert_points(uris[0], points, dims)
    time.sleep(2)

    result = Result("baseline", {'points': points, 'dims': dims})
    for i in range(runs):
        print(f"\nRun {i+1}/{runs}:")
        cleanup_replica(uris)
        time.sleep(1)
        m = run_transfer(uris)
        m.num_points = points
        m.vector_dims = dims
        result.runs.append(m)

    s = result.stats()
    print(f"\nResults: {s['duration_mean']:.2f}s avg, {s['throughput_mean']:,.0f} pts/s")
    return result


def run_disk_latency(uris: List[str], dirs: List[Path], points: int, dims: int, runs: int) -> Result:
    print(f"\n{'='*50}\nDISK LATENCY: warm vs cold cache\n{'='*50}")

    create_collection(uris[0], dims)
    upsert_points(uris[0], points, dims)
    time.sleep(2)

    results = []
    for cache_state in ['warm', 'cold']:
        print(f"\n--- {cache_state.upper()} CACHE ---")
        result = Result(f"{cache_state}_cache", {'cache': cache_state, 'points': points})

        for i in range(runs):
            print(f"\nRun {i+1}/{runs}:")
            cleanup_replica(uris)
            time.sleep(1)

            if cache_state == 'warm' and dirs:
                warm_cache(dirs[0] / "storage" / "collections")
            elif cache_state == 'cold':
                drop_caches()
            time.sleep(1)

            m = run_transfer(uris)
            m.num_points = points
            m.vector_dims = dims
            result.runs.append(m)

        results.append(result)

    # Summary
    warm_s, cold_s = results[0].stats(), results[1].stats()
    print(f"\n{'='*50}\nSUMMARY")
    print(f"Warm: {warm_s['throughput_mean']:,.0f} pts/s")
    print(f"Cold: {cold_s['throughput_mean']:,.0f} pts/s")
    if cold_s['throughput_mean'] > 0:
        ratio = warm_s['throughput_mean'] / cold_s['throughput_mean']
        print(f"Ratio: {ratio:.2f}x")

    return Result("disk_latency", {'points': points},
                  [m for r in results for m in r.runs])


def run_vector_size(uris: List[str], dirs: List[Path], points: int, runs: int) -> Result:
    print(f"\n{'='*50}\nVECTOR SIZE EXPERIMENT\n{'='*50}")

    all_runs = []
    for dims in [128, 384, 768, 1536]:
        print(f"\n--- {dims} dims ---")
        create_collection(uris[0], dims)
        upsert_points(uris[0], points, dims)
        time.sleep(2)

        for i in range(runs):
            print(f"  Run {i+1}/{runs}:")
            cleanup_replica(uris)
            time.sleep(1)
            m = run_transfer(uris)
            m.num_points = points
            m.vector_dims = dims
            all_runs.append(m)
            print(f"    {m.mbps:.2f} MB/s")

    return Result("vector_size", {'points': points}, all_runs)


# --- Main ---

def main():
    parser = argparse.ArgumentParser(description="Streaming Transfer Benchmark")
    parser.add_argument('experiment', choices=['baseline', 'disk-latency', 'vector-size', 'all'])
    parser.add_argument('--points', '-p', type=int, default=100_000)
    parser.add_argument('--dims', '-d', type=int, default=768)
    parser.add_argument('--runs', '-r', type=int, default=3)
    parser.add_argument('--output', '-o', type=Path, default=Path("benchmark_results"))
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"{'='*50}")
    print(f"STREAMING TRANSFER BENCHMARK")
    print(f"Experiment: {args.experiment}, Points: {args.points:,}, Dims: {args.dims}, Runs: {args.runs}")
    print(f"{'='*50}")

    tmp = Path(tempfile.mkdtemp(prefix="qdrant_bench_"))

    try:
        print(f"\nStarting cluster in {tmp}")
        uris, dirs, _ = start_cluster(tmp, num_peers=2, port_seed=None)
        print(f"  Peer 0: {uris[0]}\n  Peer 1: {uris[1]}")

        results = []

        if args.experiment in ['baseline', 'all']:
            r = run_baseline(uris, dirs, args.points, args.dims, args.runs)
            results.append(('baseline', r))

        if args.experiment in ['disk-latency', 'all']:
            r = run_disk_latency(uris, dirs, args.points, args.dims, args.runs)
            results.append(('disk-latency', r))

        if args.experiment in ['vector-size', 'all']:
            r = run_vector_size(uris, dirs, args.points, args.runs)
            results.append(('vector-size', r))

        for name, r in results:
            f = args.output / f"{name}_{ts}.json"
            f.write_text(r.to_json())
            print(f"\nSaved: {f}")

    finally:
        print("\nStopping cluster...")
        kill_all_processes()


if __name__ == '__main__':
    main()