#!/usr/bin/env python3
"""
Cloud Streaming Transfer Benchmark.

Measures shard transfer throughput on a remote Qdrant cluster.

Usage:
    python cloud_benchmark.py --nodes http://node1:6333 http://node2:6333 --api-key KEY
"""

import argparse
import json
import random
import re
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

COLLECTION = "benchmark"
DEFAULT_TIMEOUT = 60

API_KEY: Optional[str] = None
NODES: List[str] = []


def _headers() -> Dict[str, str]:
    return {"api-key": API_KEY} if API_KEY else {}


@dataclass
class Metrics:
    num_points: int = 0
    vector_dims: int = 768
    start_time: float = 0.0
    end_time: float = 0.0
    progress: List[Dict] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

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
        d = {
            'num_points': self.num_points,
            'vector_dims': self.vector_dims,
            'duration_s': round(self.duration, 3),
            'throughput_pts_s': round(self.throughput, 1),
            'mbps': round(self.mbps, 2),
        }
        d.update(self.extra)
        return d


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
            'duration_std': round(statistics.stdev(durations), 3) if len(durations) > 1 else 0,
            'throughput_mean': round(statistics.mean(throughputs), 1),
            'throughput_std': round(statistics.stdev(throughputs), 1) if len(throughputs) > 1 else 0,
        }

    def to_dict(self) -> Dict:
        return {'name': self.name, 'params': self.params, 'stats': self.stats(),
                'runs': [r.to_dict() for r in self.runs]}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


def get_cluster_info(uri: str) -> Dict:
    r = requests.get(f"{uri}/cluster", headers=_headers(), timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return r.json()['result']


def get_collection_cluster_info(uri: str) -> Dict:
    r = requests.get(f"{uri}/collections/{COLLECTION}/cluster", headers=_headers(), timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return r.json()['result']


def get_node_info(uri: str) -> Dict:
    try:
        cluster = get_cluster_info(uri)
        telemetry = requests.get(f"{uri}/telemetry", headers=_headers(), timeout=DEFAULT_TIMEOUT).json()
        return {
            'peer_id': cluster['peer_id'],
            'uri': uri,
            'version': telemetry.get('result', {}).get('app', {}).get('version', 'unknown'),
        }
    except Exception as e:
        return {'uri': uri, 'error': str(e)}


def create_collection(dims: int, shards: int = 3, rf: int = 2, on_disk: bool = True):
    uri = NODES[0]
    requests.delete(f"{uri}/collections/{COLLECTION}?timeout={DEFAULT_TIMEOUT}", headers=_headers())
    time.sleep(1)
    r = requests.put(f"{uri}/collections/{COLLECTION}?timeout={DEFAULT_TIMEOUT}", headers=_headers(), json={
        "vectors": {"size": dims, "distance": "Cosine", "on_disk": on_disk},
        "shard_number": shards, "replication_factor": rf,
        "optimizers_config": {"indexing_threshold": 1},
    })
    r.raise_for_status()
    print(f"  Created collection: {shards} shards, rf={rf}, {dims} dims, on_disk={on_disk}")


def upsert_points(num: int, dims: int, batch: int = 1000):
    uri = NODES[0]
    for offset in range(0, num, batch):
        n = min(batch, num - offset)
        points = [{"id": offset + i, "vector": [random.random() for _ in range(dims)]} for i in range(n)]
        r = requests.put(f"{uri}/collections/{COLLECTION}/points?wait=true", headers=_headers(),
                         json={"points": points}, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        print(f"\r  Upsert: {offset + n:,}/{num:,}", end='', flush=True)
    print()


def count_points() -> int:
    r = requests.post(f"{NODES[0]}/collections/{COLLECTION}/points/count", headers=_headers(),
                      json={"exact": True}, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return r.json()['result']['count']


def wait_for_optimization(uri: str, timeout: int = 600) -> bool:
    """Wait for segment optimization to complete (green status)."""
    print("  Waiting for optimization...", end='', flush=True)
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"{uri}/collections/{COLLECTION}", headers=_headers(), timeout=10)
            if r.ok and r.json().get('result', {}).get('status') == 'green':
                print(f" done ({time.time() - start:.1f}s)")
                return True
        except Exception:
            pass
        time.sleep(1)
        print(".", end='', flush=True)
    print(f" timeout after {timeout}s")
    return False


def get_transfer_progress(uri: str) -> Optional[Dict]:
    try:
        info = get_collection_cluster_info(uri)
        transfers = info.get('shard_transfers', [])
        if not transfers:
            return None
        t = transfers[0]
        result = {'method': t.get('method'), 'from': t.get('from'), 'to': t.get('to'), 'shard_id': t.get('shard_id')}
        match = re.search(r'\((\d+)/(\d+)\)', t.get('comment', ''))
        if match:
            result['done'] = int(match.group(1))
            result['total'] = int(match.group(2))
            result['progress_pct'] = round(result['done'] / result['total'] * 100, 1)
        return result
    except Exception:
        return None


def wait_for_transfer_complete(uri: str, timeout: int = 600) -> List[Dict]:
    progress_samples = []
    start = time.time()
    while time.time() - start < timeout:
        try:
            info = get_collection_cluster_info(uri)
            if not info.get('shard_transfers'):
                break
            p = get_transfer_progress(uri)
            if p:
                p['elapsed'] = time.time() - start
                progress_samples.append(p)
        except Exception as e:
            print(f"\n  Warning: {e}")
        time.sleep(0.5)
    return progress_samples


def start_transfer(uri: str, shard_id: int, from_peer: int, to_peer: int, method: str = "stream_records"):
    payload = {"replicate_shard": {"shard_id": shard_id, "from_peer_id": from_peer, "to_peer_id": to_peer, "method": method}}
    r = requests.post(f"{uri}/collections/{COLLECTION}/cluster", headers=_headers(), json=payload, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()


def drop_replica(shard_id: int, peer_id: int) -> bool:
    for uri in NODES:
        try:
            r = requests.post(f"{uri}/collections/{COLLECTION}/cluster", headers=_headers(),
                              json={"drop_replica": {"shard_id": shard_id, "peer_id": peer_id}}, timeout=DEFAULT_TIMEOUT)
            r.raise_for_status()
            return True
        except Exception:
            continue
    return False


def run_single_transfer(points: int, dims: int, from_peer: int, to_peer: int,
                        shard_id: int, src_uri: str, method: str = "stream_records") -> Metrics:
    m = Metrics(num_points=points, vector_dims=dims)
    m.extra = {'method': method, 'from_peer': from_peer, 'to_peer': to_peer, 'shard_id': shard_id}

    print(f"  Transfer shard {shard_id}: peer {from_peer} -> peer {to_peer}")

    m.start_time = time.time()
    start_transfer(src_uri, shard_id, from_peer, to_peer, method)
    m.progress = wait_for_transfer_complete(src_uri)
    m.end_time = time.time()

    print(f"  Done: {m.duration:.2f}s, {m.throughput:,.0f} pts/s, {m.mbps:.2f} MB/s")
    return m


def run_benchmark(points: int, dims: int, runs: int, method: str = "stream_records",
                  from_peer: Optional[int] = None, to_peer: Optional[int] = None,
                  skip_setup: bool = False) -> Result:
    print(f"\n{'='*60}")
    print(f"CLOUD TRANSFER BENCHMARK")
    print(f"{'='*60}")

    # Show nodes
    print(f"\nNodes:")
    peer_uris = {}
    for uri in NODES:
        info = get_node_info(uri)
        print(f"  {uri} -> peer {info.get('peer_id', '?')} (v{info.get('version', '?')})")
        if 'peer_id' in info:
            peer_uris[info['peer_id']] = uri

    print(f"\nParams: {points:,} pts, {dims}d, {runs} runs, method={method}")

    # Setup
    if not skip_setup:
        print(f"\nSetup:")
        create_collection(dims)
        upsert_points(points, dims)
        wait_for_optimization(NODES[0])
        time.sleep(2)
    else:
        actual = count_points()
        if actual != points:
            print(f"  Collection has {actual:,} points (expected {points:,})")
            points = actual

    # Find source/destination
    shard_id = src_uri = actual_from = actual_to = None
    peer_shards = {}

    for uri in NODES:
        info = get_collection_cluster_info(uri)
        peer_id = info['peer_id']
        local_shards = {s['shard_id'] for s in info.get('local_shards', [])}
        peer_shards[peer_id] = local_shards
        if local_shards and shard_id is None:
            shard_id = next(iter(local_shards))
            actual_from = peer_id
            src_uri = uri

    if shard_id is None:
        return Result("cloud_transfer", {'error': 'no shards'})

    if from_peer:
        actual_from = from_peer
        src_uri = peer_uris.get(actual_from, NODES[0])

    # Find destination
    for peer_id, shards in peer_shards.items():
        if peer_id != actual_from and shard_id not in shards:
            actual_to = peer_id
            break

    if actual_to is None:
        for peer_id in peer_shards:
            if peer_id != actual_from:
                actual_to = peer_id
                drop_replica(shard_id, actual_to)
                time.sleep(2)
                break

    if to_peer:
        actual_to = to_peer

    if actual_to is None:
        return Result("cloud_transfer", {'error': 'no destination'})

    print(f"\nTransfer: peer {actual_from} -> peer {actual_to}, shard {shard_id}")

    result = Result("cloud_transfer", {
        'points': points, 'dims': dims, 'method': method,
        'from_peer': actual_from, 'to_peer': actual_to,
    })

    for i in range(runs):
        print(f"\nRun {i+1}/{runs}:")
        if i > 0:
            drop_replica(shard_id, actual_to)
            time.sleep(2)
        m = run_single_transfer(points, dims, actual_from, actual_to, shard_id, src_uri, method)
        result.runs.append(m)

    # Summary
    s = result.stats()
    print(f"\n{'='*60}")
    print(f"SUMMARY: {s['throughput_mean']:,.0f} pts/s (std: {s['throughput_std']:,.0f})")
    print(f"{'='*60}")

    return result


def main():
    global API_KEY, NODES

    parser = argparse.ArgumentParser(description="Cloud Transfer Benchmark")
    parser.add_argument('--nodes', '-n', nargs='+', required=True,
                        help="Node URIs (e.g., https://node1:6333 https://node2:6333)")
    parser.add_argument('--api-key', '-k', type=str, default=None)
    parser.add_argument('--points', '-p', type=int, default=100_000)
    parser.add_argument('--dims', '-d', type=int, default=768)
    parser.add_argument('--runs', '-r', type=int, default=3)
    parser.add_argument('--method', '-m', type=str, default="stream_records",
                        choices=["stream_records", "wal_delta", "resharding_stream_records"])
    parser.add_argument('--from-peer', type=int, default=None)
    parser.add_argument('--to-peer', type=int, default=None)
    parser.add_argument('--skip-setup', action='store_true')
    parser.add_argument('--output', '-o', type=Path, default=Path("benchmark_results"))
    args = parser.parse_args()

    if len(args.nodes) < 2:
        parser.error("At least 2 nodes required")

    API_KEY = args.api_key
    NODES = args.nodes

    result = run_benchmark(
        args.points, args.dims, args.runs, args.method,
        args.from_peer, args.to_peer, args.skip_setup
    )

    args.output.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = args.output / f"cloud-transfer_{ts}.json"
    output_file.write_text(result.to_json())
    print(f"\nSaved: {output_file}")


if __name__ == '__main__':
    main()