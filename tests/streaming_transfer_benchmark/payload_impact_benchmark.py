#!/usr/bin/env python3
"""
Payload Impact Benchmark - Tests if payload reads are a transfer bottleneck.

Compares transfer performance with different payload sizes.

Usage:
    python payload_impact_benchmark.py --points 100000 --runs 3
"""

import argparse
import json
import os
import random
import string
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import List

import requests

_TESTS_DIR = Path(__file__).parent.parent
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))

if 'PYTEST_CURRENT_TEST' not in os.environ:
    os.environ['PYTEST_CURRENT_TEST'] = 'benchmark'

from consensus_tests.utils import start_cluster, kill_all_processes
from consensus_tests.assertions import assert_http_ok

COLLECTION = "payload_bench"


def create_collection(uri: str, dims: int = 768):
    requests.delete(f"{uri}/collections/{COLLECTION}?timeout=60")
    r = requests.put(f"{uri}/collections/{COLLECTION}?timeout=60", json={
        "vectors": {"size": dims, "distance": "Cosine"},
        "shard_number": 1, "replication_factor": 1,
        "optimizers_config": {"indexing_threshold": 0},
    })
    assert_http_ok(r)


def generate_payload(payload_type: str) -> dict:
    if payload_type == "none":
        return {}
    elif payload_type == "small":
        return {"category": random.choice(["A", "B", "C", "D", "E"])}
    elif payload_type == "large":
        return {
            "category": random.choice(["A", "B", "C", "D", "E"]),
            "score": random.random(),
            "count": random.randint(0, 1000),
            "tags": [random.choice(["tag1", "tag2", "tag3"]) for _ in range(5)],
            "name": ''.join(random.choices(string.ascii_letters, k=50)),
            "description": ''.join(random.choices(string.ascii_letters + " ", k=200)),
            "metadata": {"f1": ''.join(random.choices(string.ascii_letters, k=30)),
                         "f2": random.randint(0, 10000)},
            "history": [random.random() for _ in range(10)],
            "flags": {f"flag_{i}": random.choice([True, False]) for i in range(5)},
        }
    raise ValueError(f"Unknown payload type: {payload_type}")


def upsert_points(uri: str, num: int, dims: int, payload_type: str, batch: int = 1000):
    for offset in range(0, num, batch):
        n = min(batch, num - offset)
        points = []
        for i in range(n):
            point = {"id": offset + i, "vector": [random.random() for _ in range(dims)]}
            payload = generate_payload(payload_type)
            if payload:
                point["payload"] = payload
            points.append(point)
        r = requests.put(f"{uri}/collections/{COLLECTION}/points?wait=true", json={"points": points})
        assert_http_ok(r)
        print(f"\r  Upsert ({payload_type}): {offset + n:,}/{num:,}", end='', flush=True)
    print()


def get_collection_cluster_info(uri: str) -> dict:
    r = requests.get(f"{uri}/collections/{COLLECTION}/cluster")
    assert_http_ok(r)
    return r.json()['result']


def run_transfer(uris: List[str]) -> dict:
    src_uri = dst_uri = shard_id = from_id = to_id = None

    for i, uri in enumerate(uris):
        info = get_collection_cluster_info(uri)
        if info.get('local_shards'):
            src_uri, dst_uri = uri, uris[1 - i]
            shard_id = info['local_shards'][0]['shard_id']
            from_id = info['peer_id']
            to_id = get_collection_cluster_info(dst_uri)['peer_id']
            break

    if src_uri is None:
        raise RuntimeError("No shard found")

    r = requests.post(f"{src_uri}/collections/{COLLECTION}/points/count", json={"exact": True})
    assert_http_ok(r)
    num_points = r.json()['result']['count']

    print(f"  Transfer: {from_id} -> {to_id} ({num_points:,} points)")
    start = time.time()

    r = requests.post(f"{src_uri}/collections/{COLLECTION}/cluster", json={
        "replicate_shard": {"shard_id": shard_id, "from_peer_id": from_id,
                           "to_peer_id": to_id, "method": "stream_records"}
    })
    assert_http_ok(r)

    while time.time() - start < 600:
        if not get_collection_cluster_info(src_uri).get('shard_transfers'):
            break
        time.sleep(0.5)

    duration = time.time() - start
    throughput = num_points / duration if duration > 0 else 0
    print(f"  Done: {duration:.2f}s, {throughput:,.0f} pts/s")

    return {'num_points': num_points, 'duration_s': round(duration, 3),
            'throughput_pts_s': round(throughput, 1)}


def cleanup_replica(uris: List[str]):
    try:
        peers = [(uri, get_collection_cluster_info(uri)) for uri in uris]
        peers_with_shards = [(u, i['peer_id'], i.get('local_shards', []))
                            for u, i in peers if i.get('local_shards')]
        if len(peers_with_shards) > 1:
            keep_uri = peers_with_shards[0][0]
            for _, peer_id, shards in peers_with_shards[1:]:
                for s in shards:
                    requests.post(f"{keep_uri}/collections/{COLLECTION}/cluster",
                                  json={"drop_replica": {"shard_id": s['shard_id'], "peer_id": peer_id}})
                    time.sleep(2)
    except Exception as e:
        print(f"  Cleanup warning: {e}")


def drop_caches():
    try:
        subprocess.run(["sudo", "-n", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"],
                       capture_output=True, timeout=10)
    except Exception:
        pass


def run_experiment(points: int, dims: int, runs: int, payload_types: List[str]) -> dict:
    print(f"\n{'='*60}")
    print(f"PAYLOAD IMPACT BENCHMARK: {points:,} pts, {dims}d, {runs} runs")
    print(f"{'='*60}")

    results = {'name': 'payload_impact', 'params': {'points': points, 'dims': dims}, 'configs': []}
    tmp = Path(tempfile.mkdtemp(prefix="payload_bench_"))

    try:
        uris, _, _ = start_cluster(tmp, num_peers=2)
        print(f"  Peers: {uris[0]}, {uris[1]}")

        for payload_type in payload_types:
            print(f"\n{'='*50}")
            print(f"PAYLOAD: {payload_type}")
            print(f"{'='*50}")

            create_collection(uris[0], dims)
            upsert_points(uris[0], points, dims, payload_type)
            time.sleep(2)

            config_results = {'payload_type': payload_type, 'runs': []}

            for i in range(runs):
                print(f"\n  Run {i+1}/{runs}:")
                drop_caches()
                cleanup_replica(uris)
                time.sleep(1)
                metrics = run_transfer(uris)
                metrics['run'] = i + 1
                config_results['runs'].append(metrics)

            throughputs = [r['throughput_pts_s'] for r in config_results['runs']]
            config_results['mean_throughput'] = round(sum(throughputs) / len(throughputs), 1)
            results['configs'].append(config_results)

    finally:
        kill_all_processes()

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"{'Payload':<12} {'Throughput':<15} {'vs None':<12}")
    print("-" * 40)

    baseline = next((c['mean_throughput'] for c in results['configs'] if c['payload_type'] == 'none'), None)
    for c in results['configs']:
        tp = c['mean_throughput']
        if c['payload_type'] == 'none':
            print(f"{c['payload_type']:<12} {tp:>10,.0f} pts/s  baseline")
        elif baseline:
            pct = ((tp - baseline) / baseline) * 100
            print(f"{c['payload_type']:<12} {tp:>10,.0f} pts/s  {pct:>+.1f}%")

    return results


def main():
    parser = argparse.ArgumentParser(description="Payload Impact Benchmark")
    parser.add_argument('--points', '-p', type=int, default=100_000)
    parser.add_argument('--dims', '-d', type=int, default=768)
    parser.add_argument('--runs', '-r', type=int, default=3)
    parser.add_argument('--payload-types', type=str, default="none,small,large")
    parser.add_argument('--output', '-o', type=Path, default=Path("benchmark_results"))
    args = parser.parse_args()

    results = run_experiment(args.points, args.dims, args.runs, args.payload_types.split(','))

    args.output.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = args.output / f"payload-impact_{ts}.json"
    output_file.write_text(json.dumps(results, indent=2))
    print(f"\nSaved: {output_file}")


if __name__ == '__main__':
    main()