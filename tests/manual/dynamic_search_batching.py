#!/usr/bin/env python3
"""Isolated HTTP fixture, correctness checks and closed-loop B1 load generator.

Requires numpy and Linux CPUs 2-4 by default, with a separate client CPU mask.
QDRANT_BATCH_SERVER_CPUS selects an alternative three-CPU server mask.
Set QDRANT_BATCH_RESULTS to a private output directory shared across stages.
Run seed, reference --max 0, check --max 64, then repeated measure stages.
The maximum is the physical batch cap, while every external request remains B1.
Results include full-cohort throughput, latency, source/binary hashes and validation.
"""
import argparse
import concurrent.futures as cf
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parent
DATA = Path(os.environ.get('QDRANT_BATCH_RESULTS', ROOT / 'dynamic-search-batching-results')).resolve()
DATA.mkdir(parents=True, exist_ok=True)
if not (DATA / '.gitignore').exists():
    (DATA / '.gitignore').write_text('*\n')
PORT = int(os.environ.get('QDRANT_BATCH_PORT', '16433'))
SERVER_CPUS = os.environ.get('QDRANT_BATCH_SERVER_CPUS', '2,3,4')
COLL = 'dynamic_sq8'
LOCAL = threading.local()


def call(method, path, body=None):
    """Send one persistent loopback HTTP request and preserve error bodies."""
    if not hasattr(LOCAL, 'connection'):
        LOCAL.connection = http.client.HTTPConnection('127.0.0.1', PORT, timeout=180)
    connection = LOCAL.connection
    content = json.dumps(body, separators=(',', ':')) if body is not None else None
    connection.request(method, path, content, {'Content-Type': 'application/json'})
    response = connection.getresponse()
    result = json.loads(response.read())
    if response.status != 200:
        raise RuntimeError((response.status, result))
    return result


def start(binary, maximum, label, debug=False):
    """Start a private server pinned to the same three logical CPUs in every run."""
    with socket.socket() as probe:
        if probe.connect_ex(('127.0.0.1', PORT)) == 0:
            raise RuntimeError(f'port {PORT} is already occupied')
    config = DATA / f'{label}.yaml'
    # JSON is valid YAML and avoids adding a YAML dependency to the runner.
    config.write_text(json.dumps({
        'telemetry_disabled': True,
        'log_level': 'debug,actix_web=warn' if debug else 'warn',
        'storage': {'storage_path': str(DATA / 'storage'),
                    'snapshots_path': str(DATA / 'snapshots'),
                    'performance': {'max_search_threads': 3,
                                    'dynamic_search_batch_max': maximum}},
        'service': {'host': '127.0.0.1', 'http_port': PORT, 'grpc_port': PORT + 1,
                    'max_workers': 3, 'hardware_reporting': True},
        'cluster': {'enabled': False},
    }, indent=2))
    log = (DATA / f'{label}.log').open('w')
    process = subprocess.Popen(['taskset', '-c', SERVER_CPUS, binary, '--config-path', str(config)],
                               stdout=log, stderr=subprocess.STDOUT,
                               cwd=ROOT.parents[1])
    for _ in range(2400):
        if process.poll() is not None:
            raise RuntimeError(f'server exited: see {label}.log')
        try:
            probe = http.client.HTTPConnection('127.0.0.1', PORT, timeout=1)
            probe.request('GET', '/readyz')
            ready = probe.getresponse()
            ready.read()
            probe.close()
            if ready.status == 200:
                if len(os.sched_getaffinity(process.pid)) != 3:
                    process.terminate()
                    process.wait(timeout=60)
                    raise RuntimeError('the server must have exactly three logical CPUs')
                return process, log
        except Exception:
            if hasattr(LOCAL, 'connection'):
                LOCAL.connection.close()
                del LOCAL.connection
        time.sleep(0.25)
    process.terminate()
    process.wait(timeout=60)
    raise RuntimeError('server readiness timeout')


def validate_cpu_affinity(server_pid):
    """Reject client/server contention using actual process affinities before generating load."""
    server_cpus = os.sched_getaffinity(server_pid)
    client_cpus = os.sched_getaffinity(0)
    overlap = server_cpus & client_cpus
    if overlap:
        raise ValueError(f'client and server CPU masks overlap on {sorted(overlap)}; pin the client to separate CPUs')


def stop(server):
    """Gracefully stop this runner's process without touching other Qdrant instances."""
    process, log = server
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=60)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        raise
    finally:
        log.close()
        if hasattr(LOCAL, 'connection'):
            LOCAL.connection.close()
            del LOCAL.connection


def queries():
    """Use a fixed independent query pool across modes and concurrency levels."""
    import numpy as np
    rng = np.random.default_rng(77)
    q = rng.standard_normal((128, 512), dtype=np.float32)
    q /= np.linalg.norm(q, axis=1, keepdims=True)
    return q.tolist()


def seed(count):
    """Ingest normalized vectors once, then wait for scalar quantization to finish."""
    import numpy as np
    existing = call('GET', '/collections')['result']['collections']
    if any(c['name'] == COLL for c in existing):
        call('DELETE', f'/collections/{COLL}')
    call('PUT', f'/collections/{COLL}', {
        'vectors': {'size': 512, 'distance': 'Dot'},
        'shard_number': 1,
        'hnsw_config': {'m': 0},
        'quantization_config': {'scalar': {'type': 'int8', 'always_ram': True}},
        'optimizers_config': {'default_segment_number': 1, 'indexing_threshold': 0,
                              'max_optimization_threads': 1, 'max_segment_size': 10000000},
    })
    rng = np.random.default_rng(42)
    for begin in range(0, count, 2000):
        end = min(begin + 2000, count)
        vectors = rng.standard_normal((end-begin, 512), dtype=np.float32)
        vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
        call('PUT', f'/collections/{COLL}/points?wait=true', {
            'batch': {'ids': list(range(begin, end)), 'vectors': vectors.tolist(),
                      'payloads': [{'bucket': i % 100} for i in range(begin, end)]}})
        if begin % 100000 == 0:
            print(f'ingested {end}/{count}', flush=True)
    call('PUT', f'/collections/{COLL}/index?wait=true', {
        'field_name': 'bucket', 'field_schema': 'integer'})
    call('PATCH', f'/collections/{COLL}', {
        'optimizers_config': {'indexing_threshold': 1}})
    for _ in range(1200):
        info = call('GET', f'/collections/{COLL}')['result']
        (DATA / 'collection.json').write_text(json.dumps(info, indent=2))
        if (info['status'] == 'green' and info['points_count'] == count
                and info['indexed_vectors_count'] == count and info['segments_count'] <= 2):
            # The optimizer can report green before starting its first job.
            files = list((DATA / 'storage').rglob('quantized.data'))
            if files:
                print('fixture ready', count, 'segments', info['segments_count'], flush=True)
                return
        time.sleep(1)
    raise RuntimeError('fixture optimization timeout')


def body(query, filtered=False, **changes):
    """Make one B1 SQ8 request; exact=false is essential because exact bypasses SQ8."""
    value = {'vector': query, 'limit': 10, 'with_payload': False,
             'params': {'exact': False, 'quantization': {'ignore': False, 'rescore': False}}}
    if filtered:
        value['filter'] = {'must': [{'key': 'bucket', 'range': {'lt': 90}}]}
    value.update(changes)
    return value


def fingerprint(points):
    """Compare ordered IDs and exact f32 score bits, including returned metadata."""
    return [(p['id'], struct.pack('<f', p['score']).hex(), p.get('payload'), p.get('vector'))
            for p in points]


def reference(pool):
    """Record single-request references before any timed load."""
    refs = {}
    for filtered in [False, True]:
        refs[str(filtered)] = [fingerprint(call('POST', f'/collections/{COLL}/points/search',
                                               body(q, filtered))['result']) for q in pool]
    (DATA / 'references.json').write_text(json.dumps(refs))
    return refs


def run_case(pool, refs, concurrency, seconds, filtered):
    """Maintain at most C outstanding B1 HTTP calls and drain every admitted request."""
    results = []
    origin = time.perf_counter()
    deadline = origin + seconds
    gate = threading.Barrier(concurrency + 1)
    lock = threading.Lock()
    issued = 0

    def worker():
        """Send one request at a time while participating in the fixed outstanding-request budget."""
        nonlocal issued
        local = []
        gate.wait()
        while time.perf_counter() < deadline:
            with lock:
                index = issued
                issued += 1
            query_id = index % len(pool)
            begin = time.perf_counter()
            try:
                response = call('POST', f'/collections/{COLL}/points/search', body(pool[query_id], filtered))
                local.append((query_id, time.perf_counter() - begin, response, None))
            except Exception as error:
                local.append((query_id, time.perf_counter() - begin, None, repr(error)))
        return local

    with cf.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker) for _ in range(concurrency)]
        origin = time.perf_counter()
        deadline = origin + seconds
        gate.wait()
        for future in futures:
            results.extend(future.result())
    elapsed = time.perf_counter() - origin
    # Validation is outside the measured interval, and no requests are silently discarded.
    errors = [r[3] for r in results if r[3]]
    mismatches = sum(fingerprint(r[2]['result']) != [tuple(p) for p in refs[str(filtered)][r[0]]]
                     for r in results if not r[3])
    latencies = sorted(r[1] * 1000 for r in results)
    def percentile(p):
        """Select an observed latency rank from the fully drained cohort."""
        return latencies[min(len(latencies)-1, int((len(latencies)-1)*p))]
    digest = hashlib.sha256(json.dumps([(r[0], fingerprint(r[2]['result'])) for r in results if not r[3]], separators=(',', ':')).encode()).hexdigest()
    result = {'response_sha256': digest, 'concurrency': concurrency, 'filtered': filtered, 'requests': len(results),
              'seconds': elapsed, 'qps': len(results)/elapsed,
              'p50_ms': percentile(.5), 'p95_ms': percentile(.95), 'p99_ms': percentile(.99),
              'errors': errors, 'mismatches': mismatches}
    if errors or mismatches:
        raise RuntimeError(result)
    return result


def variants(pool):
    """Exercise incompatible filters, metadata, exact searches and both HTTP endpoints."""
    cases = []
    for i in range(64):
        changes = [ {}, {'limit': 7}, {'offset': 3}, {'with_payload': True},
                    {'score_threshold': 0.12}, {'with_vector': True},
                    {'params': {'exact': True}},
                    {'filter': {'must': [{'key': 'bucket', 'match': {'value': 3}}]}} ][i % 8]
        b = body(pool[i % len(pool)], i % 2 == 0, **changes)
        endpoint = 'search' if i % 3 else 'query'
        if endpoint == 'query':
            b['query'] = b.pop('vector')
        cases.append((f'/collections/{COLL}/points/{endpoint}', b))
    return cases


def points(response):
    """Normalize legacy and universal API envelopes."""
    r = response['result']
    return r['points'] if isinstance(r, dict) else r


def varied_reference(pool):
    """Write serialized references from the feature-disabled server."""
    result = [fingerprint(points(call('POST', path, b))) for path, b in variants(pool)]
    (DATA / 'varied-references.json').write_text(json.dumps(result))


def hardware():
    """Read the collection aggregate without generating search usage."""
    # The main thread is idle during load; don't reuse an expired keep-alive socket.
    if hasattr(LOCAL, 'connection'):
        LOCAL.connection.close()
        del LOCAL.connection
    return call('GET', '/telemetry?details_level=2')['result']['hardware']['collection_data'][COLL]


def check(pool):
    """Check simultaneous incompatible requests, errors and exact resource attribution."""
    cases = variants(pool)
    references = json.loads((DATA / 'varied-references.json').read_text())
    # A fresh server creates the collection usage entry lazily on its first request.
    call('POST', *cases[0])
    before = hardware()
    with cf.ThreadPoolExecutor(max_workers=128) as executor:
        futures = [executor.submit(call, 'POST', *cases[i % len(cases)]) for i in range(256)]
        responses = [f.result() for f in futures]
    for i, response in enumerate(responses):
        assert fingerprint(points(response)) == [tuple(v) for v in references[i % len(cases)]], i
    # Two compatibility groups at C128 exercise actual coalescing as well as isolation.
    base_refs = json.loads((DATA / 'references.json').read_text())
    with cf.ThreadPoolExecutor(max_workers=128) as executor:
        futures = [executor.submit(call, 'POST', f'/collections/{COLL}/points/search',
                                   body(pool[i], bool(i % 2))) for i in range(128)]
        grouped = [f.result() for f in futures]
    for i, response in enumerate(grouped):
        assert fingerprint(points(response)) == [tuple(v) for v in base_refs[str(bool(i % 2))][i]], i
    responses.extend(grouped)
    after = hardware()
    usage = {k: sum(r['usage']['hardware'].get(k, 0) for r in responses) for k in before}
    delta = {k: after[k] - before[k] for k in before}
    assert usage == delta, (usage, delta)
    # An invalid request must not poison valid neighbors in the queue.
    with cf.ThreadPoolExecutor(max_workers=32) as executor:
        bad = executor.submit(call, 'POST', f'/collections/{COLL}/points/search', body([1.0]))
        good = [executor.submit(call, 'POST', *cases[i]) for i in range(32)]
        try:
            bad.result()
            raise AssertionError('invalid dimension accepted')
        except RuntimeError as error:
            assert '400' in str(error), error
        for i, f in enumerate(good):
            assert fingerprint(points(f.result())) == [tuple(v) for v in references[i]]
    result = {'mixed_responses_verified': 416, 'hardware_usage': usage,
              'collection_usage_delta': delta, 'invalid_dimension_isolated': True}
    (DATA / 'correctness.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)


def main():
    """Run one reproducible fixture, reference or measurement stage."""
    p = argparse.ArgumentParser()
    p.add_argument('stage', choices=['seed', 'reference', 'check', 'measure'])
    p.add_argument('--binary', required=True)
    p.add_argument('--max', type=int, default=1)
    p.add_argument('--count', type=int, default=1000000)
    p.add_argument('--label', default='run')
    p.add_argument('--seconds', type=float, default=10)
    p.add_argument('--debug', action='store_true')
    a = p.parse_args()
    if not 0 <= a.max <= 64 or a.seconds <= 0 or a.count < 128:
        p.error('require 0 <= max <= 64, seconds > 0 and count >= 128')
    if a.stage == 'check' and a.max < 2:
        p.error('check requires --max >= 2 to exercise coalescing')
    a.binary = str(Path(a.binary).resolve())
    server = start(a.binary, a.max, a.label, a.debug)
    try:
        validate_cpu_affinity(server[0].pid)
        if a.stage == 'seed':
            seed(a.count)
        elif a.stage == 'reference':
            reference(queries())
            varied_reference(queries())
        elif a.stage == 'check':
            check(queries())
        else:
            pool = queries()
            refs = json.loads((DATA / 'references.json').read_text())
            # Warm caches and initialize the search pools before collecting timings.
            run_case(pool, refs, 32, 2, False)
            manifest = {'binary_sha256': hashlib.sha256(Path(a.binary).read_bytes()).hexdigest(),
                        'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                        'maximum': a.max, 'server_cpus': SERVER_CPUS, 'max_search_threads': 3,
                        'client_cpus': sorted(os.sched_getaffinity(0)),
                        'collection': call('GET', f'/collections/{COLL}')['result']}
            (DATA / f'{a.label}.manifest.json').write_text(json.dumps(manifest, indent=2))
            rows = []
            for filtered in [False, True]:
                for concurrency in [1, 4, 16, 32, 64, 128]:
                    row = run_case(pool, refs, concurrency, a.seconds, filtered)
                    rows.append(row)
                    print(a.label, json.dumps(row), flush=True)
                    (DATA / f'{a.label}.json.results').write_text(json.dumps(rows, indent=2))
    finally:
        stop(server)

if __name__ == '__main__':
    main()
