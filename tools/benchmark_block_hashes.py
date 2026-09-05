#!/usr/bin/env python3
"""Read-only block-hash/scroll benchmark. Requires requests; see docs/block-hashes.md."""

import argparse
import copy
import json
import os
import threading
import time
from pathlib import Path

import requests

from block_hashes import block_hashes, object_path, slice_point_id_hash


def process_counters(pids):
    result = {"cpu_seconds": 0, "read_bytes": 0, "rchar": 0}
    for pid in pids:
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        result["cpu_seconds"] += (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK")
        io = dict(line.split(": ") for line in Path(f"/proc/{pid}/io").read_text().splitlines())
        for field in ("read_bytes", "rchar"):
            result[field] += int(io[field])
    return result


class Client:
    def __init__(self, args):
        self.url = f"{args.url.rstrip('/')}/collections/{args.collection}/points"
        self.key = args.payload_key
        self.filter = json.loads(args.filter.read_text()) if args.filter else None
        self.session = requests.Session()
        if os.environ.get("QDRANT_API_KEY"):
            self.session.headers["api-key"] = os.environ["QDRANT_API_KEY"]
        self.reset()

    def reset(self):
        self.bytes = 0
        self.requests = 0
        self.usage = {}

    def post(self, suffix, body):
        encoded = json.dumps(body, separators=(",", ":")).encode()
        response = self.session.post(self.url + suffix, data=encoded,
                                     headers={"Content-Type": "application/json"}, timeout=300)
        response.raise_for_status()
        data = response.json()
        self.bytes += len(encoded) + len(response.content)
        self.requests += 1
        for key, value in data.get("usage", {}).get("hardware", {}).items():
            if isinstance(value, (int, float)):
                self.usage[key] = self.usage.get(key, 0) + value
        return data["result"]

    def scoped(self, slice=None):
        if slice is None:
            return self.filter
        condition = {"slice": {"total": slice[0], "index": slice[1]}}
        # Nest the original filter to preserve should/min_should/must_not semantics.
        return {"must": [condition] + ([self.filter] if self.filter else [])}

    def hashes(self, count, slice=None):
        return self.post("/block-hashes", {"payload_key": self.key, "block_count": count, "filter": self.scoped(slice)})

    def scroll(self, slice=None):
        offset = None
        records = []
        while True:
            page = self.post("/scroll", {
                "offset": offset, "limit": 1024, "filter": self.scoped(slice),
                "with_payload": [self.key], "with_vector": False,
            })
            records.extend(page["points"])
            offset = page.get("next_page_offset")
            if offset is None:
                return records


def percentile(values, fraction):
    return sorted(values)[min(len(values) - 1, int(len(values) * fraction))] if values else None


def measure(args, operation):
    client = Client(args)
    stopped = threading.Event()
    latencies = []
    query_errors = []
    query = json.loads(args.query.read_text()) if args.query else None

    def query_load():
        query_client = Client(args)
        while not stopped.is_set():
            start = time.perf_counter()
            try:
                query_client.post("/query", query)
                latencies.append(1000 * (time.perf_counter() - start))
            except Exception as error:
                query_errors.append(str(error))
            stopped.wait(args.query_interval)

    before = process_counters(args.pid)
    start = time.perf_counter()
    worker = threading.Thread(target=query_load) if query else None
    if worker:
        worker.start()
    times = []
    try:
        while time.perf_counter() - start < args.seconds:
            if operation is None:
                stopped.wait(0.01)
                continue
            audit_start = time.perf_counter()
            operation(client)
            times.append(time.perf_counter() - audit_start)
    finally:
        stopped.set()
        if worker:
            worker.join()
    elapsed = time.perf_counter() - start
    after = process_counters(args.pid)
    repetitions = len(times) or 1
    return {
        "elapsed_seconds": elapsed, "audits": len(times),
        "audit_seconds_mean": sum(times) / repetitions,
        "json_bytes_per_audit": client.bytes / repetitions,
        "http_requests_per_audit": client.requests / repetitions,
        "audit_usage_per_audit": {k: v / repetitions for k, v in client.usage.items()},
        "server_process_delta": {k: after[k] - before[k] for k in before} if args.pid else None,
        "query_samples": len(latencies), "query_p50_ms": percentile(latencies, .5),
        "query_p95_ms": percentile(latencies, .95), "query_errors": query_errors,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:6333")
    parser.add_argument("--collection", required=True)
    parser.add_argument("--payload-key", default="sync.fingerprint")
    parser.add_argument("--block-count", type=int, default=16)
    parser.add_argument("--refine-factor", type=int, default=4)
    parser.add_argument("--mismatches", type=int, default=1)
    parser.add_argument("--filter", type=Path)
    parser.add_argument("--query", type=Path)
    parser.add_argument("--query-interval", type=float, default=.02)
    parser.add_argument("--pid", type=int, nargs="*", default=[])
    parser.add_argument("--seconds", type=float, default=3)
    args = parser.parse_args()
    if not 1 <= args.block_count <= args.block_count * args.refine_factor <= 65536:
        parser.error("block-count and refined block-count must be in 1..65536")
    if args.seconds <= 0 or args.mismatches < 0 or args.query_interval < 0:
        parser.error("seconds must be positive; mismatches/query-interval must be nonnegative")

    # Read the quiescent destination as a synthetic source, then change only our
    # local copy. Preparation traffic and local hashing are outside timed trials.
    source = copy.deepcopy(Client(args).scroll())
    keys = object_path(args.payload_key)
    for record in source[:args.mismatches]:
        parent = record["payload"]
        for key in keys[:-1]:
            parent = parent[key]
        parent[keys[-1]] += ":source-changed"
    local = block_hashes(source, args.payload_key, args.block_count)
    fine_count = args.block_count * args.refine_factor
    fine = {}
    for record in source[:args.mismatches]:
        parent = slice_point_id_hash(record["id"]) % args.block_count
        if parent not in fine:
            records = [p for p in source if slice_point_id_hash(p["id"]) % args.block_count == parent]
            fine[parent] = block_hashes(records, args.payload_key, fine_count)

    def reconcile(client):
        remote = client.hashes(args.block_count)
        for theirs, ours in zip(remote["blocks"], local["blocks"]):
            if theirs == ours:
                continue
            parent = ours["block_id"]
            if parent not in fine:
                raise RuntimeError("Destination changed during benchmark; rerun on a quiescent dataset")
            refined = client.hashes(fine_count, (args.block_count, parent))
            for theirs, ours in zip(refined["blocks"], fine[parent]["blocks"]):
                if theirs != ours:
                    client.scroll((fine_count, ours["block_id"]))

    result = {"points": len(source), "simulated_changes": min(args.mismatches, len(source)),
              "block_count": args.block_count, "refined_block_count": fine_count}
    for name, operation in [
        ("query_only", None), ("scroll", lambda c: c.scroll()),
        ("block_hashes", lambda c: c.hashes(args.block_count)), ("reconcile", reconcile),
    ]:
        result[name] = measure(args, operation)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
