#!/usr/bin/env python3
"""
Reproduce the consensus stall seen in the chaos incidents:

    WARN Slow wait: removing a peer in handle_replica_changes took 66.71s

What happens: a node applies `drop_replica` for its *own* replica while the optimizer of that
shard is in the HNSW graph-heal phase (rebuilding an index on top of an old one). Applying the
entry stops the shard, which joins the optimizer task; the heal phase has no cancellation check,
so the consensus thread is blocked until the heal finishes.

Run it with no arguments: it starts a cluster, runs a few trials, prints a table, tears down.
Steps for doing it by hand:

    up     start 3 CPU-throttled peers, create a 1-shard / RF-2 collection, load it, start churn
    run    wait for a heal build, drop that peer's replica, print how long the apply took; repeat
    down   stop peers and churn

See README.md.
"""

import argparse
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
QDRANT_BIN = Path(os.environ.get("QDRANT_BIN", HERE / "../../target/perf/qdrant")).resolve()
CPU_QUOTA = os.environ.get("CPU_QUOTA", "50%")  # per peer; the chaos pods run on 0.5 CPU
BFB_IMAGE = os.environ.get("BFB_IMAGE", "qdrant/bfb:dev")
BASE_PORT = int(os.environ.get("BASE_PORT", "7300"))

PEERS = 3
COLLECTION = "benchmark"
POINTS = 100_000
DIM = 128
DATA, LOGS, TRIALS = HERE / "data", HERE / "logs", HERE / "trials"
RESULTS = HERE / "results.jsonl"
CHURN = "stall-repro-churn"


def http(i):
    return BASE_PORT + i * 10 + 3


def grpc(i):
    return BASE_PORT + i * 10 + 4


def p2p(i):
    return BASE_PORT + i * 10 + 5


def api(i):
    return f"http://127.0.0.1:{http(i)}"


def unit(i):
    return f"stall-repro-peer{i}"


# --------------------------------------------------------------------------------------- cluster


def start_peer(i):
    env = {
        **os.environ,
        "QDRANT__CLUSTER__ENABLED": "true",
        "QDRANT__SERVICE__HOST": "127.0.0.1",
        "QDRANT__SERVICE__HTTP_PORT": str(http(i)),
        "QDRANT__SERVICE__GRPC_PORT": str(grpc(i)),
        "QDRANT__CLUSTER__P2P__PORT": str(p2p(i)),
        "QDRANT__STORAGE__STORAGE_PATH": str(DATA / f"peer_{i}"),
        "QDRANT__STORAGE__SNAPSHOTS_PATH": str(DATA / f"peer_{i}" / "snapshots"),
        "QDRANT__LOG_LEVEL": "DEBUG",  # the trigger and the timeline below are DEBUG lines
        "QDRANT__LOGGER__ON_DISK__ENABLED": "false",
        "QDRANT__TELEMETRY_DISABLED": "true",
    }
    args = ["--uri", f"http://127.0.0.1:{p2p(i)}"]
    if i > 0:
        args += ["--bootstrap", f"http://127.0.0.1:{p2p(0)}"]
    log = open(LOGS / f"peer_{i}.log", "ab")
    # own systemd scope so we get CFS throttling like the k8s pods
    subprocess.Popen(
        ["systemd-run", "--user", "--scope", "-q", f"--unit={unit(i)}", "-p", f"CPUQuota={CPU_QUOTA}",
         str(QDRANT_BIN), *args],
        env=env, stdout=log, stderr=subprocess.STDOUT, cwd=HERE,
    )


def cluster(i):
    return requests.get(f"{api(i)}/cluster", timeout=5).json()["result"]


def wait_up(i):
    for _ in range(240):
        try:
            return cluster(i)
        except requests.RequestException:
            time.sleep(0.5)
    sys.exit(f"peer {i} did not come up, see {LOGS}/peer_{i}.log")


def replica_states():
    """peer index -> state of the (single) shard's replica on that peer, for peers holding one"""
    out = {}
    for i in range(PEERS):
        info = requests.get(f"{api(i)}/collections/{COLLECTION}/cluster", timeout=5).json()["result"]
        for s in info["local_shards"]:
            out[i] = s["state"]
    return out


def bfb(docker_args, bfb_args):
    return ["docker", "run", "--network", "host", *docker_args, BFB_IMAGE, "./bfb",
            "--uri", f"http://127.0.0.1:{grpc(0)}", "--collection-name", COLLECTION, "--skip-create",
            "--max-id", str(POINTS), "-d", str(DIM), "-b", "100", "--timing-threshold", "5", *bfb_args]


def cmd_up(_):
    if not QDRANT_BIN.exists():
        sys.exit(f"missing {QDRANT_BIN} (build: cargo build --profile perf --bin qdrant, or set QDRANT_BIN)")
    cmd_down(None)
    for d in (DATA, LOGS):
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir()
    for i in range(PEERS):
        start_peer(i)
        wait_up(i)
        print(f"peer {i} up: {api(i)}")
        if i == 0:
            time.sleep(2)  # let peer 0 open its p2p port before the others bootstrap from it
    while any(len(cluster(i)["peers"]) < PEERS for i in range(PEERS)):
        time.sleep(1)

    # One shard on two peers: no guessing which shard a log line belongs to.
    # 1 indexing thread like the 0.5-CPU pods; small segments so heal builds are frequent.
    requests.put(f"{api(0)}/collections/{COLLECTION}", timeout=60, json={
        "vectors": {"size": DIM, "distance": "Cosine"},
        "shard_number": 1,
        "replication_factor": 2,
        "hnsw_config": {"m": 16, "ef_construct": 100, "max_indexing_threads": 1},
        "optimizers_config": {"default_segment_number": 2, "indexing_threshold": 10_000},
    }).raise_for_status()

    print(f"loading {POINTS} points (waits for the index to be built) ...")
    subprocess.run(bfb(["--rm"], ["-n", str(POINTS), "-t", "4", "-p", "4"]), check=True)

    # endless random upserts: overwrites delete old versions -> vacuum rebuilds HNSW on the old graph
    subprocess.run(bfb(["-d", "--name", CHURN],
                       ["--skip-wait-index", "-n", str(POINTS * 1000), "-t", "2", "-p", "2", "-T", "5",
                        "--retry", "10", "--retry-interval", "1", "--ignore-errors"]),
                   check=True, capture_output=True)
    print("churn started; now: stall_repro.py run")


def cmd_down(_):
    subprocess.run(["docker", "rm", "-f", CHURN], capture_output=True)
    for i in range(PEERS):
        subprocess.run(["systemctl", "--user", "stop", f"{unit(i)}.scope"], capture_output=True)


# --------------------------------------------------------------------------------------- trials

LOG_TS = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+)Z\s*")
HEAL = re.compile(r"Reusing (\d+) points from the old index, healing (\d+) points")
APPLY_START = re.compile(r"Applying committed entry with index (\d+)")
SLOW_WAIT = re.compile(r"Slow wait: removing a peer in handle_replica_changes took ([\d.]+)(ms|s|m)")
# lines worth showing between "drop sent" and "entry applied", in the order they appear on a stall
TIMELINE = ("Applying committed entry", "Stopping flush worker", "Migrated in",
            "Optimization cancelled", "Slow wait", "Successfully applied")


def tail(i, q):
    """push (peer, log timestamp, line) for every new line of peer i's log"""
    with open(LOGS / f"peer_{i}.log", errors="replace") as f:
        f.seek(0, os.SEEK_END)
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.05)
                continue
            m = LOG_TS.match(line)
            ts = (datetime.strptime(m.group(1)[:26], "%Y-%m-%dT%H:%M:%S.%f")
                  .replace(tzinfo=timezone.utc).timestamp()) if m else time.time()
            q.put((i, ts, line.rstrip()))


def secs(m):
    return float(m.group(1)) * {"ms": 0.001, "s": 1, "m": 60}[m.group(2)]


def run_trial(k, q, peer_id, min_heal):
    # 1. wait for a heal build on a peer whose replica we may drop (both replicas Active)
    while not q.empty():
        q.get()
    print(f"\n--- trial {k}: waiting for a heal build (>= {min_heal} healed points)")
    while True:
        i, _, line = q.get()
        m = HEAL.search(line)
        if not m or int(m.group(2)) < min_heal:
            continue
        states = replica_states()
        if states.get(i) == "Active" and [j for j in states if j != i and states[j] == "Active"]:
            break
        print(f"peer {i}: heal build, but replicas are {states}; skipping")
    reused, healed = int(m.group(1)), int(m.group(2))
    print(f"peer {i}: reusing {reused} points, healing {healed}")

    # 2. drop that peer's own replica; qdrant waits up to 3 s for the entry to apply, then 500s
    t_drop = time.time()
    r = requests.post(f"{api(i)}/collections/{COLLECTION}/cluster", params={"timeout": 3}, timeout=10,
                      json={"drop_replica": {"shard_id": 0, "peer_id": peer_id[i]}})
    print(f"drop_replica sent to peer {i}: HTTP {r.status_code} after {time.time() - t_drop:.2f}s")

    # 3. follow that peer's log until the entry is applied
    lines, timeline, entry, stall, applied = [], [], None, None, None
    while applied is None and time.time() - t_drop < 900:
        try:
            j, ts, line = q.get(timeout=1)
        except queue.Empty:
            continue
        if j != i:
            continue
        rel = ts - t_drop
        lines.append(f"{rel:+8.2f}s  {line}")
        if not any(s in line for s in TIMELINE):
            continue
        msg = LOG_TS.sub("", line)
        timeline.append((round(rel, 2), msg))
        print(f"  {rel:+8.2f}s  {msg[:150]}")
        if m := APPLY_START.search(line):
            entry = entry or m.group(1)
        if m := SLOW_WAIT.search(line):
            stall = secs(m)
        if entry and f"Successfully applied consensus operation entry. Index: {entry}" in line:
            applied = rel
    print(f"=> applied after {applied and round(applied, 2)}s, slow wait: {stall}s")

    TRIALS.mkdir(exist_ok=True)
    (TRIALS / f"trial_{k:02d}_peer{i}.log").write_text("\n".join(lines) + "\n")
    with open(RESULTS, "a") as f:
        f.write(json.dumps({"trial": k, "peer": i, "reused": reused, "healed": healed,
                            "http_status": r.status_code, "applied_after": applied,
                            "slow_wait": stall, "timeline": timeline}) + "\n")

    # 4. put the replica back so the peer can be hit again
    src = next(j for j in replica_states() if j != i)
    requests.post(f"{api(src)}/collections/{COLLECTION}/cluster", params={"timeout": 60}, timeout=90,
                  json={"replicate_shard": {"shard_id": 0, "from_peer_id": peer_id[src],
                                            "to_peer_id": peer_id[i], "method": "stream_records"}})
    while replica_states().get(i) != "Active":
        time.sleep(1)
    print(f"replica restored on peer {i}")
    return stall, applied, healed


def cmd_run(args):
    q = queue.Queue()
    for i in range(PEERS):
        threading.Thread(target=tail, args=(i, q), daemon=True).start()
    peer_id = {i: cluster(i)["peer_id"] for i in range(PEERS)}
    rows = []
    for k in range(args.trials):
        try:
            rows.append(run_trial(k, q, peer_id, args.min_heal))
        except KeyboardInterrupt:
            break
        except Exception as e:  # noqa: BLE001
            print(f"trial {k} failed: {e!r}")
            time.sleep(5)
    print(f"\n{'trial':>5} {'healed':>7} {'slow wait':>10} {'applied':>8}")
    for k, (stall, applied, healed) in enumerate(rows):
        print(f"{k:>5} {healed:>7} {stall if stall is not None else '-':>10} {applied and round(applied, 2):>8}")
    print(f"details: {RESULTS}, {TRIALS}/")


def cmd_all(args):
    RESULTS.unlink(missing_ok=True)
    shutil.rmtree(TRIALS, ignore_errors=True)
    try:
        cmd_up(args)
        cmd_run(args)
    finally:
        cmd_down(args)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--trials", type=int, default=5)
    ap.add_argument("--min-heal", type=int, default=1000, help="ignore heal builds smaller than this")
    ap.set_defaults(fn=cmd_all)
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("up").set_defaults(fn=cmd_up)
    sub.add_parser("run").set_defaults(fn=cmd_run)
    sub.add_parser("down").set_defaults(fn=cmd_down)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
