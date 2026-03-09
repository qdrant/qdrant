"""
Test: nodes that miss writes while down have permanently stale data.

When a node is killed and restarted, writes that landed on other replicas
while it was down are never reconciled — there is no catch-up / anti-entropy.
The snapshot transfer amplifies this: killing the source during transfer
guarantees the source misses writes that continue flowing to other replicas.

Usage:
    pytest tests/consensus_tests/test_snapshot_transfer_consistency.py -v -s
    TEST_DURATION_MINUTES=15 pytest tests/consensus_tests/test_snapshot_transfer_consistency.py -v -s
"""
import os
import random
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from time import sleep

import requests

from .assertions import assert_http_ok
from .fixtures import create_field_index
from .utils import (
    assert_project_root,
    check_all_replicas_active,
    every_test,
    get_cluster_info,
    get_collection_cluster_info,
    get_collection_point_count,
    get_uri,
    make_peer_folder,
    processes,
    replicate_shard,
    start_cluster,
    start_peer,
    wait_collection_exists_and_active_on_all_peers,
    wait_collection_on_all_peers,
    wait_for,
    wait_for_peer_online,
)

N_PEERS = 3
N_SHARDS = 3
N_REPLICAS = 2
COLLECTION = "test_collection"
N_POINTS = 5000
DIM = 128
PORT_OFFSET = 2
DEFAULT_DURATION_MINUTES = 10
REQUEST_TIMEOUT = 30.0
PRESERVED_STORAGE_DIR = Path("/tmp/qdrant_snapshot_transfer_failure_storage")

IGNORABLE_ERRORS = (
    "does not have enough active replicas",
    "No active replica",
    "Replica set is not available",
    "Forward proxy timeout",
    "Connection refused",
    "Connection reset",
    "transport error",
    "timed out",
    "Service internal error",
)


def log(msg: str, **kw):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    extra = " ".join(f"{k}={v}" for k, v in kw.items())
    print(f"[{ts}] {msg} {extra}".rstrip())


def upsert_batch(peer_url, point_ids):
    points = [
        {
            "id": pid,
            "vector": {"": [random.random() for _ in range(DIM)]},
            "payload": {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "a": f"kw_{random.randint(0, 99)}",
            },
        }
        for pid in point_ids
    ]
    try:
        return requests.put(
            f"{peer_url}/collections/{COLLECTION}/points?wait=true",
            json={"points": points},
            timeout=REQUEST_TIMEOUT,
        )
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        return None


def check_consistency(peer_url, point_ids):
    """Scroll with consistency=all. Returns set of inconsistent point IDs."""
    consistent = set()
    offset = None
    while True:
        resp = requests.post(
            f"{peer_url}/collections/{COLLECTION}/points/scroll?consistency=all",
            json={
                "limit": len(point_ids),
                "offset": offset,
                "with_payload": ["timestamp"],
                "with_vector": False,
            },
            timeout=60,
        )
        assert_http_ok(resp)
        result = resp.json()["result"]
        consistent.update(int(p["id"]) for p in result["points"])
        offset = result.get("next_page_offset")
        if offset is None:
            break
    return point_ids - consistent


def get_point_details(peer_urls, point_ids):
    """Fetch points from each node and find differences."""
    per_node = {}
    for url in peer_urls:
        try:
            resp = requests.post(
                f"{url}/collections/{COLLECTION}/points",
                json={"ids": list(point_ids), "with_payload": True, "with_vector": True},
                timeout=REQUEST_TIMEOUT,
            )
            if resp.ok:
                per_node[url] = {p["id"]: p for p in resp.json()["result"]}
        except Exception:
            pass

    details = {}
    for pid in point_ids:
        timestamps = {}
        for url, points in per_node.items():
            p = points.get(pid)
            if p:
                timestamps[url] = (p.get("payload") or {}).get("timestamp", "?")
        if len(set(timestamps.values())) > 1:
            details[pid] = {"timestamps": timestamps}
    return details


def has_active_transfers(url):
    try:
        info = get_collection_cluster_info(url, COLLECTION)
        return len(info.get("shard_transfers", [])) > 0
    except Exception:
        return False


def preserve_storage(peer_dirs):
    dest = PRESERVED_STORAGE_DIR / datetime.now().strftime("failure_%Y%m%d_%H%M%S")
    try:
        if PRESERVED_STORAGE_DIR.exists():
            shutil.rmtree(PRESERVED_STORAGE_DIR)
        dest.mkdir(parents=True, exist_ok=True)
        for i, d in enumerate(peer_dirs):
            if d.exists():
                shutil.copytree(d, dest / f"peer_{i}")
        log(f"Storage preserved at {dest}")
        return dest
    except Exception as e:
        log(f"Failed to preserve storage: {e}")
        return None


class Cluster:
    """Lightweight cluster state tracker."""

    def __init__(self, peer_urls, peer_dirs, bootstrap_uri, tmp_path, env):
        self.peer_urls = list(peer_urls)
        self.peer_dirs = list(peer_dirs)
        self.bootstrap_uri = bootstrap_uri
        self.tmp_path = tmp_path
        self.env = env
        self._alive = [True] * len(peer_urls)
        self._restart_count = 0
        self._lock = threading.Lock()

    def alive_urls(self):
        with self._lock:
            return [u for u, a in zip(self.peer_urls, self._alive) if a]

    def _port_seed(self, idx):
        return int(self.peer_urls[idx].rsplit(":", 1)[-1]) - PORT_OFFSET

    def kill(self, idx):
        with self._lock:
            if not self._alive[idx]:
                return False
            self._alive[idx] = False
            url = self.peer_urls[idx]

        for proc in list(processes):
            if get_uri(proc.http_port) == url:
                proc.kill()
                processes.remove(proc)
                log(f"Killed peer {idx}", url=url)
                return True
        return False

    def restart(self, idx):
        with self._lock:
            if self._alive[idx]:
                return self.peer_urls[idx]
            self._restart_count += 1
            rid = self._restart_count

        try:
            url = start_peer(
                self.peer_dirs[idx],
                f"peer_{idx}_r{rid}.log",
                self.bootstrap_uri,
                port=self._port_seed(idx),
                extra_env=self.env,
            )
            wait_for_peer_online(url)
            with self._lock:
                self._alive[idx] = True
            log(f"Restarted peer {idx}", url=url)
            return url
        except Exception as e:
            log(f"Failed to restart peer {idx}: {e}")
            return None

    def kill_and_restart(self, idx, label=""):
        """Kill a peer, wait 1-3s, restart it. Returns True if killed."""
        if not self.kill(idx):
            return False
        if label:
            log(f"Killing {label} (peer {idx}) during transfer")
        sleep(random.uniform(1, 3))
        self.restart(idx)
        return True

    def restart_all(self):
        """Kill all alive peers, then restart them all. Returns list of alive URLs."""
        with self._lock:
            indices = list(range(len(self.peer_urls)))
        log("Restarting entire cluster...")
        for idx in indices:
            self.kill(idx)
        sleep(2)
        for idx in indices:
            self.restart(idx)
        alive = self.alive_urls()
        log(f"Cluster restarted, {len(alive)}/{len(indices)} peers alive")
        for url in alive:
            try:
                wait_for(check_all_replicas_active, url, COLLECTION, wait_for_timeout=60)
            except Exception:
                pass
        return alive

    def add_peer(self):
        with self._lock:
            idx = len(self.peer_urls)

        peer_dir = make_peer_folder(self.tmp_path, idx)
        url = start_peer(peer_dir, f"peer_0_{idx}.log", self.bootstrap_uri, extra_env=self.env)
        wait_for_peer_online(url)
        wait_collection_on_all_peers(COLLECTION, [url], max_wait=60)

        with self._lock:
            self.peer_urls.append(url)
            self.peer_dirs.append(peer_dir)
            self._alive.append(True)

        log(f"Added peer {idx}", url=url)
        return url, idx

    def replicate_to(self, target_idx):
        """Trigger snapshot shard replication to target peer."""
        with self._lock:
            target_url = self.peer_urls[target_idx]
        alive = self.alive_urls()
        source_url = next((u for u in alive if u != target_url), None)
        if not source_url:
            return 0

        target_id = get_cluster_info(target_url)["peer_id"]
        source_id = get_cluster_info(source_url)["peer_id"]
        source_shards = get_collection_cluster_info(source_url, COLLECTION).get("local_shards", [])

        try:
            target_shards = {s["shard_id"] for s in
                             get_collection_cluster_info(target_url, COLLECTION).get("local_shards", [])}
        except Exception:
            target_shards = set()

        count = 0
        for s in source_shards:
            sid = s["shard_id"]
            if sid not in target_shards:
                try:
                    replicate_shard(source_url, COLLECTION, sid, source_id, target_id, method="snapshot")
                    log(f"Replicating shard {sid} to peer {target_idx}", src=source_url)
                    count += 1
                except Exception as e:
                    log(f"Replication of shard {sid} failed: {e}")
        return count

    def find_transfer_source_idx(self):
        """Find the peer index of the source of an active transfer."""
        # Build peer_id → idx mapping
        id_to_idx = {}
        for i, url in enumerate(self.peer_urls):
            if self._alive[i]:
                try:
                    id_to_idx[get_cluster_info(url)["peer_id"]] = i
                except Exception:
                    pass

        for url in self.alive_urls():
            try:
                info = get_collection_cluster_info(url, COLLECTION)
                for t in info.get("shard_transfers", []):
                    from_id = t.get("from")
                    if from_id in id_to_idx:
                        return id_to_idx[from_id]
            except Exception:
                pass
        return None


class Updater:
    """Background thread that continuously upserts random batches."""

    def __init__(self, target_urls, n_points, batch_size=250):
        self.target_urls = target_urls
        self.all_ids = list(range(n_points))
        self.batch_size = batch_size
        self._stop = threading.Event()
        self._pause = threading.Event()
        self._thread = None
        self.success = 0
        self.total = 0

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=15)

    def pause(self):
        self._pause.set()

    def resume(self):
        self._pause.clear()

    def _run(self):
        while not self._stop.is_set():
            if self._pause.is_set():
                sleep(0.05)
                continue
            url = random.choice(self.target_urls)
            batch = random.sample(self.all_ids, self.batch_size)
            resp = upsert_batch(url, batch)
            self.total += 1
            if resp and resp.ok:
                self.success += 1
            sleep(0.01)


def check_with_retries(peer_url, point_ids, retries=6, delay=2.0):
    """Check consistency, retrying to filter transient issues."""
    inconsistent = check_consistency(peer_url, point_ids)
    if not inconsistent:
        return set()
    for retry in range(retries):
        sleep(delay)
        try:
            inconsistent = check_consistency(peer_url, inconsistent)
        except Exception:
            continue
        if not inconsistent:
            log(f"Resolved after {retry + 1} retries")
            return set()
        log(f"Still inconsistent (retry {retry + 1}): {len(inconsistent)} points")
    return inconsistent


def log_inconsistency_details(inconsistent, alive_urls, label=""):
    """Log details of inconsistent points. Returns details dict."""
    sample = sorted(inconsistent)[:20]
    details = get_point_details(alive_urls, sample)

    log(f"INCONSISTENCY ({label}): {len(inconsistent)} points")
    for pid, info in details.items():
        ts_items = sorted(info["timestamps"].items(), key=lambda x: x[1])
        log(f"  Point {pid}: STALE={ts_items[0][0]} ({ts_items[0][1]}) "
            f"vs NEWEST={ts_items[-1][0]} ({ts_items[-1][1]})")
    return details


def verify_inconsistency_survives_restart(cluster, inconsistent, all_ids):
    """Restart the entire cluster and re-check. Returns (still_inconsistent, details)."""
    log("Verifying inconsistency survives full cluster restart...")
    pre_restart_details = get_point_details(cluster.alive_urls(), sorted(inconsistent)[:20])

    alive = cluster.restart_all()
    assert len(alive) >= 2, f"Only {len(alive)} peers came back after restart"

    # Re-check the specific inconsistent points
    still_inconsistent = check_with_retries(alive[0], inconsistent)
    if still_inconsistent:
        post_restart_details = get_point_details(alive, sorted(still_inconsistent)[:20])
        log(f"CONFIRMED: {len(still_inconsistent)}/{len(inconsistent)} points still inconsistent after restart")
        return still_inconsistent, post_restart_details

    # Also re-check all points in case the set shifted
    all_inconsistent = check_with_retries(alive[0], all_ids)
    if all_inconsistent:
        post_restart_details = get_point_details(alive, sorted(all_inconsistent)[:20])
        log(f"CONFIRMED: {len(all_inconsistent)} points inconsistent after restart (different set)")
        return all_inconsistent, post_restart_details

    log("Inconsistency RESOLVED after cluster restart (was transient)")
    return set(), pre_restart_details


def fail_with_details(inconsistent, cluster, all_ids, label=""):
    """Log details, verify with restart, preserve storage, and fail."""
    alive = cluster.alive_urls()
    log_inconsistency_details(inconsistent, alive, label)

    # Verify the inconsistency is permanent by restarting the entire cluster
    still_inconsistent, details = verify_inconsistency_survives_restart(cluster, inconsistent, all_ids)

    preserved = preserve_storage(cluster.peer_dirs)
    if preserved:
        print(f"\n*** Storage preserved at: {preserved} ***")

    if still_inconsistent:
        assert False, (
            f"{label}: {len(still_inconsistent)} points PERMANENTLY inconsistent "
            f"(survived cluster restart). Sample: {details}"
        )
    else:
        log(f"Inconsistency from {label} was transient — resolved after cluster restart")
        return False  # signal: not a real failure


def test_snapshot_transfer_consistency(tmp_path: Path, every_test):
    """
    Repeatedly: add new peer → trigger snapshot replication → kill source or
    target during transfer → restart → verify consistency.

    write_consistency_factor=N_REPLICAS eliminates partial-write noise so any
    inconsistency is a real replication gap.
    """
    assert_project_root()

    duration = float(os.environ.get("TEST_DURATION_MINUTES", DEFAULT_DURATION_MINUTES)) * 60
    n_points = int(os.environ.get("N_POINTS", N_POINTS))

    env = {
        "QDRANT__STORAGE__SHARD_TRANSFER_METHOD": "snapshot",
        "QDRANT__CLUSTER__RESHARDING_ENABLED": "true",
    }

    log("Starting test", duration_min=f"{duration/60:.0f}", points=n_points, dim=DIM)

    # Setup cluster
    peer_urls, peer_dirs, bootstrap_uri = start_cluster(
        tmp_path, N_PEERS, port_seed=6333, extra_env=env,
    )
    cluster = Cluster(peer_urls, peer_dirs, bootstrap_uri, tmp_path, env)

    resp = requests.put(
        f"{peer_urls[0]}/collections/{COLLECTION}?timeout=30",
        json={
            "vectors": {"size": DIM, "distance": "Cosine"},
            "shard_number": N_SHARDS,
            "replication_factor": N_REPLICAS,
            "write_consistency_factor": N_REPLICAS,
            "on_disk_payload": True,
            "optimizers_config": {"indexing_threshold": 20000},
        },
    )
    assert_http_ok(resp)
    wait_collection_exists_and_active_on_all_peers(COLLECTION, peer_urls)

    create_field_index(peer_urls[0], COLLECTION, "timestamp", "datetime")
    create_field_index(peer_urls[0], COLLECTION, "a", "keyword")

    # Insert initial data
    log("Inserting initial points...")
    for off in range(0, n_points, 500):
        ids = list(range(off, min(off + 500, n_points)))
        resp = upsert_batch(peer_urls[0], ids)
        assert resp and resp.ok, f"Initial upsert failed at offset {off}"

    for url in peer_urls:
        wait_for(
            lambda u=url: get_collection_point_count(u, COLLECTION) == n_points,
            wait_for_timeout=60,
        )

    all_ids = set(range(n_points))
    assert not check_consistency(peer_urls[0], all_ids), "Initial data inconsistent"

    # Start background updates
    updater = Updater(list(peer_urls), n_points)
    updater.start()

    # Main loop: scale-up → kill during transfer → verify
    t0 = time.time()
    cycle = 0
    kills = 0

    try:
        while time.time() - t0 < duration:
            cycle += 1
            log(f"--- Cycle {cycle} ---")

            # 1. Add new peer
            try:
                new_url, new_idx = cluster.add_peer()
            except Exception as e:
                log(f"Scale-up failed: {e}")
                sleep(5)
                continue

            # 2. Trigger snapshot replication
            sleep(1)
            if not cluster.replicate_to(new_idx):
                log("No shards to replicate, skipping")
                sleep(3)
                continue

            # 3. Wait for transfer to appear
            transfer_active = False
            for _ in range(30):
                if has_active_transfers(peer_urls[0]):
                    transfer_active = True
                    break
                sleep(0.1)

            # 4. Kill source or target during transfer
            if transfer_active:
                if random.choice([True, False]):
                    src_idx = cluster.find_transfer_source_idx()
                    if src_idx is not None:
                        cluster.kill_and_restart(src_idx, "SOURCE")
                        kills += 1
                    else:
                        cluster.kill_and_restart(new_idx, "TARGET")
                        kills += 1
                else:
                    cluster.kill_and_restart(new_idx, "TARGET")
                    kills += 1
            else:
                log("Transfer too fast to catch")

            # 5. Wait for transfers to finish and replicas to activate
            try:
                wait_for(
                    lambda: not any(has_active_transfers(u) for u in cluster.alive_urls()),
                    wait_for_timeout=120, wait_for_interval=2,
                )
            except Exception:
                log("Timeout waiting for transfers")

            for url in cluster.alive_urls():
                try:
                    wait_for(check_all_replicas_active, url, COLLECTION, wait_for_timeout=30)
                except Exception:
                    pass

            # 6. Pause updates, check consistency
            updater.pause()
            sleep(1)

            alive = cluster.alive_urls()
            if len(alive) < 2:
                updater.resume()
                continue

            try:
                inconsistent = check_with_retries(alive[0], all_ids)
            except Exception as e:
                if any(err in str(e) for err in IGNORABLE_ERRORS):
                    updater.resume()
                    continue
                raise

            if inconsistent:
                updater.stop()
                # Asserts if permanent; returns False if resolved after restart
                fail_with_details(inconsistent, cluster, all_ids, f"cycle {cycle}")
                # Transient — restart updater with new alive URLs and continue
                updater = Updater(list(cluster.alive_urls()), n_points)
                updater.start()
                continue

            log(f"Consistency OK (kills={kills})")
            updater.resume()
            sleep(random.uniform(2, 5))

    finally:
        updater.stop()

    # Summary
    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"Completed in {elapsed:.0f}s ({elapsed/60:.1f} min), {cycle} cycles, {kills} kills")
    print(f"Updates: {updater.success}/{updater.total}")
    print(f"{'=' * 60}")

    # Final check
    log("Final consistency check...")
    final = cluster.alive_urls()
    inconsistent = check_with_retries(final[0], all_ids)
    if inconsistent:
        fail_with_details(inconsistent, cluster, all_ids, "final check")

    log("PASSED")
    print(f"\nAll {n_points} points consistent across {len(cluster.alive_urls())} peers")