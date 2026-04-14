"""
E2E test for memory usage reporting accuracy.

Loads a snapshot with various payload indexes and vector fields,
then removes components one by one, comparing the reported RAM usage
(from /collections/{name}/memory) against the actual change in
jemalloc allocated_bytes (from /telemetry).

Assumes Qdrant is already running on localhost:6333.
"""

import os
import time
from pathlib import Path

import requests

SNAPSHOT_URL = "https://storage.googleapis.com/qdrant-benchmark-snapshots/all-components/all-payloads-5761161004893096-2026-04-12-13-53-30.snapshot"
COLLECTION_NAME = "memory_test"
BASE_URL = "http://localhost:6333"
CACHE_DIR = Path(__file__).parent / ".snapshot_cache"


def get_cached_snapshot() -> Path:
    """Download the snapshot once and cache it locally; return the local path."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cached_path = CACHE_DIR / Path(SNAPSHOT_URL).name
    if cached_path.exists() and cached_path.stat().st_size > 0:
        print(f"Using cached snapshot: {cached_path} ({cached_path.stat().st_size} bytes)")
        return cached_path

    print(f"Downloading snapshot from {SNAPSHOT_URL}...")
    tmp_path = cached_path.with_suffix(cached_path.suffix + ".tmp")
    with requests.get(SNAPSHOT_URL, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    os.replace(tmp_path, cached_path)
    print(f"Cached snapshot to {cached_path} ({cached_path.stat().st_size} bytes)")
    return cached_path


def get_allocated_bytes() -> int:
    """Get jemalloc allocated_bytes from telemetry."""
    resp = requests.get(f"{BASE_URL}/telemetry", params={"details_level": 2})
    resp.raise_for_status()
    return resp.json()["result"]["memory"]["allocated_bytes"]


def get_memory_report(collection_name: str) -> dict:
    """Get the collection memory report."""
    resp = requests.get(f"{BASE_URL}/collections/{collection_name}/memory")
    resp.raise_for_status()
    return resp.json()["result"]


def get_collection_info(collection_name: str) -> dict:
    """Get collection info."""
    resp = requests.get(f"{BASE_URL}/collections/{collection_name}")
    resp.raise_for_status()
    return resp.json()["result"]


def wait_for_green(collection_name: str, timeout: int = 120):
    """Wait for collection optimizer to finish (status green)."""
    for _ in range(timeout):
        info = get_collection_info(collection_name)
        status = info.get("status")
        if status == "green":
            return
        time.sleep(1)
    raise TimeoutError(f"Collection {collection_name} did not reach green status in {timeout}s")


def delete_payload_index(collection_name: str, field_name: str):
    """Delete a payload index."""
    resp = requests.delete(
        f"{BASE_URL}/collections/{collection_name}/index/{field_name}",
        params={"wait": "true"},
    )
    resp.raise_for_status()


def delete_vector_field(collection_name: str, vector_name: str):
    """Delete a named vector field entirely."""
    resp = requests.delete(
        f"{BASE_URL}/collections/{collection_name}/vectors/{vector_name}",
        params={"wait": "true"},
    )
    resp.raise_for_status()


def fmt_bytes(b: int) -> str:
    """Format bytes as human-readable string."""
    if abs(b) >= 1024 * 1024:
        return f"{b / (1024 * 1024):.2f} MB"
    if abs(b) >= 1024:
        return f"{b / 1024:.2f} KB"
    return f"{b} B"


def test_memory_reporting_accuracy():
    """
    Load a snapshot, measure memory, remove components one by one,
    and compare reported vs actual memory freed.
    """
    # Verify server is up
    resp = requests.get(f"{BASE_URL}/collections")
    resp.raise_for_status()
    print("Qdrant is running.")

    # Clean up any leftover collection from a previous run
    requests.delete(f"{BASE_URL}/collections/{COLLECTION_NAME}", params={"wait": "true"})

    # Get snapshot from local cache (download once if needed)
    snapshot_path = get_cached_snapshot()

    # Upload snapshot to Qdrant
    print(f"\nUploading snapshot to Qdrant...")
    with open(snapshot_path, "rb") as f:
        resp = requests.post(
            f"{BASE_URL}/collections/{COLLECTION_NAME}/snapshots/upload",
            files={"snapshot": (snapshot_path.name, f, "application/octet-stream")},
            params={"wait": "true"},
            timeout=600,
        )
    resp.raise_for_status()
    print("Snapshot recovered.")

    wait_for_green(COLLECTION_NAME)

    # Discover what's in the collection
    info = get_collection_info(COLLECTION_NAME)
    payload_schema = info.get("payload_schema", {})
    vectors_config = info.get("config", {}).get("params", {}).get("vectors", {})
    sparse_vectors_config = info.get("config", {}).get("params", {}).get("sparse_vectors", {})

    indexed_fields = [
        name for name, schema in payload_schema.items()
        if schema.get("points", 0) > 0
    ]

    dense_vector_names = list(vectors_config.keys()) if isinstance(vectors_config, dict) else []
    sparse_vector_names = list(sparse_vectors_config.keys()) if isinstance(sparse_vectors_config, dict) else []

    print("\nCollection has:")
    print(f"  Indexed payload fields: {indexed_fields}")
    print(f"  Dense vector fields: {dense_vector_names}")
    print(f"  Sparse vector fields: {sparse_vector_names}")

    # Get initial memory state
    report = get_memory_report(COLLECTION_NAME)
    initial_allocated = get_allocated_bytes()

    print(f"\nInitial jemalloc allocated: {fmt_bytes(initial_allocated)}")
    print(f"Reported total RAM: {fmt_bytes(report['total']['ram_bytes'])}")
    print(f"Reported total cached: {fmt_bytes(report['total']['cached_bytes'])}")

    results = []

    # Build a lookup of reported memory per component
    payload_index_memory = {
        pi["name"]: pi["usage"]["ram_bytes"]
        for pi in report.get("payload_index", [])
    }
    vector_memory = {}
    for v in report.get("vectors", []):
        total_ram = v["storage"]["ram_bytes"] + v["index"]["ram_bytes"]
        if v.get("quantized"):
            total_ram += v["quantized"]["ram_bytes"]
        vector_memory[v["name"]] = total_ram

    sparse_vector_memory = {}
    for sv in report.get("sparse_vectors", []):
        total_ram = sv["storage"]["ram_bytes"] + sv["index"]["ram_bytes"]
        sparse_vector_memory[sv["name"]] = total_ram

    # Remove payload indexes one by one
    for field_name in indexed_fields:
        estimated = payload_index_memory.get(field_name, 0)
        before = get_allocated_bytes()

        print(f"\nDeleting payload index '{field_name}' (estimated RAM: {fmt_bytes(estimated)})...")
        delete_payload_index(COLLECTION_NAME, field_name)
        wait_for_green(COLLECTION_NAME)
        time.sleep(1)

        after = get_allocated_bytes()
        actual_freed = before - after

        error_abs = abs(actual_freed - estimated)
        error_pct = (error_abs / estimated * 100) if estimated > 0 else float("inf")

        results.append({
            "component": f"payload_index/{field_name}",
            "estimated": estimated,
            "actual_freed": actual_freed,
            "error_abs": error_abs,
            "error_pct": error_pct,
        })

        print(f"  Actual freed: {fmt_bytes(actual_freed)}, Error: {fmt_bytes(error_abs)} ({error_pct:.1f}%)")

    # Remove dense vector fields one by one
    for vec_name in dense_vector_names:
        estimated = vector_memory.get(vec_name, 0)
        before = get_allocated_bytes()

        print(f"\nDeleting dense vector '{vec_name}' (estimated RAM: {fmt_bytes(estimated)})...")
        delete_vector_field(COLLECTION_NAME, vec_name)
        wait_for_green(COLLECTION_NAME)
        time.sleep(1)

        after = get_allocated_bytes()
        actual_freed = before - after

        error_abs = abs(actual_freed - estimated)
        error_pct = (error_abs / estimated * 100) if estimated > 0 else float("inf")

        results.append({
            "component": f"vector/{vec_name}",
            "estimated": estimated,
            "actual_freed": actual_freed,
            "error_abs": error_abs,
            "error_pct": error_pct,
        })

        print(f"  Actual freed: {fmt_bytes(actual_freed)}, Error: {fmt_bytes(error_abs)} ({error_pct:.1f}%)")

    # Remove sparse vector fields one by one
    for sv_name in sparse_vector_names:
        estimated = sparse_vector_memory.get(sv_name, 0)
        before = get_allocated_bytes()

        print(f"\nDeleting sparse vector '{sv_name}' (estimated RAM: {fmt_bytes(estimated)})...")
        delete_vector_field(COLLECTION_NAME, sv_name)
        wait_for_green(COLLECTION_NAME)
        time.sleep(1)

        after = get_allocated_bytes()
        actual_freed = before - after

        error_abs = abs(actual_freed - estimated)
        error_pct = (error_abs / estimated * 100) if estimated > 0 else float("inf")

        results.append({
            "component": f"sparse_vector/{sv_name}",
            "estimated": estimated,
            "actual_freed": actual_freed,
            "error_abs": error_abs,
            "error_pct": error_pct,
        })

        print(f"  Actual freed: {fmt_bytes(actual_freed)}, Error: {fmt_bytes(error_abs)} ({error_pct:.1f}%)")

    # Print summary table
    print("\n" + "=" * 100)
    print(f"{'Component':<40} {'Estimated':>12} {'Actual Freed':>14} {'Error':>12} {'Error %':>10}")
    print("-" * 100)
    for r in results:
        print(
            f"{r['component']:<40} "
            f"{fmt_bytes(r['estimated']):>12} "
            f"{fmt_bytes(r['actual_freed']):>14} "
            f"{fmt_bytes(r['error_abs']):>12} "
            f"{r['error_pct']:>9.1f}%"
        )
    print("=" * 100)

    # Final check: remaining allocated should be close to initial minus sum of reported
    final_allocated = get_allocated_bytes()
    total_estimated_freed = sum(r["estimated"] for r in results)
    total_actual_freed = initial_allocated - final_allocated

    print(f"\nTotal estimated freed: {fmt_bytes(total_estimated_freed)}")
    print(f"Total actual freed:   {fmt_bytes(total_actual_freed)}")

    total_error = abs(total_actual_freed - total_estimated_freed)
    total_error_pct = (total_error / total_estimated_freed * 100) if total_estimated_freed > 0 else float("inf")
    print(f"Total error:          {fmt_bytes(total_error)} ({total_error_pct:.1f}%)")

    # Assert overall error is within 30%
    assert total_error_pct < 30, (
        f"Total memory reporting error {total_error_pct:.1f}% exceeds 30% threshold. "
        f"Estimated {fmt_bytes(total_estimated_freed)} freed, "
        f"actual {fmt_bytes(total_actual_freed)} freed."
    )


if __name__ == "__main__":
    test_memory_reporting_accuracy()
