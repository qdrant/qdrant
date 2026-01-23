"""
Benchmark search latency during concurrent updates and payload indexing.

Measures how search latency degrades when the system is under load from:
1. Continuous point updates (upserts)
2. Payload index creation

Run with: pytest tests/e2e_tests/test_search_during_indexing.py -v -s
"""

import random
import statistics
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import List

import pytest
from qdrant_client import models

from .client_utils import ClientUtils

VECTOR_SIZE = 768
COLLECTION_NAME = "search_indexing_benchmark"
NUM_POINTS = 500_000
SEARCH_TIMEOUT_SEC = 30

WORDS = [
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "theta", "lambda",
    "quick", "brown", "fox", "jumps", "lazy", "dog", "lorem", "ipsum", "dolor",
    "vector", "search", "index", "query", "filter", "payload", "segment", "shard",
    "database", "storage", "memory", "performance", "latency", "throughput",
    "algorithm", "structure", "optimization", "concurrent", "parallel", "async",
    "network", "cluster", "replica", "consensus", "raft", "snapshot", "recovery",
]

TEXT_INDEX_PARAMS = models.TextIndexParams(
    type="text",
    tokenizer=models.TokenizerType.WORD,
    min_token_len=2,
    max_token_len=15,
)

PAYLOAD_INDEXES = [
    ("keyword_field", models.PayloadSchemaType.KEYWORD),
    ("category", models.PayloadSchemaType.KEYWORD),
    ("integer_field", models.PayloadSchemaType.INTEGER),
    ("float_field", models.PayloadSchemaType.FLOAT),
    ("text_field_1", TEXT_INDEX_PARAMS),
    ("text_field_2", TEXT_INDEX_PARAMS),
    ("text_field_3", TEXT_INDEX_PARAMS),
]


@dataclass
class SearchStats:
    """Collected search latency statistics."""
    latencies_ms: List[float] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    timeouts: int = 0

    @property
    def count(self) -> int:
        return len(self.latencies_ms)

    @property
    def p50_ms(self) -> float:
        return self._percentile(50)

    @property
    def p90_ms(self) -> float:
        return self._percentile(90)

    @property
    def p99_ms(self) -> float:
        return self._percentile(99)

    @property
    def max_ms(self) -> float:
        return max(self.latencies_ms) if self.latencies_ms else 0

    def _percentile(self, p: float) -> float:
        if not self.latencies_ms:
            return 0
        sorted_vals = sorted(self.latencies_ms)
        idx = min(int(len(sorted_vals) * p / 100), len(sorted_vals) - 1)
        return sorted_vals[idx]

    def summary(self) -> str:
        if not self.latencies_ms:
            return "n=0"
        mean = statistics.mean(self.latencies_ms)
        std = statistics.stdev(self.latencies_ms) if len(self.latencies_ms) > 1 else 0
        return (
            f"n={self.count}, p50={self.p50_ms:.1f}ms, p90={self.p90_ms:.1f}ms, "
            f"p99={self.p99_ms:.1f}ms, max={self.max_ms:.1f}ms, mean={mean:.1f}ms (std={std:.1f})"
        )

    def degradation_vs(self, baseline: "SearchStats") -> dict:
        """Calculate degradation ratios compared to baseline."""
        def ratio(a: float, b: float) -> float:
            return a / b if b > 0 else 0
        return {
            "p50": ratio(self.p50_ms, baseline.p50_ms),
            "p90": ratio(self.p90_ms, baseline.p90_ms),
            "p99": ratio(self.p99_ms, baseline.p99_ms),
            "max": ratio(self.max_ms, baseline.max_ms),
        }


@dataclass
class OperationStats:
    """Statistics for background operations."""
    completed: int = 0
    errors: List[str] = field(default_factory=list)
    durations_ms: List[float] = field(default_factory=list)


def generate_payload() -> dict:
    """Generate payload with unique tokens for heavy text indexing."""
    def heavy_text() -> str:
        tokens = []
        for _ in range(50):
            tokens.append(random.choice(WORDS))
            tokens.append(uuid.uuid4().hex[:8])
        return " ".join(tokens)

    return {
        "keyword_field": random.choice(["urgent", "normal", "low", "high", "critical"]),
        "category": random.choice(["A", "B", "C", "D", "E"]),
        "integer_field": random.randint(1, 100000),
        "float_field": round(random.uniform(0.0, 1000.0), 4),
        "text_field_1": heavy_text(),
        "text_field_2": heavy_text(),
        "text_field_3": heavy_text(),
    }


class TestSearchDuringIndexing:
    """Benchmark search latency during concurrent indexing and updates."""

    @staticmethod
    def _insert_data(client: ClientUtils, num_points: int, batch_size: int = 1000):
        """Insert test data into collection."""
        for offset in range(0, num_points, batch_size):
            points = [
                models.PointStruct(
                    id=offset + i,
                    vector=[random.random() for _ in range(VECTOR_SIZE)],
                    payload=generate_payload(),
                )
                for i in range(min(batch_size, num_points - offset))
            ]
            client.client.upsert(collection_name=COLLECTION_NAME, points=points, wait=True)
            if (offset + batch_size) % 50000 == 0 or offset + batch_size >= num_points:
                print(f"  Inserted {min(offset + batch_size, num_points):,}/{num_points:,} points")

    @staticmethod
    def _wait_for_green(client: ClientUtils, timeout: int = 300) -> bool:
        """Wait for collection to reach green status."""
        print("  Waiting for optimization...", end="", flush=True)
        start = time.time()
        while time.time() - start < timeout:
            try:
                info = client.client.get_collection(COLLECTION_NAME)
                if info.status.value == "green":
                    print(f" done ({time.time() - start:.1f}s)")
                    return True
            except Exception:
                pass
            time.sleep(2)
            print(".", end="", flush=True)
        print(f" timeout after {timeout}s")
        return False

    @staticmethod
    def _run_search_baseline(client: ClientUtils, duration: float) -> SearchStats:
        """Run search-only benchmark for baseline measurement."""
        stats = SearchStats()
        end_time = time.time() + duration

        while time.time() < end_time:
            try:
                query = [random.random() for _ in range(VECTOR_SIZE)]
                start = time.perf_counter()
                client.client.query_points(
                    collection_name=COLLECTION_NAME,
                    query=query,
                    limit=10,
                    timeout=SEARCH_TIMEOUT_SEC,
                )
                stats.latencies_ms.append((time.perf_counter() - start) * 1000)
            except Exception as e:
                if "timeout" in str(e).lower():
                    stats.timeouts += 1
                else:
                    stats.errors.append(str(e))
            time.sleep(0.05)

        return stats

    @staticmethod
    def _search_loop(client: ClientUtils, stop: threading.Event, stats: SearchStats):
        """Continuously search and collect latencies."""
        while not stop.is_set():
            try:
                query = [random.random() for _ in range(VECTOR_SIZE)]
                start = time.perf_counter()
                client.client.query_points(
                    collection_name=COLLECTION_NAME,
                    query=query,
                    limit=10,
                    timeout=SEARCH_TIMEOUT_SEC,
                )
                stats.latencies_ms.append((time.perf_counter() - start) * 1000)
            except Exception as e:
                if "timeout" in str(e).lower():
                    stats.timeouts += 1
                    print("  TIMEOUT: Search timed out")
                else:
                    stats.errors.append(str(e))
            time.sleep(0.05)

    @staticmethod
    def _update_loop(client: ClientUtils, stop: threading.Event, stats: OperationStats):
        """Continuously upsert points."""
        point_id = 1_000_000
        batch_size = 100

        while not stop.is_set():
            try:
                points = [
                    models.PointStruct(
                        id=point_id + i,
                        vector=[random.random() for _ in range(VECTOR_SIZE)],
                        payload=generate_payload(),
                    )
                    for i in range(batch_size)
                ]
                client.client.upsert(collection_name=COLLECTION_NAME, points=points, wait=True)
                stats.completed += 1
                point_id += batch_size
            except Exception as e:
                stats.errors.append(str(e))
                print(f"  ERROR: Upsert failed: {str(e)[:100]}")
            time.sleep(0.2)

    @staticmethod
    def _indexing_loop(client: ClientUtils, stop: threading.Event, stats: OperationStats):
        """Create payload indexes one by one."""
        for field_name, field_schema in PAYLOAD_INDEXES:
            if stop.is_set():
                break
            try:
                print(f"  Creating index '{field_name}'...", end="", flush=True)
                start = time.perf_counter()
                client.client.create_payload_index(
                    collection_name=COLLECTION_NAME,
                    field_name=field_name,
                    field_schema=field_schema,
                    wait=True,
                )
                duration_ms = (time.perf_counter() - start) * 1000
                stats.completed += 1
                stats.durations_ms.append(duration_ms)
                print(f" done ({duration_ms:.0f}ms)")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    stats.errors.append(str(e))
                    print(f" ERROR: {str(e)[:100]}")
            time.sleep(1.0)

        while not stop.is_set():
            time.sleep(0.5)

    @staticmethod
    def _print_comparison(baseline: SearchStats, concurrent: SearchStats):
        """Print latency comparison table."""
        deg = concurrent.degradation_vs(baseline)
        print(f"\nLatency comparison (concurrent / baseline):")
        print(f"  p50:  {baseline.p50_ms:6.1f}ms -> {concurrent.p50_ms:6.1f}ms  ({deg['p50']:.1f}x)")
        print(f"  p90:  {baseline.p90_ms:6.1f}ms -> {concurrent.p90_ms:6.1f}ms  ({deg['p90']:.1f}x)")
        print(f"  p99:  {baseline.p99_ms:6.1f}ms -> {concurrent.p99_ms:6.1f}ms  ({deg['p99']:.1f}x)")
        print(f"  max:  {baseline.max_ms:6.1f}ms -> {concurrent.max_ms:6.1f}ms  ({deg['max']:.1f}x)")

    @staticmethod
    def _print_index_stats(stats: OperationStats):
        """Print indexing statistics."""
        if stats.durations_ms:
            total = sum(stats.durations_ms) / 1000
            print(f"Indexes: {stats.completed} created in {total:.1f}s total")
            for i, dur in enumerate(stats.durations_ms):
                print(f"  - Index {i+1}: {dur:.0f}ms")
        else:
            print(f"Indexes: {stats.completed} created")

    def _print_results(
        self,
        baseline: SearchStats,
        search_stats: SearchStats,
        index_stats: OperationStats,
        update_stats: OperationStats = None,
    ):
        """Print benchmark results and comparison."""
        print(f"\nConcurrent: {search_stats.summary()}")
        if update_stats:
            print(f"Updates: {update_stats.completed} batches, {len(update_stats.errors)} errors")
        self._print_index_stats(index_stats)

        print(f"\n{'='*60}")
        print("COMPARISON")
        print(f"{'='*60}")
        self._print_comparison(baseline, search_stats)

        if search_stats.timeouts:
            print(f"\n  WARNING: {search_stats.timeouts} search timeouts")
        if search_stats.errors:
            print(f"\n  WARNING: {len(search_stats.errors)} search errors")

        print(f"\n{'='*60}")
        print("BENCHMARK COMPLETE")
        print(f"{'='*60}")

    def _setup_collection(self, client: ClientUtils):
        """Create collection and insert initial data."""
        print(f"\n{'='*60}")
        print("SETUP")
        print(f"{'='*60}")
        print(f"Creating collection '{COLLECTION_NAME}'...")
        print(f"  Vector size: {VECTOR_SIZE}, Points: {NUM_POINTS:,}")

        collection_config = {
            "vectors": {"size": VECTOR_SIZE, "distance": "Cosine"},
            "optimizers_config": {"default_segment_number": 4, "indexing_threshold": 1000},
        }
        client.create_collection(COLLECTION_NAME, collection_config)

        print("Inserting data...")
        self._insert_data(client, NUM_POINTS)
        self._wait_for_green(client)

    def _run_baseline(self, client: ClientUtils) -> SearchStats:
        """Run baseline search benchmark."""
        print(f"\n{'='*60}")
        print("BASELINE (search only)")
        print(f"{'='*60}")

        baseline_duration = 10
        print(f"Running baseline for {baseline_duration}s...")
        stats = self._run_search_baseline(client, baseline_duration)
        print(f"Baseline: {stats.summary()}")

        assert stats.count > 0, "No baseline searches completed"
        assert not stats.errors, f"Baseline had errors: {stats.errors}"
        return stats

    @pytest.mark.skip
    def test_search_with_updates_and_indexing(self, qdrant_container_factory):
        """
        Benchmark search latency during concurrent updates AND payload indexing.

        Runs three concurrent operations:
        - Search queries (measuring latency)
        - Point upserts (100 points every 200ms)
        - Payload index creation
        """
        container_info = qdrant_container_factory()
        client = ClientUtils(host=container_info.host, port=container_info.http_port, timeout=30)
        assert client.wait_for_server(), "Server failed to start"

        self._setup_collection(client)
        baseline = self._run_baseline(client)

        # Concurrent phase
        print(f"\n{'='*60}")
        print("CONCURRENT (search + updates + indexing)")
        print(f"{'='*60}")

        search_stats = SearchStats()
        update_stats = OperationStats()
        index_stats = OperationStats()
        stop = threading.Event()

        threads = [
            threading.Thread(target=self._search_loop, args=(client, stop, search_stats)),
            threading.Thread(target=self._update_loop, args=(client, stop, update_stats)),
            threading.Thread(target=self._indexing_loop, args=(client, stop, index_stats)),
        ]

        duration = 30
        print(f"Running for {duration}s...")
        print("  - Search: measuring latency")
        print("  - Updates: 100 points every 200ms")
        print("  - Indexing: creating payload indexes")

        for t in threads:
            t.start()
        time.sleep(duration)
        stop.set()
        for t in threads:
            t.join(timeout=10)

        self._print_results(baseline, search_stats, index_stats, update_stats)

    def test_search_with_indexing_only(self, qdrant_container_factory):
        """
        Benchmark search latency during payload indexing only (NO concurrent updates).

        Isolates indexing impact for comparison with the full test.
        """
        container_info = qdrant_container_factory()
        client = ClientUtils(host=container_info.host, port=container_info.http_port, timeout=30)
        assert client.wait_for_server(), "Server failed to start"

        self._setup_collection(client)
        baseline = self._run_baseline(client)

        # Concurrent phase (no updates)
        print(f"\n{'='*60}")
        print("CONCURRENT (search + indexing, NO updates)")
        print(f"{'='*60}")

        search_stats = SearchStats()
        index_stats = OperationStats()
        stop = threading.Event()

        threads = [
            threading.Thread(target=self._search_loop, args=(client, stop, search_stats)),
            threading.Thread(target=self._indexing_loop, args=(client, stop, index_stats)),
        ]

        duration = 30
        print(f"Running for {duration}s...")
        print("  - Search: measuring latency")
        print("  - Indexing: creating payload indexes")
        print("  - NO updates")

        for t in threads:
            t.start()
        time.sleep(duration)
        stop.set()
        for t in threads:
            t.join(timeout=10)

        self._print_results(baseline, search_stats, index_stats)