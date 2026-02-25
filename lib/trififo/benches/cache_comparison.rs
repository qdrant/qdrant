//! Cache comparison benchmarks for trififo.
//!
//! This benchmark compares trififo against other cache implementations:
//! - quick_cache: High-performance concurrent cache using CLOCK-Pro
//! - schnellru: Simple LRU cache (single-threaded, needs mutex)
//! - foyer: Hybrid cache with disk tier support. Supports S3-FIFO
//!
//! Metrics measured:
//! - Memory usage (estimated bytes per entry)
//! - Cache hit ratio under different access patterns
//! - Single-threaded latency (insert/get operations)
//! - Multi-threaded latency (16 threads)

use std::alloc;
use std::hash::{Hash, Hasher};
use std::hint::black_box;
use std::sync::Arc;

use cap::Cap;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use foyer::{EvictionConfig, S3FifoConfig};
use itertools::Itertools;
use parking_lot::Mutex;
use quick_cache::sync::Cache as QuickCache;
use rand::distr::Distribution;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rand_distr::Zipf;
use rayon::prelude::*;
use schnellru::{ByLength, LruMap};
use strum::{EnumIter, IntoEnumIterator};

/// Cache key representing a file descriptor and page offset.
///
/// Total size: 8 bytes (i32 + u32)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Key {
    /// File descriptor
    pub fd: i32,
    /// Offset within the file, in page units
    pub page: u32,
}

impl Key {
    /// Create a new cache key.
    #[inline]
    pub const fn new(fd: i32, page: u32) -> Self {
        Self { fd, page }
    }

    pub const fn from_u64(u: u64) -> Self {
        let fd = (u >> 32) as i32;
        let page = u as u32;
        Self { fd, page }
    }
}

impl Hash for Key {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash as a single u64 for efficiency
        let combined = (self.fd as u64) << 32 | u64::from(self.page);
        combined.hash(state);
    }
}

// =============================================================================
// Memory Tracking Allocator
// =============================================================================

/// A global allocator wrapper that tracks memory usage using the `cap` crate.
#[global_allocator]
static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::MAX);

// =============================================================================
// Workload Generators
// =============================================================================

/// Generate keys following a Zipf distribution (realistic hot/cold access pattern)
fn generate_zipf_keys(n: usize, num_unique: usize, exponent: f64, seed: u64) -> Vec<Key> {
    let mut rng = StdRng::seed_from_u64(seed);
    let zipf = Zipf::new(num_unique as f64, exponent).unwrap();

    (0..n)
        .map(|_| {
            let id = zipf.sample(&mut rng) as u64;
            Key::from_u64(id)
        })
        .collect()
}

/// Generate sequential keys
fn generate_sequential_keys(n: usize) -> Vec<Key> {
    (0..n).map(|i| Key::from_u64(i as u64)).collect()
}

/// Generate a scan-resistant workload pattern.
/// This simulates a mix of:
/// - Hot working set (frequently accessed)
/// - Sequential scans (should not evict working set)
fn generate_scan_resistant_keys(
    n: usize,
    num_unique: usize,
    scan_size: usize,
    scan_frequency: f64,
    seed: u64,
) -> Vec<Key> {
    let mut rng = StdRng::seed_from_u64(seed);
    let zipf = Zipf::new(num_unique as f64, 1.0).unwrap();
    let mut scan = (0..num_unique as u64).cycle();

    let mut keys = Vec::with_capacity(n);

    while keys.len() < n {
        let is_scan: bool = rng.random_bool(scan_frequency);
        if is_scan {
            // Sequential scan access (cold data)
            while keys.len() < n.min(keys.len() + scan_size) {
                let id = scan.next().unwrap();
                let key = Key::from_u64(id);
                keys.push(key);
            }
        } else {
            // Hot working set access
            let id = zipf.sample(&mut rng) as u64;
            let key = Key::from_u64(id);
            keys.push(key);
        }
    }

    keys
}

/// Generate sequential keys, but repeat each one exactly twice
fn generate_duplicate_keys(num_unique: usize) -> Vec<Key> {
    (0..num_unique)
        .flat_map(|id| {
            let key = Key::from_u64(id as u64);
            [key, key] // Each key appears exactly twice in sequence
        })
        .collect()
}

// =============================================================================
// Cache Wrappers
// =============================================================================

trait CacheBench: Send + Sync {
    fn insert(&self, key: Key, value: u32);
    fn get(&self, key: &Key) -> Option<u32>;
    /// Get a value from the cache, or insert it if not present.
    /// Returns the value (either existing or newly inserted).
    fn get_or_insert(&self, key: Key, value: u32) -> u32;
}

// Quick Cache wrapper
struct QuickCacheWrapper {
    cache: QuickCache<Key, u32>,
}

impl QuickCacheWrapper {
    fn new(capacity: usize) -> Self {
        let options = quick_cache::OptionsBuilder::new()
            .estimated_items_capacity(capacity)
            .hot_allocation(0.9)
            .weight_capacity(capacity as u64)
            .build()
            .unwrap();
        Self {
            cache: QuickCache::with_options(
                options,
                quick_cache::UnitWeighter,
                ahash::RandomState::new(),
                Default::default(),
            ),
        }
    }
}

impl CacheBench for QuickCacheWrapper {
    fn insert(&self, key: Key, value: u32) {
        self.cache.insert(key, value);
    }

    fn get(&self, key: &Key) -> Option<u32> {
        self.cache.get(key)
    }

    fn get_or_insert(&self, key: Key, value: u32) -> u32 {
        self.cache
            .get_or_insert_with(&key, || Ok::<_, ()>(value))
            .unwrap()
    }
}

// Schnellru wrapper (needs mutex for thread safety)
struct SchnellruWrapper {
    cache: Mutex<LruMap<Key, u32, ByLength>>,
}

impl SchnellruWrapper {
    fn new(capacity: u32) -> Self {
        Self {
            cache: Mutex::new(LruMap::new(ByLength::new(capacity))),
        }
    }
}

impl CacheBench for SchnellruWrapper {
    fn insert(&self, key: Key, value: u32) {
        self.cache.lock().insert(key, value);
    }

    fn get(&self, key: &Key) -> Option<u32> {
        self.cache.lock().get(key).copied()
    }

    fn get_or_insert(&self, key: Key, value: u32) -> u32 {
        let mut cache = self.cache.lock();
        // get_or_insert returns Option because the limiter might reject the insert
        // In our case with ByLength limiter, it should always succeed if capacity > 0
        *cache.get_or_insert(key, || value).expect("capacity is > 0")
    }
}

// Foyer in-memory cache wrapper
struct FoyerWrapper {
    cache: foyer::Cache<Key, u32>,
}

impl FoyerWrapper {
    fn new(capacity: usize) -> Self {
        Self {
            cache: foyer::CacheBuilder::new(capacity)
                .with_eviction_config(EvictionConfig::S3Fifo(S3FifoConfig {
                    small_queue_capacity_ratio: 0.1,
                    ghost_queue_capacity_ratio: 0.5,
                    small_to_main_freq_threshold: 1,
                }))
                .build(),
        }
    }
}

impl CacheBench for FoyerWrapper {
    fn insert(&self, key: Key, value: u32) {
        self.cache.insert(key, value);
    }

    fn get(&self, key: &Key) -> Option<u32> {
        self.cache.get(key).map(|e| *e.value())
    }

    fn get_or_insert(&self, key: Key, value: u32) -> u32 {
        // Foyer doesn't have a native get_or_insert, so we implement it manually
        if let Some(entry) = self.cache.get(&key) {
            *entry.value()
        } else {
            let entry = self.cache.insert(key, value);
            *entry.value()
        }
    }
}

// TODO: trififo wrapper
// struct TrififoWrapper {
//     cache: trififo::Cache,
// }
//
// impl TrififoWrapper {
//     fn new(capacity: usize) -> Self {
//         Self {
//             cache: trififo::Cache::new(capacity),
//         }
//     }
// }
//
// impl CacheBench for TrififoWrapper {
//     fn insert(&self, key: Key, value: u32) {
//         self.cache.insert(key, value);
//     }
//
//     fn get(&self, key: &Key) -> Option<u32> {
//         self.cache.get(key)
//     }
//
// }

/// List of cache implementations to benchmark.
/// Add "trififo" here when implementation is ready.
#[derive(EnumIter, Copy, Clone)]
enum CacheName {
    QuickCache,
    Schnellru,
    Foyer,
    // Trififo,
}

impl std::fmt::Display for CacheName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheName::QuickCache => write!(f, "quick_cache"),
            CacheName::Schnellru => write!(f, "schnellru"),
            CacheName::Foyer => write!(f, "foyer"),
            // CacheName::Trififo => write!(f, "trififo"),
        }
    }
}

fn create_cache(name: CacheName, capacity: usize) -> Arc<dyn CacheBench> {
    match name {
        CacheName::QuickCache => Arc::new(QuickCacheWrapper::new(capacity)),
        CacheName::Schnellru => Arc::new(SchnellruWrapper::new(capacity as u32)),
        CacheName::Foyer => Arc::new(FoyerWrapper::new(capacity)),
        // CacheName::Trififo => Arc::new(TrififoWrapper::new(capacity)),
    }
}

// =============================================================================
// Memory Usage Check
// =============================================================================

/// Target: 26,214,400 entries. If each page is 4KB, this many entries are worth 100GB of data
const MEMORY_BENCH_ENTRIES: usize = 26_214_400;

fn measure_memory_usage<F>(name: CacheName, capacity: usize, create_cache: F)
where
    F: FnOnce() -> Arc<dyn CacheBench>,
{
    // Force garbage collection / deallocation
    std::thread::sleep(std::time::Duration::from_millis(100));

    let keys = generate_duplicate_keys(capacity);

    let before = ALLOCATOR.allocated();
    let cache = create_cache();

    // Insert entries
    for key in keys.iter() {
        cache.insert(*key, key.page);
    }

    let after = ALLOCATOR.allocated();
    let total_bytes = after.saturating_sub(before);
    let bytes_per_entry = total_bytes as f64 / capacity as f64;

    println!(
        "{name}: {capacity} entries, {total_bytes} bytes total, ~{bytes_per_entry:.1} bytes/entry"
    );

    // Verify entries are actually cached
    let mut hits = 0;
    for key in keys.iter().dedup() {
        if cache.get(key) == Some(key.page) {
            hits += 1;
        }
    }
    println!("  {hits}/{capacity} entries correctly cached");
}

/// Insert 26,214,400 entries and measure memory usage
fn test_memory_usage() {
    println!("=== Memory Usage Report ===");
    for name in CacheName::iter() {
        measure_memory_usage(name, MEMORY_BENCH_ENTRIES, || {
            create_cache(name, MEMORY_BENCH_ENTRIES)
        });
    }

    println!();
}

// =============================================================================
// Hit Ratio Check
// =============================================================================

/// Measure hit ratio by simulating a cache workload.
/// For each key access: if it's a hit, count it; if it's a miss, insert it.
fn measure_hit_ratio(cache: &dyn CacheBench, keys: &[Key]) -> f64 {
    let mut hits = 0usize;
    let total = keys.len();

    for (i, key) in keys.iter().enumerate() {
        if cache.get(key).is_some() {
            hits += 1;
        } else {
            cache.insert(*key, i as u32);
        }
    }

    hits as f64 / total as f64
}

/// Test that reports hit ratios for different cache implementations and workload patterns.
fn test_hit_ratio() {
    let num_accesses = 10_000_000;
    let cache_capacity = 100_000;
    let key_space = cache_capacity * 10;

    // Different workload patterns
    let zipf_1_2 = generate_zipf_keys(num_accesses, key_space, 1.2, 42);
    let zipf_1_0 = generate_zipf_keys(num_accesses, key_space, 1.0, 42);
    let sequential_keys = generate_sequential_keys(num_accesses);
    let scan_resistant_keys = generate_scan_resistant_keys(
        num_accesses,
        key_space,
        cache_capacity / 10_000, // scans are 1/10_000 the cache size
        0.00001,                 // 0.001% of probability to get a scan
        42,
    );

    println!("\n=== Hit Ratio Report ===");
    println!("Cache capacity: {cache_capacity}");
    println!("Number of accesses: {num_accesses}");
    println!();

    for (pattern_name, keys) in [
        ("zipf_1.2", &zipf_1_2),
        ("zipf_1.0", &zipf_1_0),
        ("sequential", &sequential_keys),
        ("scan_resistant", &scan_resistant_keys),
    ] {
        println!("Pattern: {pattern_name}");

        for cache_name in CacheName::iter() {
            let cache = create_cache(cache_name, cache_capacity);

            let hit_ratio = measure_hit_ratio(cache.as_ref(), keys);
            println!("  {cache_name}: {:.2}%", hit_ratio * 100.0);
        }
        println!();
    }
}

// =============================================================================
// Latency Benchmarks
// =============================================================================

const CACHE_CAPACITY: usize = 100_000;
const OPS_PER_ITER: usize = 1_000_000;

fn bench_single_thread_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_thread_latency");

    // Benchmark insert operations
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    let insert_keys = generate_sequential_keys(CACHE_CAPACITY * 10);
    for cache_name in CacheName::iter() {
        group.bench_with_input(
            BenchmarkId::new("insert", cache_name),
            &cache_name,
            |b, &name| {
                b.iter_batched(
                    || create_cache(name, CACHE_CAPACITY),
                    |cache| {
                        for (i, key) in insert_keys.iter().cycle().enumerate().take(OPS_PER_ITER) {
                            cache.insert(*key, i as u32);
                        }
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    // Pre-fill caches for get benchmarks
    let prefill_keys = generate_duplicate_keys(CACHE_CAPACITY);

    for cache_name in CacheName::iter() {
        let cache = create_cache(cache_name, CACHE_CAPACITY);
        // Pre-fill
        for (i, key) in prefill_keys.iter().enumerate() {
            cache.insert(*key, i as u32);
        }

        group.bench_with_input(
            BenchmarkId::new("get_hit", cache_name),
            &cache_name,
            |b, _name| {
                b.iter(|| {
                    let mut hits = 0;
                    for key in prefill_keys.iter().cycle().take(OPS_PER_ITER) {
                        if cache.get(key).is_some() {
                            hits += 1;
                        }
                    }
                    assert!(hits as f32 > OPS_PER_ITER as f32 * 0.9);
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Multi-threaded Latency Benchmarks
// =============================================================================

const NUM_THREADS: usize = 16;
const OPS_PER_THREAD: usize = 100_000;

fn bench_multi_thread_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_thread_latency");
    group.sample_size(20);

    let total_ops = NUM_THREADS * OPS_PER_THREAD;
    group.throughput(Throughput::Elements(total_ops as u64));

    // Generate Zipf-distributed keys for each thread (simulating realistic hot/cold access patterns)
    // Each thread gets its own key sequence with a different seed to avoid identical access patterns
    let thread_keys: Vec<Vec<Key>> = (0..NUM_THREADS)
        .map(|t| generate_zipf_keys(OPS_PER_THREAD, CACHE_CAPACITY * 10, 1.2, t as u64))
        .collect();

    // Benchmark: Concurrent get-or-insert under Zipf distribution
    // This measures how each cache handles the common pattern of:
    // "get if exists, otherwise fetch and insert"
    for cache_name in CacheName::iter() {
        group.bench_with_input(
            BenchmarkId::new("get_or_insert", cache_name),
            &(cache_name, &thread_keys),
            |b, &(name, keys)| {
                b.iter(|| {
                    let cache = create_cache(name, CACHE_CAPACITY);

                    keys.par_iter().enumerate().for_each(|(t, thread_keys)| {
                        for (i, key) in thread_keys.iter().enumerate() {
                            let value = (t * OPS_PER_THREAD + i) as u32;
                            black_box(cache.get_or_insert(*key, value));
                        }
                    });

                    black_box(cache)
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Main
// =============================================================================

fn bench_all(c: &mut Criterion) {
    test_memory_usage();
    test_hit_ratio();

    bench_single_thread_latency(c);
    bench_multi_thread_latency(c);
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .significance_level(0.05)
        .measurement_time(std::time::Duration::from_secs(5));
    targets = bench_all
}

criterion_main!(benches);
