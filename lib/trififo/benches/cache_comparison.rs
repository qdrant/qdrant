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

use std::hash::{Hash, Hasher};
use std::hint::black_box;
use std::sync::Arc;
use std::{alloc, thread};

use cap::Cap;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use foyer::{EvictionConfig, S3FifoConfig};
use parking_lot::Mutex;
use quick_cache::sync::Cache as QuickCache;
use rand::distr::{Distribution, Uniform};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::Zipf;
use schnellru::{ByLength, LruMap};

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
fn generate_zipf_keys(n: usize, num_unique: u64, exponent: f64, seed: u64) -> Vec<Key> {
    let mut rng = StdRng::seed_from_u64(seed);
    let zipf = Zipf::new(num_unique as f64, exponent).unwrap();

    (0..n)
        .map(|_| {
            let id = zipf.sample(&mut rng) as u64;
            Key::new((id / 1_000_000) as i32, (id % 1_000_000) as u32)
        })
        .collect()
}

/// Generate sequential keys (testing sequential read efficiency)
fn generate_sequential_keys(n: usize) -> Vec<Key> {
    (0..n)
        .map(|i| Key::new((i / 1_000_000) as i32, (i % 1_000_000) as u32))
        .collect()
}

/// Generate a scan-resistant workload pattern.
/// This simulates a mix of:
/// - Hot working set (frequently accessed)
/// - Sequential scans (should not evict working set)
fn generate_scan_resistant_keys(
    n: usize,
    working_set_size: u64,
    scan_size: u64,
    scan_frequency: f64,
    seed: u64,
) -> Vec<Key> {
    let mut rng = StdRng::seed_from_u64(seed);
    let zipf = Zipf::new(working_set_size as f64, 1.0).unwrap();
    let uniform_scan = Uniform::new(working_set_size, working_set_size + scan_size).unwrap();

    (0..n)
        .map(|_| {
            let is_scan: bool = rng.random_bool(scan_frequency);
            let id = if is_scan {
                // Sequential scan access (cold data)
                uniform_scan.sample(&mut rng)
            } else {
                // Hot working set access
                zipf.sample(&mut rng) as u64
            };
            Key::new((id / 1_000_000) as i32, (id % 1_000_000) as u32)
        })
        .collect()
}

/// Generate uniform random keys
fn generate_uniform_keys(n: usize, num_unique: u64, seed: u64) -> Vec<Key> {
    let mut rng = StdRng::seed_from_u64(seed);
    let uniform = Uniform::new(0u64, num_unique).unwrap();

    (0..n)
        .map(|_| {
            let id = uniform.sample(&mut rng);
            Key::new((id / 1_000_000) as i32, (id % 1_000_000) as u32)
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
        Self {
            cache: QuickCache::new(capacity),
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
                .with_shards(6)
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
const CACHE_NAMES: &[&str] = &["quick_cache", "schnellru", "foyer"];

fn create_cache(name: &str, capacity: usize) -> Arc<dyn CacheBench> {
    match name {
        "quick_cache" => Arc::new(QuickCacheWrapper::new(capacity)),
        "schnellru" => Arc::new(SchnellruWrapper::new(capacity as u32)),
        "foyer" => Arc::new(FoyerWrapper::new(capacity)),
        // "trififo" => Arc::new(TrififoWrapper::new(capacity)),
        _ => panic!("Unknown cache: {name}"),
    }
}

// =============================================================================
// Memory Usage Check
// =============================================================================

/// Target: 26,214,400 entries. If each page is 4KB, this many entries are worth 100GB of data
const MEMORY_BENCH_ENTRIES: usize = 26_214_400;

fn measure_memory_usage<F, T>(name: &str, capacity: usize, create_cache: F)
where
    F: FnOnce() -> T,
    T: CacheBench,
{
    // Force garbage collection / deallocation
    std::thread::sleep(std::time::Duration::from_millis(100));

    let before = ALLOCATOR.allocated();
    let cache = create_cache();

    // Insert entries
    let keys = generate_sequential_keys(capacity);
    for (i, key) in keys.iter().enumerate() {
        cache.insert(*key, i as u32);
    }

    let after = ALLOCATOR.allocated();
    let total_bytes = after.saturating_sub(before);
    let bytes_per_entry = total_bytes as f64 / capacity as f64;

    println!(
        "{name}: {capacity} entries, {total_bytes} bytes total, ~{bytes_per_entry:.1} bytes/entry"
    );

    // Verify entries are actually cached (sample check)
    let mut hits = 0;
    for (i, key) in keys.iter().enumerate() {
        if cache.get(key) == Some(i as u32) {
            hits += 1;
        }
    }
    println!("  {hits}/{capacity} entries correctly cached");
}

/// Insert 26,214,400 entries and measure memory usage
fn test_memory_usage() {
    println!("=== Memory Usage Report ===");
    measure_memory_usage("quick_cache", MEMORY_BENCH_ENTRIES, || {
        QuickCacheWrapper::new(MEMORY_BENCH_ENTRIES)
    });

    measure_memory_usage("schnellru", MEMORY_BENCH_ENTRIES, || {
        SchnellruWrapper::new(MEMORY_BENCH_ENTRIES as u32)
    });

    measure_memory_usage("foyer", MEMORY_BENCH_ENTRIES, || {
        FoyerWrapper::new(MEMORY_BENCH_ENTRIES)
    });

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
    let num_accesses = 1_000_000;
    let cache_capacity = 100_000;

    // Different workload patterns
    let zipf_keys = generate_zipf_keys(num_accesses, cache_capacity as u64 * 10, 1.0, 42);
    let uniform_keys = generate_uniform_keys(num_accesses, cache_capacity as u64 * 10, 42);
    let sequential_keys = generate_sequential_keys(num_accesses);
    // Scan-resistant workload: 80% hot set accesses, 20% sequential scans
    let scan_resistant_keys = generate_scan_resistant_keys(
        num_accesses,
        cache_capacity as u64 / 2, // working set is half the cache
        cache_capacity as u64 * 5, // scans are 5x cache size
        0.2,                       // 20% of accesses are scans
        42,
    );

    println!("\n=== Hit Ratio Report ===");
    println!("Cache capacity: {cache_capacity}");
    println!("Number of accesses: {num_accesses}");
    println!();

    for (pattern_name, keys) in [
        ("zipf_1.0", &zipf_keys),
        ("uniform", &uniform_keys),
        ("sequential", &sequential_keys),
        ("scan_resistant", &scan_resistant_keys),
    ] {
        println!("Pattern: {pattern_name}");

        for cache_name in CACHE_NAMES {
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

    let insert_keys = generate_zipf_keys(OPS_PER_ITER, CACHE_CAPACITY as u64 * 2, 1.0, 42);
    for cache_name in CACHE_NAMES {
        group.bench_with_input(
            BenchmarkId::new("insert", cache_name),
            cache_name,
            |b, &name| {
                let cache = create_cache(name, CACHE_CAPACITY);

                b.iter(|| {
                    for (i, key) in insert_keys.iter().enumerate() {
                        cache.insert(*key, i as u32);
                    }
                });
            },
        );
    }

    // Pre-fill caches for get benchmarks
    let prefill_keys = generate_sequential_keys(CACHE_CAPACITY);

    for cache_name in CACHE_NAMES {
        group.bench_with_input(
            BenchmarkId::new("get_hit", cache_name),
            cache_name,
            |b, &name| {
                let cache = create_cache(name, CACHE_CAPACITY);

                // Pre-fill
                for (i, key) in prefill_keys.iter().enumerate() {
                    cache.insert(*key, i as u32);
                }

                b.iter(|| {
                    for key in prefill_keys.iter().cycle().take(OPS_PER_ITER) {
                        black_box(cache.get(key));
                    }
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
        .map(|t| generate_zipf_keys(OPS_PER_THREAD, CACHE_CAPACITY as u64 * 10, 1.0, t as u64))
        .collect();

    // Benchmark: Concurrent get-or-insert under Zipf distribution
    // This measures how each cache handles the common pattern of:
    // "get if exists, otherwise fetch and insert"
    for cache_name in CACHE_NAMES {
        group.bench_with_input(
            BenchmarkId::new("get_or_insert", cache_name),
            &(cache_name, &thread_keys),
            |b, &(name, keys)| {
                b.iter(|| {
                    let cache = create_cache(name, CACHE_CAPACITY);

                    let handles: Vec<_> = keys
                        .iter()
                        .enumerate()
                        .map(|(t, thread_keys)| {
                            let cache = Arc::clone(&cache);
                            let keys = thread_keys.clone();
                            thread::spawn(move || {
                                for (i, key) in keys.iter().enumerate() {
                                    let value = (t * OPS_PER_THREAD + i) as u32;
                                    black_box(cache.get_or_insert(*key, value));
                                }
                            })
                        })
                        .collect();

                    for handle in handles {
                        handle.join().unwrap();
                    }

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
    // test_memory_usage();
    // test_hit_ratio();

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
