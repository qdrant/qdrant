use std::hash::{BuildHasher, Hash};
use std::num::NonZeroUsize;

use crate::array_lookup::AsIndex;
use crate::cache::Cache;
use crate::guard::GetOrGuard;
use crate::lifecycle::{Lifecycle, NoLifecycle};

/// A sharded cache that distributes keys across multiple [`Cache`] instances
/// to reduce lock contention in multi-threaded scenarios.
///
/// Each shard is an independent [`Cache`] with its own locks. By distributing
/// keys across shards based on their hash, concurrent operations on different
/// keys are likely to hit different shards, reducing contention.
///
/// # Design
///
/// - Uses fast O(1) shard selection via bitmask (num_shards must be power-of-2)
/// - Each shard is a fully independent S3-FIFO cache with its own seqlock + writer mutex
/// - The total capacity is divided among shards (capacity_per_shard = total_capacity / num_shards)
/// - Guard-based coalescing (`get_or_guard`) works per-shard, so only threads
///   accessing the same shard contend on the same mutex
pub struct ShardedCache<K, V, L = NoLifecycle, S = ahash::RandomState> {
    /// The individual cache shards
    shards: Box<[Cache<K, V, L>]>,

    /// Shard selection strategy for O(1) lookups
    shard_selector: ShardSelector,

    /// Hasher for computing key hashes (used only for shard selection)
    hasher: S,
}

impl<K, V, L, S> std::fmt::Debug for ShardedCache<K, V, L, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let num_shards = self.shards.len();
        f.debug_struct("ShardedCache")
            .field("num_shards", &num_shards)
            .finish()
    }
}

/// Fast O(1) shard selection using bitmask.
///
/// Requires power-of-2 shard count for optimal performance.
/// shard_index = hash & mask, where mask = num_shards - 1
#[derive(Clone, Copy)]
struct ShardSelector {
    mask: u64,
}

impl ShardSelector {
    fn new(num_shards: usize) -> Self {
        assert!(
            num_shards.is_power_of_two(),
            "num_shards ({num_shards}) must be a power of 2 for optimal performance"
        );
        Self {
            mask: (num_shards - 1) as u64,
        }
    }

    #[inline(always)]
    fn select(&self, hash: u64) -> usize {
        (hash & self.mask) as usize
    }
}

impl<K, V> ShardedCache<K, V, NoLifecycle, ahash::RandomState>
where
    K: Copy + Hash + Eq + AsIndex,
    V: Clone,
{
    /// Create a new sharded cache with the specified total capacity and number of shards.
    ///
    /// Uses default S3-FIFO parameters: `small_ratio = 0.1`, `ghost_ratio = 0.9`.
    ///
    /// The capacity is divided evenly among shards. For best performance, choose
    /// a number of shards that is a power of 2 (required) and roughly matches
    /// the expected concurrency level (e.g., number of CPU cores).
    ///
    /// # Panics
    ///
    /// Panics if `num_shards` is not a power of 2, or if `capacity` is less than `num_shards`.
    pub fn new(capacity: usize, num_shards: NonZeroUsize) -> Self {
        Self::with_config(capacity, num_shards, 0.1, 0.9, NoLifecycle)
    }
}

impl<K, V, L, S> ShardedCache<K, V, L, S>
where
    K: Copy + Hash + Eq + AsIndex,
    V: Clone,
    L: Lifecycle<K, V> + Clone,
    S: BuildHasher + Default,
{
    /// Create a new sharded cache with full configuration options.
    ///
    /// # Parameters
    ///
    /// - `capacity`: Total capacity across all shards
    /// - `num_shards`: Number of shards (must be a power of 2)
    /// - `small_ratio`: Ratio of capacity for the small FIFO queue (typically 0.1)
    /// - `ghost_ratio`: Ratio of capacity for the ghost queue (typically 0.9)
    /// - `lifecycle`: Lifecycle hooks for cache events
    ///
    /// # Panics
    ///
    /// Panics if `num_shards` is not a power of 2, or if `capacity` is less than `num_shards`.
    pub fn with_config(
        capacity: usize,
        num_shards: NonZeroUsize,
        small_ratio: f32,
        ghost_ratio: f32,
        lifecycle: L,
    ) -> Self {
        let num_shards = num_shards.get();
        assert!(
            capacity >= num_shards,
            "capacity ({capacity}) must be at least num_shards ({num_shards})"
        );

        let hasher = S::default();

        let capacity_per_shard = capacity / num_shards;
        // Distribute remainder among first shards
        let remainder = capacity % num_shards;

        let shards = (0..num_shards)
            .map(|i| {
                let shard_capacity = if i < remainder {
                    capacity_per_shard + 1
                } else {
                    capacity_per_shard
                };
                Cache::with_config(shard_capacity, small_ratio, ghost_ratio, lifecycle.clone())
            })
            .collect();

        let shard_selector = ShardSelector::new(num_shards);

        Self {
            shards,
            shard_selector,
            hasher,
        }
    }

    /// Compute the hash for a key.
    #[inline(always)]
    fn hash_key<Q: Hash + ?Sized>(&self, key: &Q) -> u64 {
        self.hasher.hash_one(key)
    }

    /// Get the shard for a given hash.
    #[inline(always)]
    fn get_shard_by_hash(&self, hash: u64) -> &Cache<K, V, L> {
        let idx = self.shard_selector.select(hash);
        // SAFETY: idx is always valid because shard_selector.mask < shards.len()
        unsafe { self.shards.get_unchecked(idx) }
    }

    /// Read a value from the cache.
    ///
    /// Returns a cloned value (if present).
    #[inline]
    pub fn get(&self, key: &K) -> Option<V> {
        let hash = self.hash_key(key);
        self.get_shard_by_hash(hash).get(key)
    }

    /// Insert a key-value pair into the cache.
    #[inline]
    pub fn insert(&self, key: K, value: V) {
        let hash = self.hash_key(&key);
        self.get_shard_by_hash(hash).insert(key, value);
    }

    /// Look up `key` in the cache, with guard-based coalescing for concurrent misses.
    ///
    /// - If the value is cached, returns [`GetOrGuard::Found(value)`].
    /// - If another thread is already loading the same key (on the same shard),
    ///   **blocks** until that thread completes (or abandons) and then returns
    ///   the value or retries.
    /// - Otherwise, returns a [`GetOrGuard::Guard`] that the caller must
    ///   complete by calling [`CacheGuard::insert`].
    ///
    /// This ensures that at most **one** thread performs the (potentially
    /// expensive) load for any given key at a time, preventing the
    /// "thundering herd" problem on cache misses.
    ///
    /// Because the cache is sharded, threads accessing different shards do not
    /// contend with each other at all.
    #[inline]
    pub fn get_or_guard(&self, key: &K) -> GetOrGuard<'_, K, V, L> {
        let hash = self.hash_key(key);
        self.get_shard_by_hash(hash).get_or_guard(key)
    }

    /// Returns the total number of entries across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.is_empty())
    }

    /// Returns the number of shards.
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// Returns the length of each shard (useful for checking distribution).
    pub fn shard_lengths(&self) -> Vec<usize> {
        self.shards.iter().map(|s| s.len()).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn basic_insert_get() {
        let cache = ShardedCache::<u64, String>::new(100, NonZeroUsize::new(4).unwrap());

        cache.insert(1, "hello".to_string());

        let got = cache.get(&1);
        assert_eq!(got, Some("hello".to_string()));
    }

    #[test]
    fn multiple_shards() {
        let cache = ShardedCache::<u64, String>::new(100, NonZeroUsize::new(4).unwrap());

        for i in 0..50u64 {
            cache.insert(i, format!("val_{i}"));
        }

        for i in 0..50u64 {
            assert_eq!(cache.get(&i), Some(format!("val_{i}")), "key {i} not found");
        }

        // Check that entries are distributed across shards
        let lengths = cache.shard_lengths();
        let total: usize = lengths.iter().sum();
        assert_eq!(total, 50);

        // With 4 shards and 50 keys, each shard should have some entries
        for (i, &len) in lengths.iter().enumerate() {
            assert!(len > 0, "shard {i} has no entries, distribution is broken");
        }
    }

    #[test]
    fn concurrent_reads() {
        let cache = Arc::new(ShardedCache::<u64, String>::new(
            200,
            NonZeroUsize::new(4).unwrap(),
        ));

        for i in 0..50u64 {
            cache.insert(i, format!("val_{i}"));
        }

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let c = Arc::clone(&cache);
                thread::spawn(move || {
                    for _ in 0..100 {
                        for k in 0..50u64 {
                            let _ = c.get(&k);
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn multi_threaded_inserts() {
        let cache = Arc::new(ShardedCache::<u64, u64>::new(
            10_000,
            NonZeroUsize::new(8).unwrap(),
        ));

        const THREADS: usize = 8;
        const PER: u64 = 1_000;

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let c = Arc::clone(&cache);
                thread::spawn(move || {
                    let base = t as u64 * PER;
                    for i in 0..PER {
                        c.insert(base + i, base + i);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Every key that is still in the cache must have the correct value.
        for t in 0..THREADS {
            let base = t as u64 * PER;
            for i in 0..PER {
                if let Some(v) = cache.get(&(base + i)) {
                    assert_eq!(v, base + i);
                }
            }
        }
    }

    #[test]
    fn high_contention_inserts() {
        let cache = Arc::new(ShardedCache::<u64, u64>::new(
            200,
            NonZeroUsize::new(4).unwrap(),
        ));

        const THREADS: usize = 16;
        const INSERTS: u64 = 5_000;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let c = Arc::clone(&cache);
                thread::spawn(move || {
                    for i in 0..INSERTS {
                        let key = i % 50;
                        c.insert(key, i);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert!(cache.len() <= 200);
    }

    #[test]
    fn shard_distribution() {
        let cache = ShardedCache::<u64, u64>::new(1000, NonZeroUsize::new(8).unwrap());

        for i in 0..500u64 {
            cache.insert(i, i);
        }

        let lengths = cache.shard_lengths();
        let total: usize = lengths.iter().sum();
        assert_eq!(total, 500);

        // Each shard should have some entries (ahash distributes well)
        for (i, &len) in lengths.iter().enumerate() {
            assert!(
                len > 0,
                "shard {i} is empty — hash distribution is degenerate"
            );
        }
    }

    #[test]
    fn consistent_shard_selection() {
        let cache = ShardedCache::<u64, String>::new(100, NonZeroUsize::new(4).unwrap());

        cache.insert(42, "hello".to_string());

        // Multiple gets should consistently find the same shard
        for _ in 0..100 {
            assert_eq!(cache.get(&42), Some("hello".to_string()));
        }
    }

    #[test]
    fn power_of_two_bitmask() {
        // Verify the bitmask gives correct shard indices
        let selector = ShardSelector::new(8);
        for hash in 0..100u64 {
            assert!(selector.select(hash) < 8);
        }
    }

    #[test]
    #[should_panic(expected = "must be a power of 2")]
    fn non_power_of_two_panics() {
        let _ = ShardSelector::new(3);
    }

    // -----------------------------------------------------------------------
    // get_or_guard tests
    // -----------------------------------------------------------------------

    #[test]
    fn get_or_guard_basic() {
        let cache = ShardedCache::<u64, String>::new(100, NonZeroUsize::new(4).unwrap());

        match cache.get_or_guard(&1) {
            GetOrGuard::Guard(guard) => {
                guard.insert("hello".to_string());
            }
            GetOrGuard::Found(_) => panic!("expected Guard"),
        }

        match cache.get_or_guard(&1) {
            GetOrGuard::Found(v) => assert_eq!(v, "hello"),
            GetOrGuard::Guard(_) => panic!("expected Found"),
        }
    }

    #[test]
    fn get_or_guard_abandon() {
        let cache = ShardedCache::<u64, String>::new(100, NonZeroUsize::new(4).unwrap());

        // Take guard and drop without inserting.
        {
            let result = cache.get_or_guard(&1);
            match result {
                GetOrGuard::Guard(_guard) => { /* drop */ }
                GetOrGuard::Found(_) => panic!("expected Guard"),
            }
        }

        // Should be able to get a new guard.
        match cache.get_or_guard(&1) {
            GetOrGuard::Guard(guard) => {
                guard.insert("recovered".to_string());
            }
            GetOrGuard::Found(_) => panic!("expected Guard after abandon"),
        }

        assert_eq!(cache.get(&1), Some("recovered".to_string()));
    }

    #[test]
    fn get_or_guard_coalescing() {
        let cache = Arc::new(ShardedCache::<u64, String>::new(
            100,
            NonZeroUsize::new(4).unwrap(),
        ));
        let load_count = Arc::new(AtomicUsize::new(0));

        const THREADS: usize = 8;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let c = Arc::clone(&cache);
                let lc = Arc::clone(&load_count);
                thread::spawn(move || match c.get_or_guard(&42) {
                    GetOrGuard::Guard(guard) => {
                        lc.fetch_add(1, Ordering::Relaxed);
                        thread::sleep(Duration::from_millis(50));
                        guard.insert("loaded".to_string());
                    }
                    GetOrGuard::Found(v) => {
                        assert_eq!(v, "loaded");
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(load_count.load(Ordering::Relaxed), 1);
        assert_eq!(cache.get(&42), Some("loaded".to_string()));
    }

    #[test]
    fn get_or_guard_independent_keys() {
        let cache = Arc::new(ShardedCache::<u64, String>::new(
            200,
            NonZeroUsize::new(4).unwrap(),
        ));
        let load_count = Arc::new(AtomicUsize::new(0));

        const THREADS: usize = 8;

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let c = Arc::clone(&cache);
                let lc = Arc::clone(&load_count);
                let key = t as u64;
                thread::spawn(move || match c.get_or_guard(&key) {
                    GetOrGuard::Guard(guard) => {
                        lc.fetch_add(1, Ordering::Relaxed);
                        thread::sleep(Duration::from_millis(10));
                        guard.insert(format!("val_{key}"));
                    }
                    GetOrGuard::Found(_) => {}
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(load_count.load(Ordering::Relaxed), THREADS);
    }

    #[test]
    fn get_or_guard_abandon_wakes_waiters() {
        let cache = Arc::new(ShardedCache::<u64, String>::new(
            100,
            NonZeroUsize::new(4).unwrap(),
        ));
        let load_count = Arc::new(AtomicUsize::new(0));

        const THREADS: usize = 4;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let c = Arc::clone(&cache);
                let lc = Arc::clone(&load_count);
                thread::spawn(move || {
                    loop {
                        match c.get_or_guard(&99) {
                            GetOrGuard::Guard(guard) => {
                                let count = lc.fetch_add(1, Ordering::Relaxed);
                                if count == 0 {
                                    // First loader abandons.
                                    thread::sleep(Duration::from_millis(20));
                                    drop(guard);
                                    return;
                                } else {
                                    // Subsequent loader completes.
                                    guard.insert("finally".to_string());
                                    return;
                                }
                            }
                            GetOrGuard::Found(v) => {
                                assert_eq!(v, "finally");
                                return;
                            }
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(cache.get(&99), Some("finally".to_string()));
    }

    #[test]
    fn get_or_guard_stress() {
        let cache = Arc::new(ShardedCache::<u64, u64>::new(
            2000,
            NonZeroUsize::new(8).unwrap(),
        ));

        const THREADS: usize = 16;
        const KEY_SPACE: u64 = 500;
        const OPS_PER_THREAD: usize = 5_000;

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let c = Arc::clone(&cache);
                thread::spawn(move || {
                    use rand::Rng;
                    let mut rng = rand::rng();
                    for _ in 0..OPS_PER_THREAD {
                        let key = rng.random_range(0..KEY_SPACE);
                        match c.get_or_guard(&key) {
                            GetOrGuard::Guard(guard) => {
                                let value = key * 1000 + t as u64;
                                guard.insert(value);
                            }
                            GetOrGuard::Found(v) => {
                                // Value should be key * 1000 + some thread id
                                assert_eq!(v / 1000, key);
                            }
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    /// Fuzz test to ensure the sharded cache never returns incorrect values.
    #[test]
    fn fuzz_never_returns_unseen_value() {
        const CAPACITY: usize = 2048;
        const NUM_SHARDS: usize = 8;
        let cache = Arc::new(ShardedCache::<u64, u64>::new(
            CAPACITY,
            NonZeroUsize::new(NUM_SHARDS).unwrap(),
        ));

        const KEY_SPACE: u64 = 10240;
        const WRITERS: usize = 8;
        const READERS: usize = 8;
        const OPS_PER_WRITER: usize = 100_000;

        // Pre-generate allowed values per key
        let mut initial: HashMap<u64, Vec<u64>> = HashMap::new();
        for k in 0..KEY_SPACE {
            let count = (rand::random::<u8>() as usize % 8) + 1;
            let mut set = HashSet::new();
            while set.len() < count {
                set.insert(rand::random::<u64>());
            }
            initial.insert(k, set.into_iter().collect());
        }
        let initial = Arc::new(initial);

        let mut handles = Vec::with_capacity(WRITERS + READERS);

        // Writers
        for _ in 0..WRITERS {
            let c = Arc::clone(&cache);
            let initial = Arc::clone(&initial);
            handles.push(thread::spawn(move || {
                let mut rnd = rand::rng();
                for _ in 0..OPS_PER_WRITER {
                    use rand::Rng;
                    let key = rnd.random_range(0..KEY_SPACE);
                    let vec = &initial[&key];
                    let value = vec[rnd.random_range(0..vec.len())];
                    c.insert(key, value);
                    if (rnd.random::<u32>() & 0x3ff) == 0 {
                        thread::yield_now();
                    }
                }
            }));
        }

        // Readers
        for _ in 0..READERS {
            let c = Arc::clone(&cache);
            let initial = Arc::clone(&initial);
            handles.push(thread::spawn(move || {
                let mut rnd = rand::rng();
                let reads = (OPS_PER_WRITER * WRITERS) / (READERS * 2).max(1);
                for _ in 0..reads {
                    use rand::Rng;
                    let key = rnd.random_range(0..KEY_SPACE);
                    if let Some(v) = c.get(&key) {
                        let vec = &initial[&key];
                        assert!(
                            vec.contains(&v),
                            "cache returned unseen value {v} for key {key}",
                        );
                    }
                    if (rnd.random::<u8>() & 0x1f) == 0 {
                        thread::yield_now();
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Final verification
        for k in 0..KEY_SPACE {
            if let Some(v) = cache.get(&k) {
                let vec = &initial[&k];
                assert!(
                    vec.contains(&v),
                    "final check: returned {v} for key {k} which wasn't in the initial set",
                );
            }
        }
    }
}
