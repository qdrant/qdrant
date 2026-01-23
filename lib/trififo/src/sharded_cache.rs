//! A sharded S3-FIFO cache for concurrent access.
//!
//! This module provides a thread-safe cache implementation by sharding the data
//! across multiple independent `Cache` instances, each protected by a `Mutex`.
//!
//! The key benefits of this design:
//! - Operations on different shards don't block each other
//! - Simple implementation that reuses the existing `Cache` logic
//! - Predictable performance with configurable number of shards
//! - parking_lot's Mutex is fast and doesn't suffer from lock poisoning

use std::hash::{BuildHasher, Hash};

use parking_lot::Mutex;

use crate::cache::Cache;

/// A sharded S3-FIFO cache for concurrent access.
///
/// This cache distributes keys across multiple shards based on their hash,
/// allowing concurrent access to different shards. Each shard is an independent
/// `Cache` instance protected by a `Mutex`.
///
/// # Example
///
/// ```ignore
/// use trififo::ShardedCache;
///
/// let cache = ShardedCache::<u64, String>::new(1000, 0.1, 0.9, Default::default());
///
/// // Insert (locks only one shard)
/// cache.insert(1, "hello".to_string());
///
/// // Get (locks only one shard)
/// if let Some(value) = cache.get(&1) {
///     println!("Got: {}", value);
/// }
/// ```
pub struct ShardedCache<K, V, S = ahash::RandomState> {
    shards: Box<[Mutex<Cache<K, V, S>>]>,
    shard_mask: usize,
    hasher: S,
}

impl<K, V, S> ShardedCache<K, V, S>
where
    K: Copy + Hash + PartialEq,
    S: BuildHasher + Clone + Default,
{
    /// Default number of shards (must be a power of 2).
    /// 4 shards provides a reasonable balance between concurrency and ensuring
    /// each shard has meaningful capacity.
    const DEFAULT_NUM_SHARDS: usize = 4;

    /// Creates a new sharded cache with default number of shards.
    ///
    /// # Arguments
    /// * `capacity` - Total maximum number of entries across all shards
    /// * `small_ratio` - Fraction of capacity for small queue (typically 0.1)
    /// * `ghost_ratio` - Fraction of capacity for ghost queue (typically 0.9)
    /// * `hasher` - Hash builder for key hashing
    ///
    /// # Panics
    /// Panics if capacity is 0.
    pub fn new(capacity: usize, small_ratio: f32, ghost_ratio: f32, hasher: S) -> Self {
        Self::with_num_shards(
            capacity,
            small_ratio,
            ghost_ratio,
            hasher,
            Self::DEFAULT_NUM_SHARDS,
        )
    }

    /// Creates a new sharded cache with a custom number of shards.
    ///
    /// # Arguments
    /// * `capacity` - Total maximum number of entries across all shards
    /// * `small_ratio` - Fraction of capacity for small queue (typically 0.1)
    /// * `ghost_ratio` - Fraction of capacity for ghost queue (typically 0.9)
    /// * `hasher` - Hash builder for key hashing
    /// * `num_shards` - Number of shards (will be rounded up to next power of 2)
    ///
    /// # Panics
    /// Panics if capacity is 0 or num_shards is 0.
    pub fn with_num_shards(
        capacity: usize,
        small_ratio: f32,
        ghost_ratio: f32,
        hasher: S,
        num_shards: usize,
    ) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
        assert!(num_shards > 0, "num_shards must be > 0");

        // Round up to next power of 2 for efficient masking
        let num_shards = num_shards.next_power_of_two();
        let shard_mask = num_shards - 1;

        // Distribute capacity across shards
        // Each shard needs at least 10 entries for S3-FIFO to work properly
        // (small queue needs at least 1 entry at 10% ratio)
        let min_capacity_per_shard = 10;
        let capacity_per_shard = (capacity / num_shards).max(min_capacity_per_shard);

        let shards: Vec<_> = (0..num_shards)
            .map(|_| Mutex::new(Cache::new(capacity_per_shard, small_ratio, ghost_ratio, hasher.clone())))
            .collect();

        Self {
            shards: shards.into_boxed_slice(),
            shard_mask,
            hasher,
        }
    }

    /// Returns the number of shards.
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// Computes the shard index for a given key.
    #[inline]
    fn shard_index(&self, key: &K) -> usize {
        let hash = self.hasher.hash_one(key) as usize;
        // Use upper bits for shard selection (often better distributed than lower bits)
        (hash >> 7) & self.shard_mask
    }

    /// Retrieves a value from the cache.
    ///
    /// This operation acquires a lock on the appropriate shard.
    /// Operations on different shards can proceed concurrently.
    ///
    /// Returns `None` if the key is not found.
    #[inline]
    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let shard_idx = self.shard_index(key);
        let shard = self.shards[shard_idx].lock();
        shard.get(key).cloned()
    }

    /// Retrieves a value from the cache, applying a function to it.
    ///
    /// This is more efficient than `get` when you don't need to clone the value.
    ///
    /// Returns `None` if the key is not found.
    #[inline]
    pub fn get_with<F, R>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        let shard_idx = self.shard_index(key);
        let shard = self.shards[shard_idx].lock();
        shard.get(key).map(f)
    }

    /// Checks if a key exists in the cache.
    ///
    /// Note: This also increments the recency counter if the key exists.
    #[inline]
    pub fn contains(&self, key: &K) -> bool {
        let shard_idx = self.shard_index(key);
        let shard = self.shards[shard_idx].lock();
        shard.get(key).is_some()
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// This operation acquires a lock on the appropriate shard.
    /// Operations on different shards can proceed concurrently.
    ///
    /// If the key already exists:
    /// - In ghost queue: promotes to main queue with the new value
    /// - In small/main queue: increments recency (value is NOT updated)
    #[inline]
    pub fn insert(&self, key: K, value: V) {
        let shard_idx = self.shard_index(&key);
        let mut shard = self.shards[shard_idx].lock();
        shard.insert(key, value);
    }

    /// Returns the total number of entries across all shards.
    ///
    /// Note: This acquires locks on all shards sequentially,
    /// so the count may not be perfectly consistent in concurrent scenarios.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().len()).sum()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|shard| shard.lock().is_empty())
    }

    /// Returns the number of entries in each shard.
    ///
    /// Useful for debugging and monitoring shard distribution.
    pub fn shard_sizes(&self) -> Vec<usize> {
        self.shards.iter().map(|shard| shard.lock().len()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_and_get() {
        let cache: ShardedCache<u64, String> = ShardedCache::new(1000, 0.1, 0.9, Default::default());

        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());
        cache.insert(3, "three".to_string());

        assert_eq!(cache.get(&1), Some("one".to_string()));
        assert_eq!(cache.get(&2), Some("two".to_string()));
        assert_eq!(cache.get(&3), Some("three".to_string()));
        assert_eq!(cache.get(&4), None);
    }

    #[test]
    fn test_get_with() {
        let cache: ShardedCache<u64, String> = ShardedCache::new(1000, 0.1, 0.9, Default::default());

        cache.insert(1, "hello".to_string());

        let len = cache.get_with(&1, |v| v.len());
        assert_eq!(len, Some(5));

        let missing = cache.get_with(&2, |v| v.len());
        assert_eq!(missing, None);
    }

    #[test]
    fn test_contains() {
        let cache: ShardedCache<u64, String> = ShardedCache::new(1000, 0.1, 0.9, Default::default());

        cache.insert(1, "one".to_string());

        assert!(cache.contains(&1));
        assert!(!cache.contains(&2));
    }

    #[test]
    fn test_len_and_is_empty() {
        let cache: ShardedCache<u64, String> = ShardedCache::new(1000, 0.1, 0.9, Default::default());

        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);

        cache.insert(1, "one".to_string());
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);

        cache.insert(2, "two".to_string());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_custom_num_shards() {
        let cache: ShardedCache<u64, String> =
            ShardedCache::with_num_shards(1000, 0.1, 0.9, Default::default(), 8);

        assert_eq!(cache.num_shards(), 8);

        // Non-power-of-2 should be rounded up
        let cache2: ShardedCache<u64, String> =
            ShardedCache::with_num_shards(1000, 0.1, 0.9, Default::default(), 5);

        assert_eq!(cache2.num_shards(), 8); // 5 rounded up to 8
    }

    #[test]
    fn test_shard_distribution() {
        let cache: ShardedCache<u64, String> =
            ShardedCache::with_num_shards(1000, 0.1, 0.9, Default::default(), 4);

        // Insert many keys
        for i in 0..100 {
            cache.insert(i, format!("value_{}", i));
        }

        let sizes = cache.shard_sizes();
        assert_eq!(sizes.len(), 4);

        // All entries should be distributed (note: total may differ slightly
        // due to S3-FIFO eviction behavior within shards)
        let total: usize = sizes.iter().sum();
        assert!(total >= 90 && total <= 110, "Total entries should be around 100, got {}", total);

        // Check that distribution isn't too skewed (all in one shard)
        // With random hash distribution, each shard should have at least some entries
        let non_empty_shards = sizes.iter().filter(|&&s| s > 0).count();
        assert!(non_empty_shards >= 2, "At least 2 shards should have entries");
    }

    #[test]
    fn test_concurrent_reads() {
        use std::sync::Arc;
        use std::thread;

        let cache: Arc<ShardedCache<u64, String>> =
            Arc::new(ShardedCache::new(10000, 0.1, 0.9, Default::default()));

        // Pre-populate with enough capacity to hold all values
        for i in 0..100 {
            cache.insert(i, format!("value_{}", i));
        }

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    for i in 0..100 {
                        // Values may be evicted, so just check we can read without panic
                        let _ = cache.get(&i);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_inserts() {
        use std::sync::Arc;
        use std::thread;

        let cache: Arc<ShardedCache<u64, u64>> =
            Arc::new(ShardedCache::new(50000, 0.1, 0.9, Default::default()));

        const THREADS: usize = 4;
        const INSERTS_PER_THREAD: u64 = 1000;

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    let start = t as u64 * INSERTS_PER_THREAD;
                    for i in start..(start + INSERTS_PER_THREAD) {
                        cache.insert(i, i * 2);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify cache is not empty and has reasonable size
        assert!(!cache.is_empty());
        // With enough capacity, most values should be retained
        let len = cache.len();
        assert!(len > 3000, "Expected most values to be retained, got {}", len);
    }

    #[test]
    fn test_concurrent_read_write() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::thread;

        let cache: Arc<ShardedCache<u64, u64>> =
            Arc::new(ShardedCache::new(1000, 0.1, 0.9, Default::default()));
        let done = Arc::new(AtomicBool::new(false));

        // Writer thread
        let cache_writer = Arc::clone(&cache);
        let done_writer = Arc::clone(&done);
        let writer = thread::spawn(move || {
            for i in 0..1000 {
                cache_writer.insert(i % 100, i);
            }
            done_writer.store(true, Ordering::Release);
        });

        // Reader threads
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let done = Arc::clone(&done);
                thread::spawn(move || {
                    let mut reads = 0;
                    while !done.load(Ordering::Acquire) || reads < 100 {
                        for i in 0..100 {
                            let _ = cache.get(&i);
                            reads += 1;
                        }
                    }
                })
            })
            .collect();

        writer.join().unwrap();
        for reader in readers {
            reader.join().unwrap();
        }
    }

    #[test]
    fn test_different_value_types() {
        // Note: Keys must implement Copy due to underlying Cache requirement
        let cache: ShardedCache<u64, Vec<i32>> =
            ShardedCache::new(1000, 0.1, 0.9, Default::default());

        cache.insert(1, vec![1, 2, 3]);
        cache.insert(2, vec![4, 5, 6]);

        assert_eq!(cache.get(&1), Some(vec![1, 2, 3]));
        assert_eq!(cache.get(&2), Some(vec![4, 5, 6]));
    }
}