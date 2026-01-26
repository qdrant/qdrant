//! A concurrent S3-FIFO cache with sharded hashtable for reduced lock contention.
//!
//! This module provides an alternative thread-safe cache implementation where:
//! - Reads and writes use a sharded hashtable (128 shards by default)
//! - Writes use the LMAX Disruptor pattern for minimal latency
//!
//! This implementation uses hashbrown's HashTable with mutex-protected shards
//! instead of papaya's lock-free hashmap, allowing for benchmarking comparisons.

use std::cell::UnsafeCell;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use disruptor::{build_multi_producer, BusySpin, Producer};
use parking_lot::Mutex;

use crate::cache::GlobalOffset;
use crate::concurrent_ringbuffer::ConcurrentRingBuffer;
use crate::sharded_hashtable::ShardedHashTable;

/// A concurrent S3-FIFO cache using sharded hashtable and the Disruptor pattern.
///
/// This cache supports concurrent reads via `get()` which takes `&self`.
/// Inserts are performed via `insert()` which also takes `&self` and publishes
/// to a high-performance Disruptor ring buffer for processing by a dedicated thread.
///
/// # Example
///
/// ```ignore
/// use trififo::ConcurrentCacheSharded;
///
/// let cache = ConcurrentCacheSharded::<u64, String>::new(1000, 0.1, 0.9, Default::default());
///
/// // Insert (low-latency, publishes to disruptor)
/// cache.insert(1, "hello".to_string());
///
/// // Get (locks only one shard)
/// if let Some(value) = cache.get(&1) {
///     println!("Got: {}", value);
/// }
/// ```
pub struct ConcurrentCacheSharded<K, V, S = ahash::RandomState>
where
    K: Copy + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Clone + Default + Send + Sync + 'static,
{
    inner: Arc<CacheInner<K, V, S>>,
    /// Pool of producers for reduced contention on multi-threaded inserts.
    /// Each producer is protected by its own mutex, and threads round-robin
    /// across the pool to minimize contention.
    producer_pool: Arc<ProducerPool<K, V>>,
}

/// Pool of producers to reduce contention on multi-threaded inserts.
struct ProducerPool<K, V> {
    producers: Box<[Mutex<Box<dyn FnMut(K, V) + Send>>]>,
    /// Counter for round-robin selection of producers
    next_producer: AtomicUsize,
}

impl<K, V> ProducerPool<K, V> {
    fn new(producers: Vec<Box<dyn FnMut(K, V) + Send>>) -> Self {
        Self {
            producers: producers.into_iter().map(Mutex::new).collect(),
            next_producer: AtomicUsize::new(0),
        }
    }

    /// Publish using round-robin producer selection.
    /// This distributes load across producers to reduce contention.
    #[inline]
    fn publish(&self, key: K, value: V) {
        let pool_size = self.producers.len();
        // Use relaxed ordering - we don't need strict round-robin, just distribution
        let index = self.next_producer.fetch_add(1, Ordering::Relaxed) % pool_size;
        let mut producer = self.producers[index].lock();
        (producer)(key, value);
    }
}

/// Event published to the disruptor ring buffer.
/// Uses UnsafeCell to allow taking values out in the processor.
struct InsertEvent<K, V> {
    key: UnsafeCell<Option<K>>,
    value: UnsafeCell<Option<V>>,
}

// Safety: InsertEvent is only accessed by one thread at a time:
// - The producer thread writes to it
// - The consumer thread reads from it
// The disruptor guarantees these accesses don't overlap.
unsafe impl<K: Send, V: Send> Send for InsertEvent<K, V> {}
unsafe impl<K: Send, V: Send> Sync for InsertEvent<K, V> {}

impl<K, V> Default for InsertEvent<K, V> {
    fn default() -> Self {
        Self {
            key: UnsafeCell::new(None),
            value: UnsafeCell::new(None),
        }
    }
}

struct CacheInner<K, V, S> {
    /// Sharded hashtable mapping key -> global offset
    /// Uses 128 shards by default for minimal lock contention
    hashtable: ShardedHashTable<(K, GlobalOffset), S>,
    fifos: ConcurrentFifos<K, V>,
}

struct ConcurrentFifos<K, V> {
    small: ConcurrentRingBuffer<Entry<K, V>>,
    ghost: ConcurrentRingBuffer<K>,
    main: ConcurrentRingBuffer<Entry<K, V>>,

    small_end: GlobalOffset,
    ghost_end: GlobalOffset,
    #[allow(dead_code)]
    main_end: GlobalOffset,
}

struct Entry<K, V> {
    key: K,
    value: V,
    recency: AtomicU8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalOffset {
    Small(u32),
    Ghost(u32),
    Main(u32),
}

/// Number of shards for the hashtable (must be power of 2)
const DEFAULT_NUM_SHARDS: usize = 128;

// ============================================================================
// ConcurrentCacheSharded implementation
// ============================================================================

impl<K, V, S> ConcurrentCacheSharded<K, V, S>
where
    K: Copy + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Clone + Default + Send + Sync + 'static,
{
    /// Default number of producers in the pool.
    /// This provides a good balance between reduced contention and resource usage.
    const DEFAULT_PRODUCER_POOL_SIZE: usize = 4;

    /// Creates a new concurrent cache with sharded hashtable.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of entries (small + main queues)
    /// * `small_ratio` - Fraction of capacity for small queue (typically 0.1)
    /// * `ghost_ratio` - Fraction of capacity for ghost queue (typically 0.9)
    /// * `hasher` - Hash builder for key hashing
    ///
    /// # Panics
    /// Panics if capacity is 0.
    pub fn new(capacity: usize, small_ratio: f32, ghost_ratio: f32, hasher: S) -> Self {
        Self::with_disruptor_config(
            capacity,
            small_ratio,
            ghost_ratio,
            hasher,
            1024,
            Self::DEFAULT_PRODUCER_POOL_SIZE,
            DEFAULT_NUM_SHARDS,
        )
    }

    /// Creates a new concurrent cache with a custom disruptor ring buffer size.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of entries (small + main queues)
    /// * `small_ratio` - Fraction of capacity for small queue (typically 0.1)
    /// * `ghost_ratio` - Fraction of capacity for ghost queue (typically 0.9)
    /// * `hasher` - Hash builder for key hashing
    /// * `disruptor_size` - Size of the disruptor ring buffer (must be power of 2)
    ///
    /// # Panics
    /// Panics if capacity is 0 or disruptor_size is not a power of 2.
    pub fn with_disruptor_size(
        capacity: usize,
        small_ratio: f32,
        ghost_ratio: f32,
        hasher: S,
        disruptor_size: usize,
    ) -> Self {
        Self::with_disruptor_config(
            capacity,
            small_ratio,
            ghost_ratio,
            hasher,
            disruptor_size,
            Self::DEFAULT_PRODUCER_POOL_SIZE,
            DEFAULT_NUM_SHARDS,
        )
    }

    /// Creates a new concurrent cache with full configuration options.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of entries (small + main queues)
    /// * `small_ratio` - Fraction of capacity for small queue (typically 0.1)
    /// * `ghost_ratio` - Fraction of capacity for ghost queue (typically 0.9)
    /// * `hasher` - Hash builder for key hashing
    /// * `disruptor_size` - Size of the disruptor ring buffer (must be power of 2)
    /// * `producer_pool_size` - Number of producers in the pool (reduces contention)
    /// * `num_shards` - Number of hashtable shards (will be rounded up to power of 2)
    ///
    /// # Panics
    /// Panics if capacity is 0, disruptor_size is not a power of 2, or producer_pool_size is 0.
    pub fn with_disruptor_config(
        capacity: usize,
        small_ratio: f32,
        ghost_ratio: f32,
        hasher: S,
        disruptor_size: usize,
        producer_pool_size: usize,
        num_shards: usize,
    ) -> Self {
        assert!(capacity > 0);
        assert!(
            disruptor_size.is_power_of_two(),
            "disruptor_size must be a power of 2"
        );
        assert!(producer_pool_size > 0, "producer_pool_size must be > 0");

        let inner = Arc::new(CacheInner::new(
            capacity,
            small_ratio,
            ghost_ratio,
            hasher,
            num_shards,
        ));
        let processor_inner = Arc::clone(&inner);

        // Create the disruptor with a processor that handles inserts
        let factory = InsertEvent::default;
        let processor = move |event: &InsertEvent<K, V>, _sequence: i64, _end_of_batch: bool| {
            // Safety: The disruptor guarantees single-threaded access to each event.
            // We use UnsafeCell to take ownership of the values.
            let key = unsafe { (*event.key.get()).take() };
            let value = unsafe { (*event.value.get()).take() };

            if let (Some(key), Some(value)) = (key, value) {
                processor_inner.do_insert(key, value);
            }
        };

        let producer = build_multi_producer(disruptor_size, factory, BusySpin)
            .handle_events_with(processor)
            .build();

        // Create a pool of producers by cloning the MultiProducer
        // Each clone can publish independently, reducing contention
        let producers: Vec<Box<dyn FnMut(K, V) + Send>> = (0..producer_pool_size)
            .map(|_| {
                let mut producer_clone = producer.clone();
                let publish_fn: Box<dyn FnMut(K, V) + Send> = Box::new(move |key: K, value: V| {
                    producer_clone.publish(|event| {
                        // Safety: We have exclusive access during publish
                        unsafe {
                            *event.key.get() = Some(key);
                            *event.value.get() = Some(value);
                        }
                    });
                });
                publish_fn
            })
            .collect();

        Self {
            inner,
            producer_pool: Arc::new(ProducerPool::new(producers)),
        }
    }

    /// Retrieves a value from the cache.
    ///
    /// This operation acquires a lock on one shard of the hashtable.
    /// Operations on different shards can proceed concurrently.
    ///
    /// Returns `None` if the key is not found or if it's in the ghost queue (no value).
    #[inline]
    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// This operation publishes to the Disruptor ring buffer for low-latency
    /// processing by the dedicated writer thread.
    ///
    /// If the key already exists:
    /// - In ghost queue: promotes to main queue with the new value
    /// - In small/main queue: increments recency (value is NOT updated)
    ///
    /// This method is thread-safe and can be called concurrently from multiple threads.
    /// Uses a pool of producers with round-robin selection to minimize contention.
    #[inline]
    pub fn insert(&self, key: K, value: V) {
        self.producer_pool.publish(key, value);
    }

    /// Returns the number of entries in the cache.
    pub fn len(&self) -> usize {
        self.inner.hashtable.len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.hashtable.is_empty()
    }

    /// Returns the number of hashtable shards.
    pub fn num_shards(&self) -> usize {
        self.inner.hashtable.num_shards()
    }
}

// ============================================================================
// CacheInner implementation
// ============================================================================

impl<K, V, S> CacheInner<K, V, S>
where
    K: Copy + Hash + Eq + Send + Sync,
    V: Send + Sync,
    S: BuildHasher + Clone + Default + Send + Sync,
{
    fn new(
        capacity: usize,
        small_ratio: f32,
        ghost_ratio: f32,
        hasher: S,
        num_shards: usize,
    ) -> Self {
        let hashtable =
            ShardedHashTable::with_capacity_hasher_and_shards(capacity, hasher, num_shards);

        let small_size = (capacity as f32 * small_ratio) as usize;
        let small = ConcurrentRingBuffer::with_capacity(small_size.max(1));

        let ghost_size = (capacity as f32 * ghost_ratio) as usize;
        let ghost = ConcurrentRingBuffer::with_capacity(ghost_size.max(1));

        let main_size = capacity - small_size;
        let main = ConcurrentRingBuffer::with_capacity(main_size.max(1));

        let small_end = small_size as GlobalOffset;
        let ghost_end = small_end + ghost_size as GlobalOffset;
        let main_end = ghost_end + main_size as GlobalOffset;

        Self {
            hashtable,
            fifos: ConcurrentFifos {
                small,
                ghost,
                main,
                small_end,
                ghost_end,
                main_end,
            },
        }
    }

    /// Get operation using sharded hashtable.
    fn get(&self, key: &K) -> Option<&V> {
        let hash = self.hashtable.hash_one(key);

        // Lookup in sharded hashtable
        let global_offset = self.hashtable.find(hash, |(k, _)| k == key).map(|(_, o)| o)?;

        // Read from ring buffer
        let local_offset = self.fifos.local_offset(global_offset);
        let entry = self.fifos.get_entry_by_local_offset(local_offset)?;

        // Verify key still matches (handles race with eviction)
        if &entry.key != key {
            return None;
        }

        // Atomically increment recency
        entry.incr_recency();

        Some(&entry.value)
    }

    /// Performs the actual insert operation. Only called from writer thread.
    fn do_insert(&self, key: K, value: V) {
        let hash = self.hashtable.hash_one(&key);

        // Check if key already exists
        if let Some((_, global_offset)) = self.hashtable.find(hash, |(k, _)| *k == key) {
            let local_offset = self.fifos.local_offset(global_offset);
            self.promote_existing(local_offset, key, value);
            return;
        }

        // New key: insert to small queue
        let entry = Entry::new(key, value);
        let local_offset = self.push_to_small_queue(entry);
        let global_offset = self.fifos.global_offset(local_offset);

        self.hashtable.insert(
            hash,
            (key, global_offset),
            |(k, _)| *k == key,
            |(k, _)| self.hashtable.hash_one(k),
        );
    }

    /// Promotes an existing entry (from ghost) or increments recency (small/main).
    fn promote_existing(&self, local_offset: LocalOffset, key: K, value: V) {
        match local_offset {
            LocalOffset::Ghost(offset) => {
                // Verify key still matches
                let ghost_key = self.fifos.ghost.get_absolute_unchecked(offset as usize);
                if ghost_key != &key {
                    debug_assert!(false, "Key mismatch in ghost promotion");
                    return;
                }

                // CoW: Insert to main first, then update hashtable
                let entry = Entry::new(key, value);
                let new_local_offset = self.push_to_main_queue(entry);
                let new_global_offset = self.fifos.global_offset(new_local_offset);

                let hash = self.hashtable.hash_one(&key);
                self.hashtable.insert(
                    hash,
                    (key, new_global_offset),
                    |(k, _)| *k == key,
                    |(k, _)| self.hashtable.hash_one(k),
                );
            }
            LocalOffset::Small(offset) => {
                let entry = self.fifos.small.get_absolute_unchecked(offset as usize);
                entry.incr_recency();
            }
            LocalOffset::Main(offset) => {
                let entry = self.fifos.main.get_absolute_unchecked(offset as usize);
                entry.incr_recency();
            }
        }
    }

    /// Pushes to main queue, handling eviction with recency-based reinsertion.
    #[must_use]
    fn push_to_main_queue(&self, entry: Entry<K, V>) -> LocalOffset {
        // Try to push if not full
        let entry = match self.fifos.main.try_push(entry) {
            Ok(local_offset) => return LocalOffset::Main(local_offset as u32),
            Err(entry) => entry,
        };

        // Reinsert entries with non-zero recency
        while self.fifos.main.reinsert_unchecked_if(|e| {
            if e.recency.load(Ordering::Relaxed) > 0 {
                e.decr_recency();
                true
            } else {
                false
            }
        }) {}

        // Now evict the oldest entry (recency == 0)
        let (evicted_entry, local_offset) = self.fifos.main.pop_push_unchecked(entry);

        // Remove evicted entry from hashtable
        let hash = self.hashtable.hash_one(&evicted_entry.key);
        self.hashtable.remove(hash, |(k, _)| *k == evicted_entry.key);

        LocalOffset::Main(local_offset as u32)
    }

    /// Pushes to small queue, promoting to main or demoting to ghost as needed.
    #[must_use]
    fn push_to_small_queue(&self, entry: Entry<K, V>) -> LocalOffset {
        if self.fifos.small.is_full() {
            let (oldest_entry, small_offset) = self.fifos.small.pop_push_unchecked(entry);

            if oldest_entry.recency() > 0 {
                // CoW: Insert to main first, then update hashtable
                let oldest_key = oldest_entry.key;
                let new_local_offset = self.push_to_main_queue(oldest_entry);
                let new_global_offset = self.fifos.global_offset(new_local_offset);

                let hash = self.hashtable.hash_one(&oldest_key);
                self.hashtable.insert(
                    hash,
                    (oldest_key, new_global_offset),
                    |(k, _)| *k == oldest_key,
                    |(k, _)| self.hashtable.hash_one(k),
                );
            } else {
                // Demote to ghost queue
                let ghost_offset = self.fifos.ghost.overwriting_push(oldest_entry.key);
                let ghost_global_offset =
                    self.fifos.global_offset(LocalOffset::Ghost(ghost_offset as u32));

                let hash = self.hashtable.hash_one(&oldest_entry.key);
                self.hashtable.insert(
                    hash,
                    (oldest_entry.key, ghost_global_offset),
                    |(k, _)| *k == oldest_entry.key,
                    |(k, _)| self.hashtable.hash_one(k),
                );
            }

            return LocalOffset::Small(small_offset as u32);
        }

        let local_offset = self.fifos.small.overwriting_push(entry);
        LocalOffset::Small(local_offset as u32)
    }
}

// ============================================================================
// ConcurrentFifos implementation
// ============================================================================

impl<K, V> ConcurrentFifos<K, V>
where
    K: Eq,
{
    #[inline]
    fn local_offset(&self, global_offset: GlobalOffset) -> LocalOffset {
        if global_offset < self.small_end {
            LocalOffset::Small(global_offset)
        } else if global_offset < self.ghost_end {
            LocalOffset::Ghost(global_offset - self.small_end)
        } else {
            LocalOffset::Main(global_offset - self.ghost_end)
        }
    }

    #[inline]
    fn global_offset(&self, local_offset: LocalOffset) -> GlobalOffset {
        match local_offset {
            LocalOffset::Small(offset) => offset,
            LocalOffset::Ghost(offset) => offset + self.small_end,
            LocalOffset::Main(offset) => offset + self.ghost_end,
        }
    }

    #[inline]
    fn get_entry_by_local_offset(&self, local_offset: LocalOffset) -> Option<&Entry<K, V>> {
        match local_offset {
            LocalOffset::Small(offset) => Some(self.small.get_absolute_unchecked(offset as usize)),
            LocalOffset::Ghost(_) => None,
            LocalOffset::Main(offset) => Some(self.main.get_absolute_unchecked(offset as usize)),
        }
    }
}

// ============================================================================
// Entry implementation
// ============================================================================

impl<K, V> Entry<K, V> {
    fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            recency: AtomicU8::new(0),
        }
    }

    fn recency(&self) -> u8 {
        self.recency.load(Ordering::Relaxed)
    }

    fn incr_recency(&self) {
        let _ = self.recency.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            if current < 4 {
                Some(current + 1)
            } else {
                None
            }
        });
    }

    fn decr_recency(&self) {
        let _ = self.recency.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            if current > 0 {
                Some(current - 1)
            } else {
                None
            }
        });
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_basic_insert_and_get() {
        let cache: ConcurrentCacheSharded<u64, String> =
            ConcurrentCacheSharded::new(1000, 0.1, 0.9, Default::default());

        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());
        cache.insert(3, "three".to_string());

        // Give the disruptor time to process
        thread::sleep(Duration::from_millis(10));

        assert_eq!(cache.get(&1), Some(&"one".to_string()));
        assert_eq!(cache.get(&2), Some(&"two".to_string()));
        assert_eq!(cache.get(&3), Some(&"three".to_string()));
        assert_eq!(cache.get(&4), None);
    }

    #[test]
    fn test_concurrent_reads() {
        let cache: Arc<ConcurrentCacheSharded<u64, u64>> =
            Arc::new(ConcurrentCacheSharded::new(10000, 0.1, 0.9, Default::default()));

        // Pre-populate
        for i in 0..100 {
            cache.insert(i, i * 2);
        }

        // Give the disruptor time to process
        thread::sleep(Duration::from_millis(50));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    for i in 0..100 {
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
    fn test_concurrent_insert_and_read() {
        let cache: Arc<ConcurrentCacheSharded<u64, u64>> =
            Arc::new(ConcurrentCacheSharded::new(10000, 0.1, 0.9, Default::default()));

        let cache_writer = Arc::clone(&cache);
        let writer = thread::spawn(move || {
            for i in 0..1000 {
                cache_writer.insert(i, i * 2);
            }
        });

        let cache_reader = Arc::clone(&cache);
        let reader = thread::spawn(move || {
            for _ in 0..100 {
                for i in 0..1000 {
                    let _ = cache_reader.get(&i);
                }
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn test_len_and_is_empty() {
        let cache: ConcurrentCacheSharded<u64, String> =
            ConcurrentCacheSharded::new(1000, 0.1, 0.9, Default::default());

        assert!(cache.is_empty());

        cache.insert(1, "one".to_string());
        thread::sleep(Duration::from_millis(10));

        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_custom_disruptor_size() {
        let cache: ConcurrentCacheSharded<u64, u64> =
            ConcurrentCacheSharded::with_disruptor_size(1000, 0.1, 0.9, Default::default(), 256);

        for i in 0..100 {
            cache.insert(i, i * 2);
        }

        thread::sleep(Duration::from_millis(50));

        for i in 0..100 {
            assert_eq!(cache.get(&i), Some(&(i * 2)));
        }
    }

    #[test]
    fn test_num_shards() {
        let cache: ConcurrentCacheSharded<u64, u64> =
            ConcurrentCacheSharded::new(1000, 0.1, 0.9, Default::default());

        // Default is 128 shards
        assert_eq!(cache.num_shards(), 128);

        // Custom shards
        let cache2: ConcurrentCacheSharded<u64, u64> =
            ConcurrentCacheSharded::with_disruptor_config(1000, 0.1, 0.9, Default::default(), 1024, 4, 64);

        assert_eq!(cache2.num_shards(), 64);
    }

    #[test]
    fn test_multi_threaded_inserts() {
        let cache: Arc<ConcurrentCacheSharded<u64, u64>> =
            Arc::new(ConcurrentCacheSharded::new(50000, 0.1, 0.9, Default::default()));

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

        // Give the disruptor time to process all inserts
        thread::sleep(Duration::from_millis(100));

        // Verify most values were inserted
        let mut found = 0;
        for t in 0..THREADS {
            let start = t as u64 * INSERTS_PER_THREAD;
            for i in start..(start + INSERTS_PER_THREAD) {
                if cache.get(&i) == Some(&(i * 2)) {
                    found += 1;
                }
            }
        }

        // Most values should be found (some might be evicted due to capacity)
        let total = THREADS as u64 * INSERTS_PER_THREAD;
        assert!(
            found as f64 / total as f64 > 0.9,
            "Expected at least 90% of values, got {}/{}",
            found,
            total
        );
    }

    #[test]
    fn test_high_contention_inserts() {
        let cache: Arc<ConcurrentCacheSharded<u64, u64>> =
            Arc::new(ConcurrentCacheSharded::new(10000, 0.1, 0.9, Default::default()));

        const THREADS: usize = 8;
        const INSERTS_PER_THREAD: u64 = 500;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    // All threads insert to the same key range for maximum contention
                    for i in 0..INSERTS_PER_THREAD {
                        cache.insert(i, i * 2);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Give the disruptor time to process
        thread::sleep(Duration::from_millis(100));

        // Verify values - all keys should be present with correct values
        let mut found = 0;
        for i in 0..INSERTS_PER_THREAD {
            if cache.get(&i) == Some(&(i * 2)) {
                found += 1;
            }
        }

        // All unique keys should be present
        assert!(
            found as f64 / INSERTS_PER_THREAD as f64 > 0.9,
            "Expected at least 90% of values, got {}/{}",
            found,
            INSERTS_PER_THREAD
        );
    }
}