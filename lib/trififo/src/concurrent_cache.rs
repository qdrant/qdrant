//! A concurrent S3-FIFO cache with lock-free reads and low-latency inserts.
//!
//! This module provides a thread-safe cache implementation where:
//! - Reads are lock-free and can happen concurrently
//! - Writes use the LMAX Disruptor pattern for minimal latency
//!
//! The key insight enabling this design is that reads only:
//! 1. Look up an offset in a lock-free concurrent hashtable (papaya)
//! 2. Read the entry at that offset from the ring buffer
//! 3. Verify the key matches (to detect races with eviction)
//! 4. Increment recency atomically
//!
//! With Copy-on-Write style moves (insert at destination, update hashtable, then
//! the old entry naturally becomes orphaned), entries remain valid at their old
//! location until overwritten by new insertions.

use std::cell::UnsafeCell;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use disruptor::{BusySpin, Producer, build_multi_producer};
use parking_lot::Mutex;

use crate::cache::GlobalOffset;
use crate::concurrent_fifos::{Entry, FifosReader, FifosWriter, LocalOffset, new_fifos};

/// A concurrent S3-FIFO cache using the Disruptor pattern.
///
/// This cache supports lock-free reads via `get()` which takes `&self`.
/// Inserts are performed via `insert()` which also takes `&self` and publishes
/// to a high-performance Disruptor ring buffer for processing by a dedicated thread.
///
/// # Example
///
/// ```ignore
/// use trififo::ConcurrentCache;
///
/// let cache = ConcurrentCache::<u64, String>::new(1000, 0.1, 0.9, Default::default());
///
/// // Insert (low-latency, publishes to disruptor)
/// cache.insert(1, "hello".to_string());
///
/// // Get (lock-free read)
/// if let Some(value) = cache.get(&1) {
///     println!("Got: {}", value);
/// }
/// ```
pub struct ConcurrentCache<K, V, S = ahash::RandomState> {
    /// Shared state for lock-free reads
    reader: Arc<CacheReader<K, V, S>>,
    /// Pool of producers for receiving inserts from multiple threads, but applying
    /// them on a single one.
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

/// Shared reader state - contains only read-side handles.
/// This is `Send + Sync` and can be shared via `Arc`.
struct CacheReader<K, V, S> {
    /// Lock-free concurrent hashtable mapping key -> global offset
    hashtable: Arc<papaya::HashMap<K, GlobalOffset, S>>,
    /// Reader handles for the FIFO queues
    fifos: FifosReader<K, V>,
}

/// Writer state - contains only write-side handles.
/// This is `Send` but NOT `Sync` (due to RingBufferWriter being !Sync).
/// Owned exclusively by the disruptor consumer thread.
struct CacheWriter<K, V, S> {
    /// Lock-free concurrent hashtable (shared with readers)
    hashtable: Arc<papaya::HashMap<K, GlobalOffset, S>>,
    /// Writer handles for the FIFO queues
    fifos: FifosWriter<K, V>,
}

// ============================================================================
// ConcurrentCache implementation
// ============================================================================

impl<K, V, S> ConcurrentCache<K, V, S>
where
    K: Copy + Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Default + Send + Sync + 'static,
{
    /// Default number of producers in the pool.
    /// This provides a good balance between reduced contention and resource usage.
    const DEFAULT_PRODUCER_POOL_SIZE: usize = 4;

    /// Creates a new concurrent cache.
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
    ) -> Self {
        assert!(capacity > 0);
        assert!(
            disruptor_size.is_power_of_two(),
            "disruptor_size must be a power of 2"
        );
        assert!(producer_pool_size > 0, "producer_pool_size must be > 0");

        // Create the FIFO queues with separate reader/writer handles
        let (fifos_writer, fifos_reader) = new_fifos(capacity, small_ratio, ghost_ratio);

        // Create shared hashtable
        let hashtable = Arc::new(papaya::HashMap::with_capacity_and_hasher(capacity, hasher));

        // Create reader state (shared across threads)
        let reader = Arc::new(CacheReader {
            hashtable: Arc::clone(&hashtable),
            fifos: fifos_reader.clone(),
        });

        // Create writer state (owned by disruptor consumer thread)
        let mut cache_writer = CacheWriter {
            hashtable,
            fifos: fifos_writer,
        };

        // Create the disruptor with a processor that handles inserts
        let factory = InsertEvent::default;
        let processor = move |event: &InsertEvent<K, V>, _sequence: i64, _end_of_batch: bool| {
            // Safety: The disruptor guarantees single-threaded access to each event.
            // We use UnsafeCell to take ownership of the values.
            let key = unsafe { (*event.key.get()).take() };
            let value = unsafe { (*event.value.get()).take() };

            if let (Some(key), Some(value)) = (key, value) {
                cache_writer.do_insert(key, value);
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
            reader,
            producer_pool: Arc::new(ProducerPool::new(producers)),
        }
    }

    /// Retrieves a value from the cache.
    ///
    /// This operation is lock-free and can be called concurrently from multiple threads.
    /// If the entry exists, its recency is incremented atomically.
    ///
    /// Returns `None` if the key is not found or if it's in the ghost queue (no value).
    #[inline]
    pub fn get(&self, key: &K) -> Option<&V> {
        // Lock-free lookup using papaya's guard
        let guard = self.reader.hashtable.guard();
        let global_offset = *self.reader.hashtable.get(key, &guard)?;
        drop(guard); // Release the guard - we have the offset now

        // Read from ring buffer (lock-free, no guards needed)
        let local_offset = self.reader.fifos.local_offset(global_offset);
        let entry = self.reader.fifos.get_entry(local_offset)?;

        // Verify key still matches (handles race with eviction)
        // If the entry was evicted and the slot reused, the key won't match
        if &entry.key != key {
            return None;
        }

        // Atomically increment recency
        entry.incr_recency();

        Some(&entry.value)
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
        self.reader.hashtable.pin().len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<K, V, S> CacheWriter<K, V, S>
where
    K: Copy + Hash + Eq + Send + Sync,
    V: Clone + Send + Sync,
    S: BuildHasher + Clone + Default + Send + Sync,
{
    /// Updates the hashtable by inserting a key with its corresponding global offset,
    /// or removes the key if `local_offset` is `None`.
    fn update_hashtable(&self, key: K, local_offset: Option<LocalOffset>) {
        let guard = self.hashtable.guard();
        match local_offset {
            Some(offset) => {
                let global_offset = self.fifos.global_offset(offset);
                self.hashtable.insert(key, global_offset, &guard);
            }
            None => {
                self.hashtable.remove(&key, &guard);
            }
        }
    }

    /// Performs the actual insert operation. Only called from writer thread.
    fn do_insert(&mut self, key: K, value: V) {
        // Check if key already exists - get the offset first, then drop the borrow
        let existing_offset = {
            let guard = self.hashtable.guard();
            self.hashtable.get(&key, &guard).copied()
        };

        if let Some(global_offset) = existing_offset {
            let local_offset = self.fifos.local_offset(global_offset);
            self.promote_existing(local_offset, key, value);
            return;
        }

        // New key: insert to small queue
        let entry = Entry::new(key, value);
        let local_offset = self.push_to_small_queue(entry);
        self.update_hashtable(key, Some(local_offset));
    }

    /// Promotes an existing entry (from ghost) or increments recency (small/main).
    fn promote_existing(&mut self, local_offset: LocalOffset, key: K, value: V) {
        match local_offset {
            LocalOffset::Ghost(offset) => {
                // Verify key still matches
                let ghost_key = self.fifos.get_ghost_key(offset);
                if ghost_key != &key {
                    debug_assert!(false, "Key mismatch in ghost promotion");
                    return;
                }

                // CoW: Insert to main first, then update hashtable
                // Old ghost entry remains until overwritten (harmless)
                let entry = Entry::new(key, value);
                let new_local_offset = self.push_to_main_queue(entry);
                self.update_hashtable(key, Some(new_local_offset));
            }
            LocalOffset::Small(offset) => {
                let entry = self.fifos.get_small_entry(offset);
                entry.incr_recency();
            }
            LocalOffset::Main(offset) => {
                let entry = self.fifos.get_main_entry(offset);
                entry.incr_recency();
            }
        }
    }

    /// Pushes to main queue, handling eviction with recency-based reinsertion.
    #[must_use]
    fn push_to_main_queue(&mut self, entry: Entry<K, V>) -> LocalOffset {
        // Try to push if not full
        let entry = match self.fifos.main.try_push(entry) {
            Ok(local_offset) => return LocalOffset::Main(local_offset as u32),
            Err(entry) => entry,
        };

        // Reinsert entries with non-zero recency
        while self.fifos.main.reinsert_unchecked_if(|e| {
            if e.recency() > 0 {
                e.decr_recency();
                true
            } else {
                false
            }
        }) {}

        // Safe eviction sequence to ensure readers always see valid data:
        // 1. Take the entry about to be evicted (leaves slot uninitialized)
        let to_evict = self.fifos.main.peek_oldest_unchecked();

        // 2. Remove from hashtable so readers can't find it anymore
        self.update_hashtable(to_evict.key, None);

        // 3. Actually write the new entry at the evicted slot
        let offset = self.fifos.main.overwriting_push(entry);

        LocalOffset::Main(offset as u32)
    }

    /// Pushes to small queue, promoting to main or demoting to ghost as needed.
    #[must_use]
    fn push_to_small_queue(&mut self, entry: Entry<K, V>) -> LocalOffset {
        let local_offset = match self.fifos.small.try_push(entry) {
            Ok(offset) => offset,
            Err(entry) => {
                // Safe eviction sequence to ensure readers always see valid data:
                // 1. Peek the entry from the end of the queue
                let oldest_entry = self.fifos.small.peek_oldest_unchecked();
                let oldest_key = oldest_entry.key;

                // 2. Add the evicted entry to its new location (main or ghost)
                if oldest_entry.recency() > 0 {
                    // Promote to main queue with the full entry we peeked
                    let new_local_offset = self.push_to_main_queue(oldest_entry.clone());
                    self.update_hashtable(oldest_key, Some(new_local_offset));
                } else {
                    // Demote to ghost queue (only stores key)
                    let ghost_offset = self.fifos.ghost.overwriting_push(oldest_key);
                    self.update_hashtable(oldest_key, Some(LocalOffset::Ghost(ghost_offset as u32)));
                }

                // 3. Remove from hashtable so readers can't find it anymore
                self.update_hashtable(oldest_key, None);

                // 4. Actually write the new entry at the evicted slot
                let small_offset = self.fifos.small.overwriting_push(entry);

                return LocalOffset::Small(small_offset as u32);
            }
        };

        LocalOffset::Small(local_offset as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_basic_insert_and_get() {
        let cache = ConcurrentCache::<u64, String>::new(100, 0.1, 0.9, Default::default());

        cache.insert(1, "hello".to_string());

        // Give the disruptor time to process
        thread::sleep(Duration::from_millis(10));

        let value = cache.get(&1);
        assert_eq!(value.map(|s| s.as_str()), Some("hello"));
    }

    #[test]
    fn test_concurrent_reads() {
        let cache = ConcurrentCache::<u64, String>::new(100, 0.1, 0.9, Default::default());

        // Insert some values
        for i in 0..10 {
            cache.insert(i, format!("value_{}", i));
        }

        // Give time for inserts to process
        thread::sleep(Duration::from_millis(50));

        // Spawn multiple reader threads
        let cache = Arc::new(cache);
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    for _ in 0..100 {
                        for i in 0..10 {
                            let _ = cache.get(&i);
                        }
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
        let cache = Arc::new(ConcurrentCache::<u64, u64>::new(
            1000,
            0.1,
            0.9,
            Default::default(),
        ));

        let cache_writer = Arc::clone(&cache);
        let cache_reader = Arc::clone(&cache);

        let writer = thread::spawn(move || {
            for i in 0..100 {
                cache_writer.insert(i, i * 10);
            }
        });

        let reader = thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            for _ in 0..100 {
                for i in 0..100 {
                    let _ = cache_reader.get(&i);
                }
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn test_len_and_is_empty() {
        let cache = ConcurrentCache::<u64, String>::new(100, 0.1, 0.9, Default::default());

        assert!(cache.is_empty());

        cache.insert(1, "hello".to_string());
        thread::sleep(Duration::from_millis(10));

        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_custom_disruptor_size() {
        // Use larger capacity to ensure all entries fit
        let cache = ConcurrentCache::<u64, String>::with_disruptor_size(
            1000,
            0.1,
            0.9,
            Default::default(),
            256,
        );

        for i in 0..50 {
            cache.insert(i, format!("value_{}", i));
        }

        thread::sleep(Duration::from_millis(100));

        // Verify entries are found (some may be in ghost queue and return None,
        // but most should be accessible)
        let mut found = 0;
        for i in 0..50 {
            if cache.get(&i).is_some() {
                found += 1;
            }
        }
        assert!(found > 0, "Should have found at least some entries");
    }

    #[test]
    fn test_multi_threaded_inserts() {
        let cache = Arc::new(ConcurrentCache::<u64, u64>::new(
            10000,
            0.1,
            0.9,
            Default::default(),
        ));

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

        // Give time for all inserts to process
        thread::sleep(Duration::from_millis(100));

        // Verify some entries (not all may be present due to cache size)
        let mut found = 0;
        for i in 0..(THREADS as u64 * INSERTS_PER_THREAD) {
            if cache.get(&i).is_some() {
                found += 1;
            }
        }

        // Should have found at least some entries
        assert!(found > 0, "Should have found some entries");
    }

    #[test]
    fn test_high_contention_inserts() {
        let cache = Arc::new(ConcurrentCache::<u64, u64>::new(
            1000,
            0.1,
            0.9,
            Default::default(),
        ));
        let counter = Arc::new(AtomicUsize::new(0));

        const THREADS: usize = 8;
        const INSERTS_PER_THREAD: usize = 500;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let counter = Arc::clone(&counter);
                thread::spawn(move || {
                    for _ in 0..INSERTS_PER_THREAD {
                        let key = counter.fetch_add(1, Ordering::Relaxed) as u64;
                        cache.insert(key, key);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Give time for processing
        thread::sleep(Duration::from_millis(100));

        assert!(cache.len() > 0);
    }
}
