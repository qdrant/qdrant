/*
qdrant/lib/trififo/src/concurrent_cache_hashbrown.rs

A concurrent S3-FIFO cache implementation that keeps the single-writer
(disruptor) + concurrent-read (seqlock) architecture from
`concurrent_cache.rs` but uses `hashbrown::HashTable` instead of `papaya`.
*/

use std::hash::{BuildHasher, Hash};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use disruptor::{BusySpin, Producer, build_multi_producer};
use parking_lot::Mutex;

use hashbrown::HashTable;

use crate::concurrent_fifos::{Entry, LocalOffset, S3Fifo};
use crate::seqlock::{SeqLock, SeqLockReader};

/// A concurrent S3-FIFO cache using the Disruptor pattern and `hashbrown` for
/// the hashtable.
///
/// Design:
/// - Readers call `get(&self, key)` and obtain a read-side snapshot via a
///   `SeqLockReader`. Reads are lock-free (only atomic seqlock checks) and can
///   happen concurrently.
/// - Writers publish insert events to a disruptor ring buffer. A single
///   dedicated writer thread processes events and mutates the `CacheInner`.
pub struct ConcurrentCacheHashbrown<K, V, S = ahash::RandomState> {
    /// Shared state for lock-free readers.
    reader: SeqLockReader<CacheInner<K, V, S>>,
    /// Pool of producers for publishing insert events from multiple threads.
    producer_pool: Arc<ProducerPool<K, V>>,
}

/// Pool of mutex-protected publish closures to reduce publish contention.
struct ProducerPool<K, V> {
    producers: Box<[Mutex<Box<dyn FnMut(K, V) + Send>>]>,
    next_producer: AtomicUsize,
}

impl<K, V> ProducerPool<K, V> {
    fn new(producers: Vec<Box<dyn FnMut(K, V) + Send>>) -> Self {
        Self {
            producers: producers.into_iter().map(Mutex::new).collect(),
            next_producer: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn publish(&self, key: K, value: V) {
        let pool_size = self.producers.len();
        let index = self.next_producer.fetch_add(1, Ordering::Relaxed) % pool_size;
        let mut producer = self.producers[index].lock();
        (producer)(key, value);
    }
}

/// Event published into the disruptor ring buffer.
///
/// The event is simple: stores a key and a value. The processor will take a
/// clone of the value when needed. UnsafeCell isn't necessary here because the
/// disruptor factory provides fresh events; keep this simple and derive
/// Default.
#[derive(Default)]
struct InsertEvent<K, V> {
    key: K,
    value: V,
}

/// Inner cache state. This is the state the seqlock protects.
/// Readers observe an immutable snapshot of this struct via `SeqLockReader`.
struct CacheInner<K, V, S> {
    /// Non-concurrent hashtable mapping key -> global offset in the FIFOs.
    hashtable: HashTable<AtomicU32>,
    /// The actual FIFO structures (small, ghost, main).
    fifos: S3Fifo<K, V>,
    /// Hasher state used to compute the hash for lookups.
    hasher: S,
}

// ============================================================================
// Public API
// ============================================================================

impl<K, V, S> ConcurrentCacheHashbrown<K, V, S>
where
    K: Default + Copy + Hash + Eq + Send + Sync + 'static,
    V: Default + Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Default + Send + Sync + 'static,
{
    /// Default number of producers in the pool.
    const DEFAULT_PRODUCER_POOL_SIZE: usize = 4;

    /// Create a new concurrent cache with default disruptor size and producer pool.
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

    /// Create with custom disruptor ring size.
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

    /// Full configuration entrypoint.
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
            "disruptor_size must be power of two"
        );
        assert!(producer_pool_size > 0, "producer_pool_size must be > 0");

        // Create FIFOs (reader + writer halves are managed by S3Fifo)
        let fifos = S3Fifo::new(capacity, small_ratio, ghost_ratio);

        // Create a hashbrown hashtable with requested capacity.
        // IMPORTANT: allocate 1.5x the logical cache capacity up-front and avoid
        // resizing. Readers may be concurrently reading from the hashtable at any
        // moment, so we must ensure the table does not reallocate while the
        // system is running. We provision extra space (ceil(capacity * 1.5)) so
        // inserts performed by the single writer will not trigger a resize.
        let initial_capacity = (capacity * 3 + 1) / 2; // ceil(capacity * 1.5)
        let hashtable = HashTable::with_capacity(initial_capacity);

        let cache_inner = CacheInner {
            hashtable,
            fifos,
            hasher,
        };

        // Create seqlock reader/writer pair
        let (reader, cache_writer) = SeqLock::new_reader_writer(cache_inner);

        // Build disruptor with a processor that applies inserts on the writer thread.
        let factory = InsertEvent::default;
        let processor = move |event: &InsertEvent<K, V>, _sequence: i64, _end_of_batch: bool| {
            cache_writer.write(|cache| cache.do_insert(event.key, event.value.clone()));
        };

        let producer = build_multi_producer(disruptor_size, factory, BusySpin)
            .handle_events_with(processor)
            .build();

        // Create a small pool of producers to reduce publish contention
        let producers: Vec<Box<dyn FnMut(K, V) + Send>> = (0..producer_pool_size)
            .map(|_| {
                let mut producer_clone = producer.clone();
                Box::new(move |key: K, value: V| {
                    producer_clone.publish(|event| {
                        event.key = key;
                        event.value = value;
                    });
                }) as Box<dyn FnMut(K, V) + Send>
            })
            .collect();

        Self {
            reader,
            producer_pool: Arc::new(ProducerPool::new(producers)),
        }
    }

    /// Lock-free read of a value from the cache.
    ///
    /// Returns a cloned value (if present and not in the ghost queue).
    #[inline]
    pub fn get(&self, key: &K) -> Option<V> {
        self.reader.read(|cache| {
            // Compute hash using the stored hasher
            let hash = cache.hash_key(key);

            let global_offset = cache.hashtable.find(hash, |global_offset| {
                cache.fifos.key_eq(global_offset.load(Ordering::Relaxed), key)
            })?;

            let local = cache.fifos.local_offset(global_offset.load(Ordering::Relaxed));
            let entry = cache.fifos.get_entry(local)?;
            entry.incr_recency();
            Some(entry.value.clone())
        })
    }

    /// Publish an insert for processing by the writer thread.
    #[inline]
    pub fn insert(&self, key: K, value: V) {
        self.producer_pool.publish(key, value);
    }

    /// Number of entries tracked by the hashtable (reads via seqlock).
    pub fn len(&self) -> usize {
        self.reader.read(|cache| cache.hashtable.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ============================================================================
// CacheInner: writer-side logic (only touched by writer thread + seqlock)
// ============================================================================

impl<K, V, S> CacheInner<K, V, S>
where
    K: Copy + Hash + Eq + Send + Sync,
    V: Send + Sync,
    S: BuildHasher + Clone + Default + Send + Sync,
{
    /// Convenience to compute hash for a key using stored hasher.
    #[inline]
    fn hash_key(&self, key: &K) -> u64 {
        // Reuse same helper used in `cache.rs` / other modules: BuildHasher impls
        // in this crate expose `hash_one`. If unavailable, adapt accordingly.
        self.hasher.hash_one(key)
    }

    /// Update hashtable: insert or remove based on `local_offset`.
    fn update_hashtable(&mut self, key: &K, local_offset: Option<LocalOffset>) {
        let hash = self.hash_key(key);

        match local_offset {
            Some(local) => {
                let global_offset = self.fifos.global_offset(local);
                // Use the entry API to update or insert.
                let entry = self.hashtable.entry(
                    hash,
                    |global_offset| self.fifos.key_eq(global_offset.load(Ordering::Relaxed), key),
                    |global_offset| self.fifos.hash_key_at_offset(global_offset.load(Ordering::Relaxed), &self.hasher),
                );

                match entry {
                    hashbrown::hash_table::Entry::Occupied(occ) => {
                        occ.get().store(global_offset, Ordering::Relaxed);
                    }
                    hashbrown::hash_table::Entry::Vacant(vac) => {
                        vac.insert(AtomicU32::new(global_offset));
                    }
                }
            }
            None => {
                // Remove key if present
                if let Ok(entry) = self.hashtable.find_entry(hash, |global_offset| {
                    self.fifos.key_eq(global_offset.load(Ordering::Relaxed), key)
                }) {
                    entry.remove();
                }
            }
        }
    }

    /// Main insert implementation. Only executed on writer thread.
    fn do_insert(&mut self, key: K, value: V) {
        // Check existing entry
        let hash = self.hash_key(&key);
        if let Some(global_offset) = self.hashtable.find(hash, |global_offset| {
            self.fifos.key_eq(global_offset.load(Ordering::Relaxed), &key)
        }) {
            let local = self.fifos.local_offset(global_offset.load(Ordering::Relaxed));
            self.promote_existing(local, key, value);
            return;
        }

        // New entry -> insert into small queue
        let entry = Entry::new(key, value);
        let local = self.push_to_small_queue(entry);
        self.update_hashtable(&key, Some(local));
    }

    /// Promote existing entry or increment recency.
    fn promote_existing(&mut self, local_offset: LocalOffset, key: K, value: V) {
        match local_offset {
            LocalOffset::Ghost(offset) => {
                // Verify ghost key still matches
                let ghost_key = self.fifos.get_ghost_key(offset);
                if ghost_key != &key {
                    debug_assert!(false, "Key mismatch in ghost promotion");
                    return;
                }

                let entry = Entry::new(key, value);
                let new_local = self.push_to_main_queue(entry);
                self.update_hashtable(&key, Some(new_local));
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

    /// Push to main queue. Evict or reinsert based on recency.
    #[must_use]
    fn push_to_main_queue(&mut self, entry: Entry<K, V>) -> LocalOffset {
        // Try fast path
        let entry = match self.fifos.main.try_push(entry) {
            Ok(off) => return LocalOffset::Main(off as u32),
            Err(entry) => entry,
        };

        // Reinsert while entries with recency > 0 exist
        while self.fifos.main.reinsert_unchecked_if(|e| {
            if e.recency() > 0 {
                e.decr_recency();
                true
            } else {
                false
            }
        }) {}

        let (evicted, off) = self.fifos.main.pop_push_unchecked(entry);

        // Evicted entry's key should be removed from hashtable (it had a value)
        self.update_hashtable(&evicted.key, None);

        LocalOffset::Main(off as u32)
    }

    /// Push to small queue, handling eviction to main/ghost and updating hashtable.
    #[must_use]
    fn push_to_small_queue(&mut self, entry: Entry<K, V>) -> LocalOffset {
        // Try fast path
        let entry = match self.fifos.small.try_push(entry) {
            Ok(off) => return LocalOffset::Small(off as u32),
            Err(entry) => entry,
        };

        // Else, pop oldest and push new entry in its place
        let (oldest, off) = self.fifos.small.pop_push_unchecked(entry);
        let oldest_key = oldest.key;

        if oldest.recency() > 0 {
            // Promote to main queue and update hashtable
            let new_local = self.push_to_main_queue(oldest);
            self.update_hashtable(&oldest_key, Some(new_local));
        } else {
            // Move key to ghost queue and update hashtable
            let ghost_offset = self.push_to_ghost_queue(oldest_key);
            self.update_hashtable(&oldest_key, Some(ghost_offset));
        }

        LocalOffset::Small(off as u32)
    }

    fn push_to_ghost_queue(&mut self, key: K) -> LocalOffset {
        // Try fast path
        let key = match self.fifos.ghost.try_push(key) {
            Ok(offset) => return LocalOffset::Ghost(offset as u32),
            Err(key) => key,
        };

        // Else, pop oldest and push new entry in its place
        let (evicted, offset) = self.fifos.ghost.pop_push_unchecked(key);

        // Evict from hashtable
        self.update_hashtable(evicted, None);

        LocalOffset::Ghost(offset as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::thread;
    use std::time::Duration;
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex as StdMutex;
    use rand::{rng, Rng};

    #[test]
    fn basic_insert_get() {
        let cache = ConcurrentCacheHashbrown::<u64, String>::new(100, 0.1, 0.9, Default::default());

        cache.insert(1, "hello".to_string());
        thread::sleep(Duration::from_millis(10));

        let got = cache.get(&1);
        assert_eq!(got, Some("hello".to_string()));
    }

    #[test]
    fn concurrent_reads() {
        let cache = ConcurrentCacheHashbrown::<u64, String>::new(200, 0.1, 0.9, Default::default());

        for i in 0..20u64 {
            cache.insert(i, format!("val_{}", i));
        }

        thread::sleep(Duration::from_millis(50));

        let arc = Arc::new(cache);
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let c = Arc::clone(&arc);
                thread::spawn(move || {
                    for _ in 0..100 {
                        for k in 0..20u64 {
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
        let cache = Arc::new(ConcurrentCacheHashbrown::<u64, u64>::new(
            5000,
            0.1,
            0.9,
            Default::default(),
        ));

        const THREADS: usize = 4;
        const PER: u64 = 1000;

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let c = Arc::clone(&cache);
                thread::spawn(move || {
                    let start = (t as u64) * PER;
                    for i in start..(start + PER) {
                        c.insert(i, i * 2);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        thread::sleep(Duration::from_millis(100));

        // At least some entries should be present
        let mut found = 0usize;
        for i in 0..(THREADS as u64 * PER) {
            if cache.get(&i).is_some() {
                found += 1;
            }
        }
        assert!(found > 0);
    }

    #[test]
    fn len_is_empty() {
        let cache = ConcurrentCacheHashbrown::<u64, String>::new(100, 0.1, 0.9, Default::default());
        assert!(cache.is_empty());
        cache.insert(42, "v".to_string());
        thread::sleep(Duration::from_millis(10));
        assert!(!cache.is_empty());
        assert!(cache.len() > 0);
    }

    #[test]
    fn high_contention_inserts() {
        let cache = Arc::new(ConcurrentCacheHashbrown::<u64, u64>::new(
            2000,
            0.1,
            0.9,
            Default::default(),
        ));
        let counter = Arc::new(AtomicUsize::new(0));

        const THREADS: usize = 8;
        const INSERTS: usize = 500;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let c = Arc::clone(&cache);
                let ctr = Arc::clone(&counter);
                thread::spawn(move || {
                    for _ in 0..INSERTS {
                        let k = ctr.fetch_add(1, Ordering::Relaxed) as u64;
                        c.insert(k, k);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        thread::sleep(Duration::from_millis(100));
        assert!(cache.len() > 0);
    }

    // --- New fuzz test using repo RNG and a parallel seen map -----------------

    /// Heavy fuzz test that uses the repository RNG utilities and a parallel map
    /// of seen values per key to ensure the cache never returns a value that
    /// was not previously inserted for that key.
    #[test]
    fn fuzz_never_returns_unseen_value() {
        let cache = Arc::new(ConcurrentCacheHashbrown::<u64, u64>::new(
            1024, // capacity
            0.1,
            0.2,
            Default::default(),
        ));

        const KEY_SPACE: u64 = 512;
        const WRITERS: usize = 6;
        const READERS: usize = 6;
        const OPS_PER_WRITER: usize = 800_000;

        // Map of key -> set of values we've published for that key. Writers
        // insert into this map before publishing to the cache so readers can
        // validate returned values against this set.
        let seen: Arc<StdMutex<HashMap<u64, HashSet<u64>>>> = Arc::new(StdMutex::new(HashMap::new()));

        let mut handles = Vec::with_capacity(WRITERS + READERS);

        // Writers: generate pseudorandom (key, value) pairs using the repo RNG.
        for _ in 0..WRITERS {
            let c = Arc::clone(&cache);
            let seen = Arc::clone(&seen);
            handles.push(thread::spawn(move || {
                let mut rnd = rng();
                for _ in 0..OPS_PER_WRITER {
                    let key = rnd.random_range(0..KEY_SPACE) as u64;
                    let value = rnd.random::<u64>();
                    {
                        let mut map = seen.lock().unwrap();
                        map.entry(key).or_insert_with(HashSet::new).insert(value);
                    }
                    c.insert(key, value);
                    // occasional yield to increase interleaving
                    if (rnd.random::<u32>() & 0x3ff) == 0 {
                        thread::yield_now();
                    }
                }
            }));
        }

        // Readers: randomly probe keys and verify any returned value was seen.
        for _ in 0..READERS {
            let c = Arc::clone(&cache);
            let seen = Arc::clone(&seen);
            handles.push(thread::spawn(move || {
                let mut rnd = rng();
                // number of reads is proportional to total writes
                let reads = (OPS_PER_WRITER * WRITERS) / (READERS * 2).max(1);
                for _ in 0..reads {
                    let key = rnd.random_range(0..KEY_SPACE) as u64;
                    if let Some(v) = c.get(&key) {
                        let map = seen.lock().unwrap();
                        match map.get(&key) {
                            Some(set) => {
                                assert!(set.contains(&v), "cache returned unseen value {} for key {}", v, key);
                            }
                            None => {
                                panic!("cache returned value {} for key {} but no writer has published for this key", v, key);
                            }
                        }
                    }
                    // occasionally yield
                    if (rnd.random::<u8>() & 0x1f) == 0 {
                        thread::yield_now();
                    }
                }
            }));
        }

        // Wait for all threads to finish
        for h in handles {
            h.join().unwrap();
        }

        // Allow the writer/disruptor to flush events
        thread::sleep(Duration::from_millis(200));

        // Final verification: any value returned by the cache must belong to the seen set
        for k in 0..KEY_SPACE {
            if let Some(v) = cache.get(&k) {
                let map = seen.lock().unwrap();
                let set = map.get(&k).expect("cache returned value for key with no published values");
                assert!(set.contains(&v), "final check: returned {} for key {} which wasn't published", v, k);
            }
        }
    }
}
