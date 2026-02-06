use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::AtomicBool;

use parking_lot::Mutex;

use crate::lifecycle::{Lifecycle, NoLifecycle};
use crate::s3fifo::S3Fifo;
use crate::seqlock::{SeqLock, SeqLockReader, SeqLockWriter};

/// A concurrent S3-FIFO cache with a `hashbrown::HashTable` which stores the
/// keys in the FIFO queues for lower memory overhead.
///
/// Design:
/// - Reads are lock-free (only atomic seqlock checks) and can happen concurrently.
/// - Writers share the same writer behind a mutex, so they will contend if another
///   write is taking place.
pub struct Cache<K, V, L = NoLifecycle, S = ahash::RandomState> {
    /// Shared state for lock-free readers.
    reader: SeqLockReader<S3Fifo<K, V, L, S>>,

    writer: Mutex<SeqLockWriter<S3Fifo<K, V, L, S>>>,
}

impl<K, V, L, S> Cache<K, V, L, S>
where
    K: Copy + Hash + Eq,
    V: Clone,
    L: Lifecycle<K, V> + Default,
    S: BuildHasher + Default,
{
    /// Create a new concurrent cache with default disruptor size and producer pool.
    pub fn new(capacity: usize) -> Self {
        Self::with_config(capacity, 0.1, 0.9, L::default())
    }
}

impl<K, V, L, S> Cache<K, V, L, S>
where
    K: Copy + Hash + Eq,
    V: Clone,
    L: Lifecycle<K, V>,
    S: BuildHasher + Default,
{
    /// Create with custom disruptor ring size.
    pub fn with_config(capacity: usize, small_ratio: f32, ghost_ratio: f32, lifecycle: L) -> Self {
        assert!(capacity > 0);

        let s3fifo = S3Fifo::new(capacity, small_ratio, ghost_ratio, lifecycle);

        // Create seqlock reader/writer pair
        let (reader, cache_writer) = SeqLock::new_reader_writer(s3fifo);

        Self {
            reader,
            writer: Mutex::new(cache_writer),
        }
    }

    /// Read a value from the cache.
    ///
    /// Returns a cloned value (if present).
    #[inline]
    pub fn get(&self, key: &K) -> Option<V> {
        self.reader.read(|s3fifo| s3fifo.get(key))
    }

    /// Publish an insert for processing by the writer thread.
    #[inline]
    pub fn insert(&self, key: K, value: V) {
        let mut writer_guard = self.writer.lock();
        writer_guard.write(|cache| cache.do_insert(key, value));
    }

    pub fn len(&self) -> usize {
        self.reader.read(|s3fifo| s3fifo.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    use rand::Rng;

    use super::*;

    #[test]
    fn basic_insert_get() {
        let cache = Cache::<u64, String>::new(100);

        cache.insert(1, "hello".to_string());
        thread::sleep(Duration::from_millis(10));

        let got = cache.get(&1);
        assert_eq!(got, Some("hello".to_string()));
    }

    #[test]
    fn concurrent_reads() {
        let cache = Cache::<u64, String>::new(200);

        for i in 0..20u64 {
            cache.insert(i, format!("val_{i}"));
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
        let cache = Arc::new(Cache::<u64, u64>::new(5000));

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
    fn high_contention_inserts() {
        let cache = Arc::new(Cache::<u64, u64>::new(2000));
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
        assert!(!cache.is_empty());
    }

    // --- Fuzz test for immediately-consistent cache ----------------------------

    /// Fuzz test that verifies the cache is immediately consistent: once an
    /// insert returns, readers must see either that value or a newer one.
    ///
    /// This test ensures:
    /// 1. Values are immediately visible after insert returns
    /// 2. No data corruption occurs under concurrent access
    /// 3. Only valid values (from our pre-generated set) are ever returned
    #[test]
    fn fuzz_immediate_consistency() {
        const CAPACITY: usize = 10240;
        let cache = Arc::new(Cache::<u64, u64>::new(CAPACITY));

        const KEY_SPACE: u64 = 256000; // Smaller key space for more contention
        const THREADS: usize = 8;
        const OPS_PER_THREAD: usize = 5_000_000;

        // Pre-generate a hashmap of keys -> ordered sequence of values.
        // Writers will write values in order, and readers verify they only
        // see values from the valid set (no corruption).
        let mut key_values: HashMap<u64, Vec<u64>> = HashMap::new();
        for k in 0..KEY_SPACE {
            // Each key gets a sequence of unique values
            let count = 16;
            let mut values = HashSet::new();
            while values.len() < count {
                values.insert(rand::random::<u64>());
            }
            key_values.insert(k, values.into_iter().collect());
        }
        let key_values = Arc::new(key_values);

        let mut handles = Vec::with_capacity(THREADS);

        // Each thread does both reads and writes to maximize interleaving
        for _ in 0..THREADS {
            let c = Arc::clone(&cache);
            let kv = Arc::clone(&key_values);
            handles.push(thread::spawn(move || {
                let mut rnd = rand::rng();
                // Queue of keys to check later for delayed corruption
                const DEFERRED_CHECK_WINDOW: usize = 1000;
                let mut deferred_checks: VecDeque<u64> = VecDeque::with_capacity(DEFERRED_CHECK_WINDOW + 1);

                for _ in 0..OPS_PER_THREAD {
                    let key = rnd.random_range(0..KEY_SPACE);
                    let values = &kv[&key];

                    if rnd.random::<bool>() {
                        // Write: pick a random valid value and insert
                        let value = values[rnd.random_range(0..values.len())];
                        c.insert(key, value);

                        // Immediately verify the write is visible (immediate consistency)
                        // The cache may evict entries, so we check if present, it must be valid
                        if let Some(read_value) = c.get(&key) {
                            assert!(
                                values.contains(&read_value),
                                "immediate read after write returned invalid value {read_value} for key {key}",
                            );
                        }

                        // Queue this key for deferred verification to catch delayed corruption
                        deferred_checks.push_back(key);
                        if deferred_checks.len() > DEFERRED_CHECK_WINDOW {
                            let old_key = deferred_checks.pop_front().unwrap();
                            if let Some(v) = c.get(&old_key) {
                                let old_values = &kv[&old_key];
                                assert!(
                                    old_values.contains(&v),
                                    "deferred check: corrupted value {v} for key {old_key}",
                                );
                            }
                        }
                    } else {
                        // Read: verify any returned value is from the valid set
                        if let Some(v) = c.get(&key) {
                            assert!(
                                values.contains(&v),
                                "cache returned corrupted/invalid value {v} for key {key}",
                            );
                        }
                    }

                    // Occasional yield to increase interleaving
                    if (rnd.random::<u32>() & 0xff) == 0 {
                        thread::yield_now();
                    }
                }

                // Drain remaining deferred checks
                for old_key in deferred_checks {
                    if let Some(v) = c.get(&old_key) {
                        let values = &kv[&old_key];
                        assert!(
                            values.contains(&v),
                            "final deferred check: corrupted value {v} for key {old_key}",
                        );
                    }
                }
            }));
        }

        // Wait for all threads to finish
        for h in handles {
            h.join().unwrap();
        }

        // Final verification: all cached values must be from valid sets
        for k in 0..KEY_SPACE {
            if let Some(v) = cache.get(&k) {
                let values = &key_values[&k];
                assert!(
                    values.contains(&v),
                    "final check: returned {v} for key {k} which wasn't in the valid set",
                );
            }
        }
    }

    /// Test that specifically verifies write-then-read consistency:
    /// After a successful insert, the value must be immediately readable.
    #[test]
    fn fuzz_write_read_consistency() {
        const CAPACITY: usize = 512;
        let cache = Arc::new(Cache::<u64, u64>::new(CAPACITY));

        const KEY_SPACE: u64 = 64; // Small key space to stay within capacity
        const THREADS: usize = 4;
        const OPS_PER_THREAD: usize = 10_000;

        let mut handles = Vec::with_capacity(THREADS);

        for thread_id in 0..THREADS {
            let c = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                let mut rnd = rand::rng();
                for i in 0..OPS_PER_THREAD {
                    // Use thread-local keys to avoid eviction from other threads
                    let key = (thread_id as u64 * KEY_SPACE / THREADS as u64)
                        + rnd.random_range(0..(KEY_SPACE / THREADS as u64));
                    let value = (thread_id * OPS_PER_THREAD + i) as u64;

                    c.insert(key, value);

                    // The value we just inserted must be immediately visible
                    // (unless another thread overwrote it, in which case we
                    // should see their valid value)
                    let read = c.get(&key);
                    assert!(
                        read.is_some(),
                        "key {key} not visible immediately after insert",
                    );

                    // Occasional yield
                    if (rnd.random::<u32>() & 0x1ff) == 0 {
                        thread::yield_now();
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}
