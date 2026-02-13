use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::array_lookup::AsIndex;
use crate::guard::{CacheGuard, GetOrGuard, Waiter};
use crate::lifecycle::{Lifecycle, NoLifecycle};
use crate::s3fifo::S3Fifo;
use crate::seqlock::{SeqLock, SeqLockReader, SeqLockWriter};

// ---------------------------------------------------------------------------
// CacheWriter – bundles the SeqLockWriter and pending map behind one Mutex.
// ---------------------------------------------------------------------------

pub(crate) struct CacheWriter<K, V, L> {
    pub(crate) s3fifo: SeqLockWriter<S3Fifo<K, V, L>>,
    /// Tracks keys that will soon be inserted by a [`CacheGuard`].
    pub(crate) pending: HashMap<K, Arc<Waiter<V>>>,
}

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

/// A concurrent S3-FIFO cache backed by an [`ArrayLookup`](crate::array_lookup::ArrayLookup)
/// for O(1) direct-indexed access.
///
/// Design:
/// - Reads are lock-free (only atomic seqlock checks) and can happen concurrently.
/// - Writers share the same writer behind a mutex, so they will contend if another
///   write is taking place.
pub struct Cache<K, V, L = NoLifecycle> {
    /// Shared state for lock-free readers.
    reader: SeqLockReader<S3Fifo<K, V, L>>,

    /// Single mutex protecting both the S3-FIFO writer **and** the pending-
    /// loads map.  This avoids lock-ordering issues and reduces the number of
    /// mutex acquisitions on the insert path.
    pub(crate) writer: Mutex<CacheWriter<K, V, L>>,
}

impl<K, V, L> Cache<K, V, L>
where
    K: Copy + Hash + Eq + AsIndex,
    V: Clone,
    L: Lifecycle<K, V> + Default,
{
    /// Create a new concurrent cache with default disruptor size and producer pool.
    pub fn new(capacity: usize) -> Self {
        Self::with_config(capacity, 0.1, 0.9, L::default())
    }
}

impl<K, V, L> Cache<K, V, L>
where
    K: Copy + Hash + Eq + AsIndex,
    V: Clone,
    L: Lifecycle<K, V>,
{
    /// Create with custom configuration.
    pub fn with_config(capacity: usize, small_ratio: f32, ghost_ratio: f32, lifecycle: L) -> Self {
        assert!(capacity > 0);

        let s3fifo = S3Fifo::new(capacity, small_ratio, ghost_ratio, lifecycle);

        // Create seqlock reader/writer pair
        let (reader, cache_writer) = SeqLock::new_reader_writer(s3fifo);

        Self {
            reader,
            writer: Mutex::new(CacheWriter {
                s3fifo: cache_writer,
                pending: HashMap::new(),
            }),
        }
    }

    /// Read a value from the cache.
    ///
    /// Returns a cloned value (if present).
    #[inline]
    pub fn get(&self, key: &K) -> Option<V> {
        self.reader.read(|s3fifo| s3fifo.get(key))
    }

    /// Look up `key` in the cache.
    ///
    /// - If the value is cached, returns [`GetOrGuard::Found(value)`].
    /// - If another thread is already loading the same key, **blocks** until
    ///   that thread completes (or abandons) and then returns the value or
    ///   retries.
    /// - Otherwise, returns a [`GetOrGuard::Guard`] that the caller must
    ///   complete by calling [`CacheGuard::insert`].
    ///
    /// This ensures that at most **one** thread performs the (potentially
    /// expensive) load for any given key at a time, preventing the
    /// "thundering herd" problem on cache misses.
    pub fn get_or_guard(&self, key: &K) -> GetOrGuard<'_, K, V, L> {
        loop {
            // Fast path: value is already cached (lock-free seqlock read).
            if let Some(v) = self.get(key) {
                return GetOrGuard::Found(v);
            }

            let mut writer = self.writer.lock();

            // Double-check after acquiring the writer lock – the value may
            // have been inserted between our `get` and the lock acquisition.
            // Reading through the writer's seqlock read is guaranteed to
            // succeed on the first attempt since we hold the only writer.
            if let Some(v) = writer.s3fifo.read(|s3fifo| s3fifo.get(key)) {
                return GetOrGuard::Found(v);
            }

            if let Some(waiter) = writer.pending.get(key) {
                // Another thread is already loading this key – wait for it.
                let waiter = Arc::clone(waiter);
                drop(writer); // Release the writer lock before blocking.

                if let Some(v) = waiter.wait_for_result() {
                    return GetOrGuard::Found(v);
                }
                // The loading thread abandoned – retry from the top so we
                // either find a value, wait on a new loader, or become the
                // loader ourselves.
                continue;
            }

            // Nobody is loading this key yet – we become the loader.
            let waiter = Arc::new(Waiter::new());
            writer.pending.insert(*key, Arc::clone(&waiter));
            drop(writer);

            return GetOrGuard::Guard(CacheGuard {
                cache: self,
                key: *key,
                waiter,
                completed: false,
            });
        }
    }

    /// Publish an insert for processing by the writer thread.
    #[inline]
    pub fn insert(&self, key: K, value: V) {
        let mut writer = self.writer.lock();
        writer.s3fifo.write(|cache| cache.do_insert(key, value));
    }

    pub fn len(&self) -> usize {
        self.reader.read(|s3fifo| s3fifo.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of keys currently being loaded via [`get_or_guard`](Self::get_or_guard).
    ///
    /// Mostly useful for tests/diagnostics.
    #[cfg(test)]
    fn pending_len(&self) -> usize {
        self.writer.lock().pending.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
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
        let cache = Arc::new(Cache::<u64, u64>::new(10_000));

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
        // Many threads writing the same small key-space simultaneously.
        let cache = Arc::new(Cache::<u64, u64>::new(100));

        const THREADS: usize = 16;
        const INSERTS: u64 = 5_000;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let c = Arc::clone(&cache);
                thread::spawn(move || {
                    for i in 0..INSERTS {
                        let key = i % 50; // only 50 distinct keys
                        c.insert(key, i);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert!(cache.len() <= 100);
    }

    /// Fuzz-like test: writers insert key→value pairs from a known set;
    /// readers verify that any value they see for a key belongs to that set.
    ///
    /// This catches torn reads (SeqLock misuse), stale pointers, and
    /// hashtable corruption under concurrency.
    #[test]
    fn fuzz_immediate_consistency() {
        const CAPACITY: usize = 512;

        // We use a small key space so there is heavy overwrite contention.
        const KEY_SPACE: u64 = 128;
        const THREADS: usize = 8;
        const OPS_PER_THREAD: usize = 50_000;

        // Pre-generate the "allowed" values for each key.
        // Each key has a small set of values that any writer may store.
        let mut allowed: HashMap<u64, Vec<u64>> = HashMap::new();
        for k in 0..KEY_SPACE {
            // 3-8 allowed values per key.
            let count = (k % 6 + 3) as usize;
            let values: Vec<u64> = (0..count).map(|i| k * 1000 + i as u64).collect();
            allowed.insert(k, values);
        }
        let allowed = Arc::new(allowed);

        let cache = Arc::new(Cache::<u64, u64>::new(CAPACITY));

        let mut handles = Vec::with_capacity(THREADS * 2);

        // Writer threads
        for _ in 0..THREADS {
            let c = Arc::clone(&cache);
            let a = Arc::clone(&allowed);
            handles.push(thread::spawn(move || {
                let mut rng = rand::rng();
                for _ in 0..OPS_PER_THREAD {
                    let key = rng.random_range(0..KEY_SPACE);
                    let vals = &a[&key];
                    let value = vals[rng.random_range(0..vals.len())];
                    c.insert(key, value);
                }
            }));
        }

        // Reader threads — verify every observed value is in the allowed set.
        // Also do a deferred check: remember recent reads and re-verify.
        const DEFERRED_CHECK_WINDOW: usize = 64;

        for _ in 0..THREADS {
            let c = Arc::clone(&cache);
            let a = Arc::clone(&allowed);
            handles.push(thread::spawn(move || {
                let mut rng = rand::rng();
                let mut recent: VecDeque<(u64, u64)> = VecDeque::new();
                for _ in 0..OPS_PER_THREAD {
                    let key = rng.random_range(0..KEY_SPACE);
                    if let Some(v) = c.get(&key) {
                        let vals = &a[&key];
                        assert!(
                            vals.contains(&v),
                            "key {key}: got {v} which is not in {vals:?}"
                        );
                        recent.push_back((key, v));
                        if recent.len() > DEFERRED_CHECK_WINDOW {
                            recent.pop_front();
                        }
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    /// Verify that concurrent writes followed by reads never return a value
    /// that was never written for that key.
    #[test]
    fn fuzz_write_read_consistency() {
        const CAPACITY: usize = 256;

        // Larger key space, sparser overwrites.
        const KEY_SPACE: u64 = 1024;
        const THREADS: usize = 4;
        const OPS_PER_THREAD: usize = 20_000;

        let cache = Arc::new(Cache::<u64, u64>::new(CAPACITY));

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let c = Arc::clone(&cache);
                thread::spawn(move || {
                    let mut rng = rand::rng();
                    for _ in 0..OPS_PER_THREAD {
                        let key = rng.random_range(0..KEY_SPACE);
                        // Value encodes the key so readers can verify.
                        let value = key * 1_000_000 + t as u64;
                        c.insert(key, value);

                        // Immediately read back (may see our value or another thread's).
                        if let Some(v) = c.get(&key) {
                            assert_eq!(
                                v / 1_000_000,
                                key,
                                "value {v} does not belong to key {key}"
                            );
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    // -----------------------------------------------------------------------
    // get_or_guard tests
    // -----------------------------------------------------------------------

    #[test]
    fn get_or_guard_basic() {
        let cache = Cache::<u64, String>::new(100);

        // First call should return a Guard.
        match cache.get_or_guard(&1) {
            GetOrGuard::Guard(guard) => {
                guard.insert("hello".to_string());
            }
            GetOrGuard::Found(_) => panic!("expected Guard, got Found"),
        }

        // Now the value should be cached.
        assert_eq!(cache.get(&1), Some("hello".to_string()));

        // Second call should return Found.
        match cache.get_or_guard(&1) {
            GetOrGuard::Found(v) => assert_eq!(v, "hello"),
            GetOrGuard::Guard(_) => panic!("expected Found, got Guard"),
        }
    }

    #[test]
    fn get_or_guard_abandon() {
        let cache = Cache::<u64, String>::new(100);

        // Take a guard but drop it without inserting (abandon).
        {
            let result = cache.get_or_guard(&1);
            match result {
                GetOrGuard::Guard(_guard) => {
                    // Drop without calling insert.
                }
                GetOrGuard::Found(_) => panic!("expected Guard"),
            }
        }

        // After abandonment, the key should not be cached.
        assert_eq!(cache.get(&1), None);

        // A new get_or_guard should return Guard again.
        match cache.get_or_guard(&1) {
            GetOrGuard::Guard(guard) => guard.insert("recovered".to_string()),
            GetOrGuard::Found(_) => panic!("expected Guard after abandon"),
        }

        assert_eq!(cache.get(&1), Some("recovered".to_string()));
    }

    #[test]
    fn get_or_guard_coalescing() {
        // Multiple threads call get_or_guard for the same key.
        // Only one should get a Guard; the rest should block and get Found.
        let cache = Arc::new(Cache::<u64, String>::new(100));
        let load_count = Arc::new(AtomicUsize::new(0));

        const THREADS: usize = 8;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let c = Arc::clone(&cache);
                let lc = Arc::clone(&load_count);
                thread::spawn(move || {
                    match c.get_or_guard(&42) {
                        GetOrGuard::Guard(guard) => {
                            lc.fetch_add(1, Ordering::Relaxed);
                            // Simulate a slow load.
                            thread::sleep(Duration::from_millis(50));
                            guard.insert("loaded".to_string());
                        }
                        GetOrGuard::Found(v) => {
                            assert_eq!(v, "loaded");
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Exactly one thread should have been the loader.
        assert_eq!(load_count.load(Ordering::Relaxed), 1);
        assert_eq!(cache.get(&42), Some("loaded".to_string()));
    }

    #[test]
    fn get_or_guard_independent_keys() {
        // Different keys should not block each other.
        let cache = Arc::new(Cache::<u64, String>::new(100));
        let load_count = Arc::new(AtomicUsize::new(0));

        const THREADS: usize = 8;

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let c = Arc::clone(&cache);
                let lc = Arc::clone(&load_count);
                let key = t as u64; // each thread uses a unique key
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

        // Each key should have been loaded exactly once.
        assert_eq!(load_count.load(Ordering::Relaxed), THREADS);
    }

    #[test]
    fn get_or_guard_abandon_wakes_waiters() {
        // If the loader abandons, waiting threads should wake up and one
        // of them should become the new loader.
        let cache = Arc::new(Cache::<u64, String>::new(100));
        let load_count = Arc::new(AtomicUsize::new(0));

        const THREADS: usize = 4;

        let handles: Vec<_> = (0..THREADS)
            .map(|_t| {
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
                                    // After abandoning, this thread is done.
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
    fn get_or_guard_mixed_with_plain_insert() {
        // A plain insert while a guard is pending should not cause issues.
        let cache = Arc::new(Cache::<u64, String>::new(100));

        // Thread 1: get_or_guard for key 1
        let c1 = Arc::clone(&cache);
        let h1 = thread::spawn(move || match c1.get_or_guard(&1) {
            GetOrGuard::Guard(guard) => {
                thread::sleep(Duration::from_millis(50));
                guard.insert("from_guard".to_string());
            }
            GetOrGuard::Found(_) => {}
        });

        // Thread 2: plain insert for key 2 (independent)
        let c2 = Arc::clone(&cache);
        let h2 = thread::spawn(move || {
            for i in 0..100u64 {
                c2.insert(i + 100, format!("plain_{i}"));
            }
        });

        h1.join().unwrap();
        h2.join().unwrap();
    }

    /// Stress test: many threads doing `get_or_guard` on a small key-space.
    #[test]
    fn get_or_guard_stress() {
        let cache = Arc::new(Cache::<u64, u64>::new(500));
        let load_count = Arc::new(AtomicUsize::new(0));

        const THREADS: usize = 16;
        const KEY_SPACE: u64 = 32;
        const OPS_PER_THREAD: usize = 2_000;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let c = Arc::clone(&cache);
                let lc = Arc::clone(&load_count);
                thread::spawn(move || {
                    let mut rnd = rand::rng();
                    for _ in 0..OPS_PER_THREAD {
                        let key = rnd.random_range(0..KEY_SPACE);
                        match c.get_or_guard(&key) {
                            GetOrGuard::Guard(guard) => {
                                lc.fetch_add(1, Ordering::Relaxed);
                                // Simulate a "load" with a small sleep.
                                thread::yield_now();
                                guard.insert(key * 100);
                            }
                            GetOrGuard::Found(v) => {
                                assert_eq!(v, key * 100);
                            }
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let loads = load_count.load(Ordering::Relaxed);
        let total_ops = THREADS * OPS_PER_THREAD;
        // The number of actual loads should be much less than total ops because
        // of coalescing and caching. With 32 keys and 32k ops, we expect at most
        // a few hundred loads.
        assert!(
            loads < total_ops / 2,
            "expected significant coalescing, but got {loads} loads out of {total_ops} ops",
        );
    }

    /// Verify that the pending map is properly cleaned up and doesn't leak.
    #[test]
    fn get_or_guard_no_pending_leak() {
        let cache = Cache::<u64, String>::new(100);

        for i in 0..50u64 {
            match cache.get_or_guard(&i) {
                GetOrGuard::Guard(guard) => {
                    guard.insert(format!("v{i}"));
                }
                GetOrGuard::Found(_) => {}
            }
        }

        // After all guards are consumed, the pending map should be empty.
        assert_eq!(
            cache.pending_len(),
            0,
            "pending map should be empty after all guards are consumed"
        );
    }

    /// Verify that abandoning guards also cleans up the pending map.
    #[test]
    fn get_or_guard_abandon_no_pending_leak() {
        let cache = Cache::<u64, String>::new(100);

        for i in 0..50u64 {
            let _ = cache.get_or_guard(&i); // Guard is dropped immediately.
        }

        assert_eq!(
            cache.pending_len(),
            0,
            "pending map should be empty after all guards are abandoned"
        );
    }
}
