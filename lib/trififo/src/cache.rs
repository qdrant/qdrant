use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::lifecycle::{Lifecycle, NoLifecycle};
use crate::s3fifo::S3Fifo;
use crate::seqlock::{SeqLock, SeqLockReader, SeqLockWriter};

// ---------------------------------------------------------------------------
// Waiter: shared state that lets threads waiting for the same key coalesce.
// ---------------------------------------------------------------------------

enum WaiterState<V> {
    /// The holder of the guard is still loading the value.
    Pending,
    /// The value was successfully loaded and inserted into the cache.
    Completed(V),
    /// The guard was dropped without inserting a value.
    Abandoned,
}

struct Waiter<V> {
    state: Mutex<WaiterState<V>>,
    condvar: Condvar,
}

impl<V> Waiter<V> {
    fn new() -> Self {
        Self {
            state: Mutex::new(WaiterState::Pending),
            condvar: Condvar::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// GetOrGuard – the return type of `get_or_guard`.
// ---------------------------------------------------------------------------

/// Result of [`Cache::get_or_guard`].
///
/// Either the value was already present in the cache ([`GetOrGuard::Found`]),
/// or the caller is responsible for loading it and must complete the returned
/// [`CacheGuard`] ([`GetOrGuard::Guard`]).
pub enum GetOrGuard<'a, K: Copy + Hash + Eq, V, L, S> {
    /// The value was found in the cache (or was completed by another thread's
    /// guard while we were waiting).
    Found(V),
    /// No value is cached and no other thread is currently loading it.
    /// The caller **must** either call [`CacheGuard::insert`] to provide the
    /// value, or drop the guard to let other waiters retry.
    Guard(CacheGuard<'a, K, V, L, S>),
}

// ---------------------------------------------------------------------------
// CacheGuard – exclusive "loading" ticket for a single key.
// ---------------------------------------------------------------------------

/// A guard that indicates the current thread is responsible for loading the
/// value for a given key.
///
/// Dropping the guard **without** calling [`insert`](CacheGuard::insert) will
/// mark the load as abandoned and wake any waiting threads so they can retry.
pub struct CacheGuard<'a, K: Copy + Hash + Eq, V, L, S> {
    cache: &'a Cache<K, V, L, S>,
    key: K,
    waiter: Arc<Waiter<V>>,
    completed: bool,
}

impl<'a, K, V, L, S> CacheGuard<'a, K, V, L, S>
where
    K: Copy + Hash + Eq,
    V: Clone,
    L: Lifecycle<K, V>,
    S: BuildHasher + Default,
{
    /// Insert the loaded value into the cache and wake all waiting threads.
    ///
    /// Returns a clone of the value for convenience.
    pub fn insert(mut self, value: V) -> V {
        // 1. Lock the single writer mutex, insert into the cache AND remove
        //    from pending atomically.
        {
            let mut writer = self.cache.writer.lock();
            writer
                .s3fifo
                .write(|cache| cache.do_insert(self.key, value.clone()));
            writer.pending.remove(&self.key);
        }

        // 2. Publish the value to any threads that are waiting on this key
        //    (outside the writer lock so we don't hold it while waiters wake).
        {
            let mut state = self.waiter.state.lock();
            *state = WaiterState::Completed(value.clone());
            self.waiter.condvar.notify_all();
        }

        self.completed = true;
        value
    }

    /// Returns the key this guard is responsible for loading.
    pub fn key(&self) -> &K {
        &self.key
    }
}

impl<K, V, L, S> Drop for CacheGuard<'_, K, V, L, S>
where
    K: Copy + Hash + Eq,
{
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        // Guard was dropped without inserting → abandon.
        // Remove from pending under the writer lock so new arrivals create a
        // fresh waiter.
        {
            let mut writer = self.cache.writer.lock();
            writer.pending.remove(&self.key);
        }

        // Then wake everyone so they can retry (outside the writer lock).
        {
            let mut state = self.waiter.state.lock();
            *state = WaiterState::Abandoned;
            self.waiter.condvar.notify_all();
        }
    }
}

// ---------------------------------------------------------------------------
// CacheWriter – bundles the SeqLockWriter and pending map behind one Mutex.
// ---------------------------------------------------------------------------

struct CacheWriter<K, V, L, S> {
    s3fifo: SeqLockWriter<S3Fifo<K, V, L, S>>,
    /// Tracks keys that are currently being loaded by a [`CacheGuard`].
    pending: HashMap<K, Arc<Waiter<V>>>,
}

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

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

    /// Single mutex protecting both the S3-FIFO writer **and** the pending-
    /// loads map.  This avoids lock-ordering issues and reduces the number of
    /// mutex acquisitions on the insert path.
    writer: Mutex<CacheWriter<K, V, L, S>>,
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
    pub fn get_or_guard(&self, key: &K) -> GetOrGuard<'_, K, V, L, S> {
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

                let mut state = waiter.state.lock();
                loop {
                    match &*state {
                        WaiterState::Pending => {
                            waiter.condvar.wait(&mut state);
                        }
                        WaiterState::Completed(v) => {
                            return GetOrGuard::Found(v.clone());
                        }
                        WaiterState::Abandoned => {
                            // The loading thread gave up – break out of the
                            // inner loop and retry the outer loop so we either
                            // find a value, wait on a new loader, or become
                            // the loader ourselves.
                            break;
                        }
                    }
                }
                // Retry from the top.
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
        writer
            .s3fifo
            .write(|cache| cache.do_insert(key, value));
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
                let mut deferred_checks: VecDeque<u64> =
                    VecDeque::with_capacity(DEFERRED_CHECK_WINDOW + 1);

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

    // --- get_or_guard tests ---------------------------------------------------

    /// Basic test: `get_or_guard` returns `Guard` on a miss, then `Found` after
    /// the guard inserts.
    #[test]
    fn get_or_guard_basic() {
        let cache = Cache::<u64, String>::new(100);

        // First call should yield a guard.
        match cache.get_or_guard(&42) {
            GetOrGuard::Guard(guard) => {
                guard.insert("hello".to_string());
            }
            GetOrGuard::Found(_) => panic!("expected Guard on first access"),
        }

        // Second call should yield Found.
        match cache.get_or_guard(&42) {
            GetOrGuard::Found(v) => assert_eq!(v, "hello"),
            GetOrGuard::Guard(_) => panic!("expected Found after insert"),
        }
    }

    /// If the guard is dropped without inserting, subsequent callers should be
    /// able to get a new guard (i.e. it doesn't deadlock or leave stale state).
    #[test]
    fn get_or_guard_abandon() {
        let cache = Cache::<u64, String>::new(100);

        // Get guard and drop it without inserting.
        match cache.get_or_guard(&7) {
            GetOrGuard::Guard(_guard) => { /* drop */ }
            GetOrGuard::Found(_) => panic!("expected Guard"),
        }

        // Should be able to get a new guard.
        match cache.get_or_guard(&7) {
            GetOrGuard::Guard(guard) => {
                guard.insert("recovered".to_string());
            }
            GetOrGuard::Found(_) => panic!("expected Guard after abandon"),
        }

        assert_eq!(cache.get(&7), Some("recovered".to_string()));
    }

    /// Multiple threads call `get_or_guard` for the *same* key concurrently.
    /// Only one should get a `Guard`; the rest should block and eventually get
    /// `Found` with the correct value.
    #[test]
    fn get_or_guard_coalescing() {
        let cache = Arc::new(Cache::<u64, String>::new(100));
        let guard_count = Arc::new(AtomicUsize::new(0));
        let found_count = Arc::new(AtomicUsize::new(0));

        const THREADS: usize = 8;
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let c = Arc::clone(&cache);
                let gc = Arc::clone(&guard_count);
                let fc = Arc::clone(&found_count);
                let b = Arc::clone(&barrier);
                thread::spawn(move || {
                    b.wait(); // Synchronise all threads.
                    match c.get_or_guard(&99) {
                        GetOrGuard::Guard(guard) => {
                            gc.fetch_add(1, Ordering::SeqCst);
                            // Simulate slow load.
                            thread::sleep(Duration::from_millis(50));
                            guard.insert("coalesced".to_string());
                        }
                        GetOrGuard::Found(v) => {
                            fc.fetch_add(1, Ordering::SeqCst);
                            assert_eq!(v, "coalesced");
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            guard_count.load(Ordering::SeqCst),
            1,
            "exactly one thread should get the guard"
        );
        assert_eq!(found_count.load(Ordering::SeqCst), THREADS - 1);
    }

    /// Multiple threads call `get_or_guard` for *different* keys concurrently.
    /// Each key should get its own guard and they should not block each other.
    #[test]
    fn get_or_guard_independent_keys() {
        let cache = Arc::new(Cache::<u64, u64>::new(200));

        const THREADS: usize = 8;
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let c = Arc::clone(&cache);
                let b = Arc::clone(&barrier);
                thread::spawn(move || {
                    let key = t as u64;
                    b.wait();
                    match c.get_or_guard(&key) {
                        GetOrGuard::Guard(guard) => {
                            guard.insert(key * 10);
                        }
                        GetOrGuard::Found(_) => {
                            panic!("each thread has a unique key, should always get guard");
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        for t in 0..THREADS as u64 {
            assert_eq!(cache.get(&t), Some(t * 10));
        }
    }

    /// Test that when the guard holder abandons (drops without insert), blocked
    /// waiters wake up and one of them gets a new guard.
    #[test]
    fn get_or_guard_abandon_wakes_waiters() {
        let cache = Arc::new(Cache::<u64, String>::new(100));
        let guard_count = Arc::new(AtomicUsize::new(0));

        const THREADS: usize = 4;
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let c = Arc::clone(&cache);
                let gc = Arc::clone(&guard_count);
                let b = Arc::clone(&barrier);
                thread::spawn(move || {
                    b.wait();
                    match c.get_or_guard(&1) {
                        GetOrGuard::Guard(guard) => {
                            let n = gc.fetch_add(1, Ordering::SeqCst);
                            if n == 0 {
                                // First guard holder: abandon.
                                drop(guard);
                            } else {
                                // Subsequent guard holder: actually insert.
                                guard.insert(format!("thread-{t}"));
                            }
                        }
                        GetOrGuard::Found(v) => {
                            assert!(v.starts_with("thread-"));
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // The value should exist now (one of the later guard holders inserted).
        assert!(cache.get(&1).is_some());
    }

    /// Ensure `get_or_guard` doesn't deadlock when mixed with plain `insert`.
    #[test]
    fn get_or_guard_mixed_with_plain_insert() {
        let cache = Arc::new(Cache::<u64, u64>::new(200));

        let c1 = Arc::clone(&cache);
        let h1 = thread::spawn(move || {
            for i in 0..100u64 {
                c1.insert(i, i);
            }
        });

        let c2 = Arc::clone(&cache);
        let h2 = thread::spawn(move || {
            for i in 100..200u64 {
                match c2.get_or_guard(&i) {
                    GetOrGuard::Guard(guard) => {
                        guard.insert(i * 2);
                    }
                    GetOrGuard::Found(_) => {}
                }
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