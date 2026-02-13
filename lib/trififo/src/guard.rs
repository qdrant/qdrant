use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::Cache;
use crate::lifecycle::Lifecycle;

pub enum WaiterState<V> {
    /// The holder of the guard is still loading the value.
    Pending,
    /// The value was successfully loaded and inserted into the cache.
    Completed(V),
    /// The guard was dropped without inserting a value.
    Abandoned,
}

/// Shared state that lets threads waiting for the same key coalesce.
pub struct Waiter<V> {
    pub(crate) state: Mutex<WaiterState<V>>,
    pub(crate) condvar: Condvar,
}

impl<V> Waiter<V> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(WaiterState::Pending),
            condvar: Condvar::new(),
        }
    }
}

impl<V: Clone> Waiter<V> {
    /// Block until the waiter is resolved (completed or abandoned).
    ///
    /// Returns `Some(value)` if the loader completed successfully,
    /// or `None` if the loader abandoned the attempt.
    pub fn wait_for_result(&self) -> Option<V> {
        let mut state = self.state.lock();
        loop {
            match &*state {
                WaiterState::Pending => {
                    self.condvar.wait(&mut state);
                }
                WaiterState::Completed(v) => {
                    return Some(v.clone());
                }
                WaiterState::Abandoned => {
                    return None;
                }
            }
        }
    }
}

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

/// A guard that indicates the current thread is responsible for loading the
/// value for a given key.
///
/// Dropping the guard **without** calling [`insert`](CacheGuard::insert) will
/// mark the load as abandoned and wake any waiting threads so they can retry.
pub struct CacheGuard<'a, K: Copy + Hash + Eq, V, L, S> {
    pub(crate) cache: &'a Cache<K, V, L, S>,
    pub(crate) key: K,
    pub(crate) waiter: Arc<Waiter<V>>,
    pub(crate) completed: bool,
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
    pub fn insert(mut self, value: V) {
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
            *state = WaiterState::Completed(value);
            self.waiter.condvar.notify_all();
        }

        self.completed = true;
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
