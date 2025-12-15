use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};

/// Structure which ensures that the lock is alive at the time of locking,
/// and will prevent dropping while guarded.
///
/// Dropping this structure will also mark as dead, preventing future access through any dangling
/// handles.
///
/// This structure explicitly doesn't implement Clone, so that `handle` is used instead.
#[derive(Debug)]
pub struct IsAliveLock {
    inner: Arc<Mutex<bool>>,
}

impl IsAliveLock {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(true)),
        }
    }

    /// Get a handle for this lock.
    pub fn handle(&self) -> IsAliveHandle {
        IsAliveHandle {
            inner: self.inner.clone(),
        }
    }

    /// Waits for lock and marks as dead without dropping.
    /// Lock will no longer be usable after this.
    pub fn mark_dead(&self) {
        *self.inner.lock() = false
    }
}

impl Default for IsAliveLock {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for IsAliveLock {
    fn drop(&mut self) {
        // prevent dangling handles from accessing the lock
        self.mark_dead();
    }
}

/// Handle for `IsAliveLock` which can access the lock at a later time.
///
/// This is a separate structure so it does not change the boolean on drop.
pub struct IsAliveHandle {
    inner: Arc<Mutex<bool>>,
}

impl IsAliveHandle {
    /// Get a guard of this lock if the parent hasn't been dropped
    pub fn lock_if_alive(&self) -> Option<IsAliveGuard<'_>> {
        let guard = self.inner.lock();
        guard.then_some(IsAliveGuard(guard))
    }
}

/// Guards a `true` boolean
#[expect(dead_code)]
pub struct IsAliveGuard<'a>(MutexGuard<'a, bool>);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_marking_dead() {
        let lock = IsAliveLock::new();
        let handle = lock.handle();

        assert!(handle.lock_if_alive().is_some());

        lock.mark_dead();

        assert!(handle.lock_if_alive().is_none());
    }

    #[test]
    fn test_dropping() {
        let lock = IsAliveLock::default();
        let handle = lock.handle();

        assert!(handle.lock_if_alive().is_some());

        // dropping the handle does not poison the lock
        drop(handle);
        let handle = lock.handle();
        assert!(handle.lock_if_alive().is_some());

        // dropping the parent poisons the lock
        drop(lock);
        assert!(handle.lock_if_alive().is_none());
    }

    #[test]
    fn test_parent_waits_for_guard() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let lock = IsAliveLock::new();
        let handle = lock.handle();

        // Test following sequence
        // | tick | lock           | handle        |
        // | ---- | -------------  | ------        |
        // |   0  |                | guarded       |
        // |   1  | drop (waiting) | guarded       |
        // |   2  |      (waiting) | guarded       |
        // |   3  | actual drop    | release guard |
        let tick = AtomicUsize::new(0);

        // Hold the guard
        let guard = handle.lock_if_alive().unwrap();
        std::thread::scope(|s| {
            s.spawn(|| {
                // Start dropping until tick 1
                while tick.load(Ordering::SeqCst) < 1 {
                    std::thread::yield_now();
                }
                // This should block until guard is dropped (at tick 3)
                drop(lock);
                // Verify we dropped after guard was released (tick == 3)
                assert!(tick.load(Ordering::SeqCst) >= 3);
            });

            tick.store(1, Ordering::SeqCst);

            // Advance tick to show we're about to drop the guard
            tick.store(2, Ordering::SeqCst);
            tick.store(3, Ordering::SeqCst);
            drop(guard);
        });

        // After parent is dropped, handle should return None
        assert!(handle.lock_if_alive().is_none());
    }
}
