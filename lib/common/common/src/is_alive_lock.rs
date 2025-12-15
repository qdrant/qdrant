use std::sync::{Arc, Weak};

use parking_lot::{Mutex, MutexGuard};
use self_cell::self_cell;

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
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Waits for lock and marks as dead without dropping.
    /// Lock will no longer be usable after this.
    pub fn blocking_mark_dead(&self) {
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
        self.blocking_mark_dead();
    }
}

/// Handle for `IsAliveLock` which can access the lock at a later time.
///
/// This is a separate structure so it does not change the boolean on drop.
pub struct IsAliveHandle {
    inner: Weak<Mutex<bool>>,
}

impl IsAliveHandle {
    /// Get a guard of this lock if the parent hasn't been dropped
    #[must_use = "Guard must be held for lifetime of operation, abort if None is returned"]
    pub fn lock_if_alive(&self) -> Option<IsAliveGuard> {
        if let Some(mutex) = self.inner.upgrade() {
            // Since the Weak<Mutex> was upgraded, this is the safe way of keeping the mutex
            // alive while referencing it with the guard, and returning both.
            IsAliveGuardCell::try_new(mutex, |mutex| {
                let guard = mutex.lock();
                if *guard {
                    Ok(IsAliveGuardInner(guard))
                } else {
                    Err(())
                }
            })
            .ok()
            // hide implementation details with this newtype
            .map(IsAliveGuard)
        } else {
            None
        }
    }
}

/// Guards a `true` boolean
///
/// This is an opaque wrapper that hides the self_cell implementation details.
pub struct IsAliveGuard(#[expect(dead_code)] IsAliveGuardCell);

// Private self_cell implementation
self_cell!(
    struct IsAliveGuardCell {
        owner: Arc<Mutex<bool>>,

        #[covariant]
        dependent: IsAliveGuardInner,
    }
);
struct IsAliveGuardInner<'a>(#[expect(dead_code)] MutexGuard<'a, bool>);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_marking_dead() {
        let lock = IsAliveLock::new();
        let handle = lock.handle();

        assert!(handle.lock_if_alive().is_some());

        lock.blocking_mark_dead();

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
