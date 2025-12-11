use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};

/// Structure which ensures that the lock is alive at the time of locking,
/// and will prevent dropping while guarded.
///
/// This structure explicitly doesn't implement Clone, so that `handle` is used instead.
#[derive(Debug, Default)]
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

    /// Waits for lock and poisons without dropping.
    /// Lock will no longer be usable after this.
    pub fn poison(&self) {
        *self.inner.lock() = false
    }
}

impl Drop for IsAliveLock {
    fn drop(&mut self) {
        // prevent dangling handles from accessing the lock
        *self.inner.lock() = false;
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
