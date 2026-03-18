use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};

use crate::segment_holder::SegmentHolder;

/// A guard that guarantees no update operations are happening.
///
/// This is a newtype wrapper around `parking_lot::MutexGuard<'_, ()>` that provides
/// semantic meaning: while this guard is held, no concurrent update operations can proceed.
/// This is used during critical sections like segment optimization finalization and snapshot
/// operations to ensure consistency.
#[allow(dead_code)] // Field is held for its RAII Drop behavior, not for reading
pub struct UpdatesGuard<'a>(parking_lot::MutexGuard<'a, ()>);

#[derive(Clone, Debug)]
pub struct LockedSegmentHolder {
    holder: Arc<RwLock<SegmentHolder>>,
    /// Lock that prevents update operations during segment maintenance.
    /// This lock should be external to the `holder` to prevent deadlocks.
    /// This lock doesn't wrap the whole `SegmentHolder` to allow read access to segments
    ///
    ///
    ///
    /// Currently used for:
    ///
    /// - Preventing update operations during finalization of segment optimizations
    ///
    updates_mutex: Arc<Mutex<()>>,
}

impl LockedSegmentHolder {
    pub fn new(segment_holder: SegmentHolder) -> Self {
        Self {
            holder: Arc::new(RwLock::new(segment_holder)),
            updates_mutex: Arc::new(Mutex::new(())),
        }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, SegmentHolder> {
        self.holder.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, SegmentHolder> {
        self.holder.write()
    }

    pub fn upgradable_read(&self) -> RwLockUpgradableReadGuard<'_, SegmentHolder> {
        self.holder.upgradable_read()
    }

    pub fn try_read_for(&self, timeout: Duration) -> Option<RwLockReadGuard<'_, SegmentHolder>> {
        self.holder.try_read_for(timeout)
    }

    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, SegmentHolder>> {
        self.holder.try_read()
    }

    // On update operation:
    // - Should be locked before read lock on `holder`. If we can't acquire this lock,
    //   we should not block resources for other operations.
    // - On other operations, while acquiring this lock, make sure that it doesn't prevent
    //   update operation. I.e. it allows read lock on `holder` while update lock is being waited on.
    pub fn acquire_updates_lock(&self) -> UpdatesGuard<'_> {
        UpdatesGuard(self.updates_mutex.lock())
    }
}
