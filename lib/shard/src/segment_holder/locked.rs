use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};

use crate::segment_holder::SegmentHolder;

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

    pub fn acquire_updates_lock(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.updates_mutex.lock()
    }
}
