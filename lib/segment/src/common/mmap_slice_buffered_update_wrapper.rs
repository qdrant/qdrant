use std::sync::Arc;

use ahash::AHashMap;
use common::is_alive_lock::IsAliveLock;
use common::mmap::MmapSlice;
use common::types::PointOffsetType;
use parking_lot::{Mutex, RwLock};

use crate::common::Flusher;

/// A wrapper around `MmapSlice` that delays writing changes to the underlying file until they get
/// flushed manually.
/// This expects the underlying MmapSlice not to grow in size.
///
/// WARN: this structure is expected to be write-only.
#[derive(Debug)]
pub struct MmapSliceBufferedUpdateWrapper<T>
where
    T: 'static,
{
    mmap_slice: Arc<RwLock<MmapSlice<T>>>,
    len: usize,
    pending_updates: Arc<Mutex<AHashMap<PointOffsetType, T>>>,
    is_alive_lock: IsAliveLock,
}

impl<T> MmapSliceBufferedUpdateWrapper<T>
where
    T: 'static,
{
    pub fn new(mmap_slice: MmapSlice<T>) -> Self {
        let len = mmap_slice.len();
        Self {
            mmap_slice: Arc::new(RwLock::new(mmap_slice)),
            len,
            pending_updates: Arc::new(Mutex::new(AHashMap::new())),
            is_alive_lock: IsAliveLock::new(),
        }
    }

    /// Sets the item at `index` to `value` buffered.
    ///
    /// ## Panics
    /// Panics if the index is out of bounds.
    pub fn set(&self, index: PointOffsetType, value: T) {
        assert!(
            (index as usize) < self.len,
            "index {index} out of range: {}",
            self.len
        );
        self.pending_updates.lock().insert(index, value);
    }
}

impl<T> MmapSliceBufferedUpdateWrapper<T>
where
    T: 'static + Sync + Send + Clone + PartialEq,
{
    pub fn flusher(&self) -> Flusher {
        let updates = {
            let updates_guard = self.pending_updates.lock();
            if updates_guard.is_empty() {
                return Box::new(|| Ok(()));
            }
            updates_guard.clone()
        };

        let pending_updates_weak = Arc::downgrade(&self.pending_updates);
        let slice = Arc::downgrade(&self.mmap_slice);
        let is_alive_handle = self.is_alive_lock.handle();
        Box::new(move || {
            let (Some(is_alive_guard), Some(pending_updates_arc), Some(slice)) = (
                is_alive_handle.lock_if_alive(),
                pending_updates_weak.upgrade(),
                slice.upgrade(),
            ) else {
                log::debug!(
                    "Aborted flushing on a dropped MmapSliceBufferedUpdateWrapper instance"
                );
                return Ok(());
            };

            let mut mmap_slice_write = slice.write();
            for (&index, value) in &updates {
                mmap_slice_write[index as usize] = value.clone();
            }
            mmap_slice_write.flusher()()?;

            // Keep the guard till here to prevent concurrent drop/flushes
            // We don't touch files from here on and can drop the alive guard
            drop(is_alive_guard);

            Self::reconcile_persisted_changes(&pending_updates_arc, updates);

            Ok(())
        })
    }

    /// Removes the persisted updates from the pending ones.
    fn reconcile_persisted_changes(
        pending: &Mutex<AHashMap<PointOffsetType, T>>,
        persisted: AHashMap<PointOffsetType, T>,
    ) {
        pending.lock().retain(|point_offset, pending_value| {
            persisted
                .get(point_offset)
                .is_none_or(|persisted_value| pending_value != persisted_value)
        });
    }
}
