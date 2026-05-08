use std::sync::Arc;

use ahash::AHashMap;
use parking_lot::{Mutex, RwLock};

use crate::is_alive_lock::IsAliveLock;
use crate::types::PointOffsetType;
use crate::universal_io::{Flusher, UniversalIoError, UniversalWrite};

/// A wrapper around [`UniversalWrite`] that delays writing changes to the
/// underlying file until they get flushed manually.
/// This expects the underlying storage not to grow in size.
///
/// WARN: this structure is expected to be write-only.
#[derive(Debug)]
pub struct SliceBufferedUpdateWrapper<S, T>
where
    S: UniversalWrite,
    T: bytemuck::Pod,
{
    slice: Arc<RwLock<S>>,
    len: u64,
    pending_updates: Arc<Mutex<AHashMap<PointOffsetType, T>>>,
    is_alive_lock: IsAliveLock,
}

impl<S, T> SliceBufferedUpdateWrapper<S, T>
where
    S: UniversalWrite,
    T: bytemuck::Pod,
{
    pub fn new(slice_storage: S) -> Result<Self, UniversalIoError> {
        let len = slice_storage.len::<T>()?;
        Ok(Self {
            slice: Arc::new(RwLock::new(slice_storage)),
            len,
            pending_updates: Arc::new(Mutex::new(AHashMap::new())),
            is_alive_lock: IsAliveLock::new(),
        })
    }

    /// Sets the item at `index` to `value` buffered.
    ///
    /// ## Panics
    /// Panics if the index is out of bounds.
    pub fn set(&self, index: PointOffsetType, value: T) {
        assert!(
            u64::from(index) < self.len,
            "index {index} out of range: {}",
            self.len
        );
        self.pending_updates.lock().insert(index, value);
    }
}

impl<S, T> SliceBufferedUpdateWrapper<S, T>
where
    S: UniversalWrite + Send + Sync + 'static,
    T: bytemuck::Pod + PartialEq + Sync + Send,
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
        let slice = Arc::downgrade(&self.slice);
        let is_alive_handle = self.is_alive_lock.handle();
        Box::new(move || {
            let (Some(is_alive_guard), Some(pending_updates_arc), Some(slice)) = (
                is_alive_handle.lock_if_alive(),
                pending_updates_weak.upgrade(),
                slice.upgrade(),
            ) else {
                log::debug!("Aborted flushing on a dropped SliceBufferedUpdateWrapper instance");
                return Ok(());
            };

            let mut slice_guard = slice.write();

            // Coalesce contiguous updates into the same write
            let mut items: Vec<(PointOffsetType, T)> =
                updates.iter().map(|(k, v)| (*k, *v)).collect();
            items.sort_by_key(|(index, _)| *index);

            let all_values: Vec<T> = items.iter().map(|(_, value)| *value).collect();

            // Batch writes
            let mut remaining_values: &[T] = &all_values[..];
            let it = items.chunk_by(|(a, _), (b, _)| *a + 1 == *b).map(|chunk| {
                let elem_start: PointOffsetType = chunk[0].0;
                let byte_start = u64::from(elem_start) * size_of::<T>() as u64;
                let chunk_values = remaining_values
                    .split_off(..chunk.len())
                    .expect("`chunk.len()` is sourced from the same slice as `remaining_values`");
                (byte_start, chunk_values)
            });
            slice_guard.write_batch(it)?;

            slice_guard.flusher()()?;

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
