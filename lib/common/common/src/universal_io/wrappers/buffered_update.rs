use std::sync::Arc;

use ahash::AHashMap;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};

use crate::is_alive_lock::IsAliveLock;
use crate::types::PointOffsetType;
use crate::universal_io::{
    Flusher, TypedStorage, UniversalIoError, UniversalRead as _, UniversalWrite,
};

/// A wrapper around `TypedStorage<S, T>` that delays writing changes to the underlying file until they get
/// flushed manually.
/// This expects the underlying storage not to grow in size.
///
/// WARN: this structure is expected to be write-only.
#[derive(Debug)]
pub struct SliceBufferedUpdateWrapper<S: UniversalWrite<T>, T: Copy>
where
    T: 'static,
{
    slice: Arc<RwLock<TypedStorage<S, T>>>,
    len: u64,
    pending_updates: Arc<Mutex<AHashMap<PointOffsetType, T>>>,
    is_alive_lock: IsAliveLock,
}

impl<S: UniversalWrite<T>, T: Copy> SliceBufferedUpdateWrapper<S, T>
where
    T: 'static,
{
    pub fn new(slice_storage: TypedStorage<S, T>) -> Result<Self, UniversalIoError> {
        let len = slice_storage.len()?;
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

impl<S: UniversalWrite<T> + Send + Sync + 'static, T: Copy> SliceBufferedUpdateWrapper<S, T>
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

            // Coalesce consecutive indices into the same run
            let mut prev_element: Option<u32> = None;
            let mut run_start = 0u32;
            let runs =
                updates
                    .iter()
                    .sorted_by_key(|(index, _)| *index)
                    .chunk_by(move |(index, _)| {
                        let index = **index;
                        if prev_element.is_none_or(|prev| index != prev + 1) {
                            run_start = index;
                        }
                        prev_element = Some(index);
                        run_start
                    });

            // Write runs in batch
            let offset_data = runs
                .into_iter()
                .map(|(run_start, run_iter)| {
                    let byte_start = u64::from(run_start) * std::mem::size_of::<T>() as u64;
                    let values = run_iter.map(|(_, &value)| value).collect_vec();

                    (byte_start, values)
                })
                .collect_vec();
            // Unfortunately, we need to collect again so that the values are taken as a slice for write_batch
            let offset_data_refs: Vec<(u64, &[T])> = offset_data
                .iter()
                .map(|(offset, values)| (*offset, values.as_slice()))
                .collect();
            slice_guard.write_batch(offset_data_refs)?;

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
