use std::collections::HashMap;
use std::sync::Arc;

use memory::mmap_type::MmapSlice;
use parking_lot::{Mutex, RwLock};

use crate::common::Flusher;

/// A wrapper around `MmapSlice` that delays writing changes to the underlying file until they get
/// flushed manually.
/// This expects the underlying MmapSlice not to grow in size.
#[derive(Debug)]
pub struct MmapSliceBufferedUpdateWrapper<T>
where
    T: 'static,
{
    mmap_slice: Arc<RwLock<MmapSlice<T>>>,
    len: usize,
    pending_updates: Arc<Mutex<HashMap<usize, T>>>,
}

impl<T> MmapSliceBufferedUpdateWrapper<T>
where
    T: 'static + PartialEq,
{
    pub fn new(mmap_slice: MmapSlice<T>) -> Self {
        let len = mmap_slice.len();
        Self {
            mmap_slice: Arc::new(RwLock::new(mmap_slice)),
            len,
            pending_updates: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Removes from `pending_updates` all results that are flushed.
    /// If values in `pending_updates` are changed, do not remove them.
    fn clear_flushed_updated(
        flushed: HashMap<usize, T>,
        pending_updates: Arc<Mutex<HashMap<usize, T>>>,
    ) {
        let mut pending_updates = pending_updates.lock();
        for (index, value) in flushed {
            if let Some(pending_value) = pending_updates.get(&index) {
                if *pending_value == value {
                    pending_updates.remove(&index);
                }
            }
        }
    }

    /// Sets the item at `index` to `value` buffered.
    ///
    /// ## Panics
    /// Panics if the index is out of bounds.
    pub fn set(&self, index: usize, value: T) {
        assert!(index < self.len, "index {index} out of range: {}", self.len);
        self.pending_updates.lock().insert(index, value);
    }
}

impl<T> MmapSliceBufferedUpdateWrapper<T>
where
    T: 'static + Sync + Send + Clone + PartialEq,
{
    pub fn flusher(&self) -> Flusher {
        let pending_updates = self.pending_updates.lock().clone();
        let slice = self.mmap_slice.clone();
        let pending_updates_arc = self.pending_updates.clone();

        Box::new(move || {
            let mut mmap_slice_write = slice.write();
            for (index, value) in pending_updates.iter() {
                mmap_slice_write[*index] = value.clone();
            }
            mmap_slice_write.flusher()()?;
            Self::clear_flushed_updated(pending_updates, pending_updates_arc);
            Ok(())
        })
    }
}
