use std::mem;
use std::sync::Arc;

use ahash::AHashMap;
use memory::mmap_type::MmapSlice;
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
    pending_updates: Mutex<AHashMap<usize, T>>,
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
            pending_updates: Mutex::new(AHashMap::new()),
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
    T: 'static + Sync + Send,
{
    pub fn flusher(&self) -> Flusher {
        let pending_updates = mem::take(&mut *self.pending_updates.lock());
        let slice = self.mmap_slice.clone();
        Box::new(move || {
            let mut mmap_slice_write = slice.write();
            for (index, value) in pending_updates {
                mmap_slice_write[index] = value;
            }
            Ok(mmap_slice_write.flusher()()?)
        })
    }
}
