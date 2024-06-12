use crate::common::mmap_type::MmapSlice;
use crate::common::Flusher;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

pub struct MmapSliceBufferedUpdateWrapper<T>
where
    T: 'static,
{
    mmap_slice: Arc<RwLock<MmapSlice<T>>>,
    len: usize,
    pending_updates: Mutex<HashMap<usize, T>>,
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
            pending_updates: Mutex::new(HashMap::new()),
        }
    }

    /// Sets the item at `index` to `value` buffered.
    ///
    /// ## Panics
    /// Panics if the index is out of bounds.
    pub fn set(&self, index: usize, value: T) {
        assert!(index < self.len, "index {index} out of range: {}", self.len);
        *self.pending_updates.lock().get_mut(&index).unwrap() = value;
    }
}

impl<T> MmapSliceBufferedUpdateWrapper<T>
where
    T: 'static + Sync + Send,
{
    pub fn flusher(&self) -> Flusher {
        let pending_updates = mem::take(&mut *self.pending_updates.lock());
        let bitslice = self.mmap_slice.clone();
        Box::new(move || {
            let mut mmap_slice_write = bitslice.write();
            for (index, value) in pending_updates {
                mmap_slice_write[index] = value;
            }
            mmap_slice_write.flusher()()
        })
    }
}
