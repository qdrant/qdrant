use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::common::mmap_type::MmapBitSlice;
use crate::common::Flusher;

/// A wrapper around `MmapBitSlice` that delays writing changes to the underlying file until they get
/// flushed manually.
/// This expects the underlying MmapBitSlice not to grow in size.
pub struct MmapBitSliceBufferedUpdateWrapper {
    bitslice: Arc<RwLock<MmapBitSlice>>,
    len: usize,
    pending_updates: Mutex<HashMap<usize, bool>>,
}

impl MmapBitSliceBufferedUpdateWrapper {
    pub fn new(bitslice: MmapBitSlice) -> Self {
        let len = bitslice.len();
        Self {
            bitslice: Arc::new(RwLock::new(bitslice)),
            len,
            pending_updates: Mutex::new(HashMap::new()),
        }
    }

    /// Sets the bit at `index` to `value` buffered.
    ///
    /// ## Panics
    /// Panics if the index is out of bounds.
    pub fn set(&self, index: usize, value: bool) {
        assert!(index < self.len, "index {index} out of range: {}", self.len);
        self.pending_updates.lock().insert(index, value);
    }

    pub fn flusher(&self) -> Flusher {
        let pending_updates = mem::take(&mut *self.pending_updates.lock());
        let bitslice = self.bitslice.clone();
        Box::new(move || {
            let mut mmap_slice_write = bitslice.write();
            for (index, value) in pending_updates {
                mmap_slice_write.set(index, value);
            }
            mmap_slice_write.flusher()()
        })
    }
}
