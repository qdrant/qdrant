use std::sync::Arc;

use ahash::AHashMap;
use common::ext::BitSliceExt as _;
use memory::mmap_type::MmapBitSlice;
use parking_lot::{Mutex, RwLock};

use crate::common::Flusher;

/// A wrapper around `MmapBitSlice` that delays writing changes to the underlying file until they get
/// flushed manually.
/// This expects the underlying MmapBitSlice not to grow in size.
#[derive(Debug)]
pub struct MmapBitSliceBufferedUpdateWrapper {
    bitslice: Arc<RwLock<MmapBitSlice>>,
    len: usize,
    pending_updates: Arc<Mutex<AHashMap<usize, bool>>>,
    /// Lock to prevent concurrent flush and drop
    is_alive_flush_lock: Arc<Mutex<bool>>,
}

impl MmapBitSliceBufferedUpdateWrapper {
    pub fn new(bitslice: MmapBitSlice) -> Self {
        let len = bitslice.len();
        let is_alive_flush_lock = Arc::new(Mutex::new(true));
        Self {
            bitslice: Arc::new(RwLock::new(bitslice)),
            len,
            pending_updates: Arc::new(Mutex::new(AHashMap::new())),
            is_alive_flush_lock,
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

    pub fn get(&self, index: usize) -> Option<bool> {
        if index >= self.len {
            return None;
        }
        if let Some(value) = self.pending_updates.lock().get(&index) {
            Some(*value)
        } else {
            self.bitslice.read().get_bit(index)
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Removes from `pending_updates` all results that are flushed.
    /// If values in `pending_updates` are changed, do not remove them.
    fn clear_flushed_updates(
        flushed: AHashMap<usize, bool>,
        pending_updates: Arc<Mutex<AHashMap<usize, bool>>>,
    ) {
        pending_updates
            .lock()
            .retain(|point_id, a| flushed.get(point_id).is_none_or(|b| a != b));
    }

    pub fn flusher(&self) -> Flusher {
        let pending_updates = self.pending_updates.lock().clone();
        let bitslice = Arc::downgrade(&self.bitslice);
        let pending_updates_arc = Arc::downgrade(&self.pending_updates);
        let is_alive_flush_lock = self.is_alive_flush_lock.clone();

        Box::new(move || {
            // Keep the guard till the end of the flush to prevent concurrent drop/flushes
            let is_alive_flush_guard = is_alive_flush_lock.lock();

            if !*is_alive_flush_guard {
                // Already dropped, skip flush
                return Ok(());
            }

            let (Some(bitslice), Some(pending_updates_arc)) =
                (bitslice.upgrade(), pending_updates_arc.upgrade())
            else {
                log::debug!(
                    "Aborted flushing on a dropped MmapBitSliceBufferedUpdateWrapper instance"
                );
                return Ok(());
            };

            let mut mmap_slice_write = bitslice.write();
            for (index, value) in pending_updates.iter() {
                mmap_slice_write.set(*index, *value);
            }
            mmap_slice_write.flusher()()?;
            Self::clear_flushed_updates(pending_updates, pending_updates_arc);
            Ok(())
        })
    }
}

impl Drop for MmapBitSliceBufferedUpdateWrapper {
    fn drop(&mut self) {
        // Wait for all background flush operations to finish, and cancel future flushes
        *self.is_alive_flush_lock.lock() = false;
    }
}
