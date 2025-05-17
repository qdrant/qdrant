use std::sync::Arc;

use ahash::AHashMap;
use common::ext::BitSliceExt as _;
use memory::mmap_type::MmapBitSlice;
use parking_lot::{RwLock, RwLockReadGuard};

use crate::common::Flusher;

const RELEASE_LOCK_EVERY_N_GETS: usize = 100;

/// A wrapper around `MmapBitSlice` that delays writing changes to the underlying file until they get
/// flushed manually.
/// This expects the underlying MmapBitSlice not to grow in size.
#[derive(Debug)]
pub struct MmapBitSliceBufferedUpdateWrapper {
    state: Arc<RwLock<WrapperState>>,
    len: usize,
}

#[derive(Debug)]
struct WrapperState {
    bitslice: MmapBitSlice,
    pending_updates: AHashMap<usize, bool>,
}

pub struct MmapBitSliceBufferedUpdateReader<'a> {
    wrapper: &'a MmapBitSliceBufferedUpdateWrapper,
    state: Option<RwLockReadGuard<'a, WrapperState>>,
    len: usize,
    get_count: usize,
}

impl<'a> MmapBitSliceBufferedUpdateReader<'a> {
    pub fn new(wrapper: &'a MmapBitSliceBufferedUpdateWrapper) -> Self {
        let state = Some(wrapper.state.read());
        let len = wrapper.len;
        Self {
            wrapper,
            state,
            len,
            get_count: 0,
        }
    }

    pub fn get(&mut self, index: usize) -> Option<bool> {
        if index >= self.len {
            return None;
        }

        // Re-acquire locks every `RELEASE_LOCK_EVERY_N_GETS` get calls
        if self.get_count % RELEASE_LOCK_EVERY_N_GETS == 0 {
            self.state = Some(self.wrapper.state.read());
        }
        self.get_count += 1;

        let state = self.state.as_ref().unwrap();
        if let Some(value) = state.pending_updates.get(&index) {
            Some(*value)
        } else {
            state.bitslice.get_bit(index)
        }
    }
}

impl MmapBitSliceBufferedUpdateWrapper {
    pub fn new(bitslice: MmapBitSlice) -> Self {
        let len = bitslice.len();
        Self {
            state: Arc::new(RwLock::new(WrapperState {
                bitslice,
                pending_updates: AHashMap::new(),
            })),
            len,
        }
    }

    /// Sets the bit at `index` to `value` buffered.
    ///
    /// ## Panics
    /// Panics if the index is out of bounds.
    pub fn set(&self, index: usize, value: bool) {
        assert!(index < self.len, "index {index} out of range: {}", self.len);
        self.state.write().pending_updates.insert(index, value);
    }

    pub fn get(&self, index: usize) -> Option<bool> {
        if index >= self.len {
            return None;
        }
        let state = self.state.read();
        if let Some(value) = state.pending_updates.get(&index) {
            Some(*value)
        } else {
            state.bitslice.get_bit(index)
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
    fn clear_flushed_updates(flushed: AHashMap<usize, bool>, state: &mut WrapperState) {
        state
            .pending_updates
            .retain(|point_id, a| flushed.get(point_id).is_none_or(|b| a != b));
    }

    pub fn flusher(&self) -> Flusher {
        let state_arc = self.state.clone();

        Box::new(move || {
            let mut state = state_arc.write();
            let pending_updates = state.pending_updates.clone();

            for (index, value) in pending_updates.iter() {
                state.bitslice.set(*index, *value);
            }
            state.bitslice.flusher()()?;
            Self::clear_flushed_updates(pending_updates, &mut state);
            Ok(())
        })
    }
}
