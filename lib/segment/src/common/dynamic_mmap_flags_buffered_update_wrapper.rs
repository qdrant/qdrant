use std::sync::Arc;

use ahash::AHashMap;
use common::counter::referenced_counter::HwMetricRefCounter;
use parking_lot::RwLock;

use crate::common::Flusher;
use crate::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;

/// A wrapper around `DynamicMmapFlags` that delays writing changes to the underlying file until they get
/// flushed manually.
#[derive(Debug)]
pub struct DynamicMmapFlagsBufferedUpdateWrapper {
    flags: Arc<RwLock<DynamicMmapFlags>>,
    pending_updates: Arc<RwLock<AHashMap<u32, bool>>>,
    /// Cached length of the flags. Flags will be extended to this length when flushed.
    cached_len: usize,
}

impl DynamicMmapFlagsBufferedUpdateWrapper {
    pub fn new(flags: DynamicMmapFlags) -> Self {
        let initial_len = flags.len();
        Self {
            flags: Arc::new(RwLock::new(flags)),
            pending_updates: Arc::new(RwLock::new(AHashMap::new())),
            cached_len: initial_len,
        }
    }

    /// Sets the flag at `index` to `value` buffered.
    ///
    /// Returns the previous value of the flag (considering pending updates).
    pub fn set(&mut self, key: u32, value: bool, hw_counter_ref: HwMetricRefCounter) -> bool {
        let previous_value = self.get(key);
        self.pending_updates.write().insert(key, value);

        // Update cached length if this key extends beyond current length
        // even if it is setting the flag to `false`
        let new_required_len = key as usize + 1;
        self.cached_len = self.cached_len.max(new_required_len);

        hw_counter_ref.incr_delta(size_of::<bool>());
        previous_value
    }

    /// Gets the value of the flag at `key`, considering pending updates.
    pub fn get(&self, key: u32) -> bool {
        // Check pending updates first
        if let Some(value) = self.pending_updates.read().get(&key) {
            return *value;
        }

        // Fall back to the underlying flags
        self.flags.read().get(key)
    }

    pub fn len(&self) -> usize {
        self.cached_len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Count number of set flags, considering pending updates.
    pub fn count_flags(&self) -> usize {
        self.iter_trues().count()
    }

    /// Iterate over all "true" flags, considering pending updates.
    pub fn iter_trues(&self) -> impl Iterator<Item = u32> {
        let len = self.len();
        let flags = self.flags.read();
        let pending = self.pending_updates.read();

        let is_true = move |i| {
            if let Some(value) = pending.get(&i) {
                return *value;
            };

            // Fall back to the underlying flags
            flags.get(i)
        };

        (0..len as u32).filter_map(move |i| is_true(i).then_some(i))
    }

    /// Iterate over all "false" flags, considering pending updates.
    pub fn iter_falses(&self) -> impl Iterator<Item = u32> {
        let len = self.len();
        let flags = self.flags.read();
        let pending = self.pending_updates.read();

        let is_false = move |i| {
            if let Some(value) = pending.get(&i) {
                return !*value;
            };

            // Fall back to the underlying flags
            !flags.get(i)
        };

        (0..len as u32).filter_map(move |i| is_false(i).then_some(i))
    }

    /// Removes from `pending_updates` all results that are flushed.
    /// If values in `pending_updates` are changed, do not remove them.
    fn clear_flushed_updates(
        flushed: AHashMap<u32, bool>,
        pending_updates: Arc<RwLock<AHashMap<u32, bool>>>,
    ) {
        pending_updates
            .write()
            .retain(|point_id, a| flushed.get(point_id).is_none_or(|b| a != b));
    }

    pub fn flusher(&self) -> Flusher {
        let pending_updates_clone = self.pending_updates.read().clone();
        let flags = self.flags.clone();
        let pending_updates_arc = self.pending_updates.clone();
        let cached_len = self.cached_len;

        Box::new(move || {
            let mut flags_write = flags.write();

            // First, determine if we need to resize
            let required_len = cached_len as usize;
            if required_len > flags_write.len() {
                flags_write.set_len(required_len)?;
            }

            // Apply all pending updates
            for (index, value) in pending_updates_clone.iter() {
                flags_write.set(*index as usize, *value);
            }

            // Flush the underlying flags
            flags_write.flusher()()?;

            // Clear the flushed updates
            Self::clear_flushed_updates(pending_updates_clone, pending_updates_arc);

            Ok(())
        })
    }

    /// Get a reference to the underlying flags for read-only operations.
    /// Note: This does not consider pending updates.
    pub fn get_underlying_flags(&self) -> Arc<RwLock<DynamicMmapFlags>> {
        self.flags.clone()
    }
}

impl From<DynamicMmapFlags> for DynamicMmapFlagsBufferedUpdateWrapper {
    fn from(flags: DynamicMmapFlags) -> Self {
        Self::new(flags)
    }
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_buffered_updates() {
        let dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        let flags = DynamicMmapFlags::open(dir.path(), false).unwrap();
        let mut wrapper = DynamicMmapFlagsBufferedUpdateWrapper::new(flags);

        // Test basic set/get operations
        let hw_counter = HardwareCounterCell::disposable();
        let hw_ref = hw_counter.ref_payload_index_io_write_counter();
        assert!(!wrapper.set(0, true, hw_ref));
        assert!(wrapper.get(0));

        // Test that underlying flags don't have the update yet
        assert!(!wrapper.flags.read().get(0));

        // Flush and check
        wrapper.flusher()().unwrap();
        assert!(wrapper.flags.read().get(0));
        assert!(wrapper.get(0));
    }

    #[test]
    fn test_buffered_count() {
        let dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        let mut flags = DynamicMmapFlags::open(dir.path(), false).unwrap();
        flags.set_len(10).unwrap();
        let mut wrapper = DynamicMmapFlagsBufferedUpdateWrapper::new(flags);

        // Set some flags in buffer
        let hw_counter = HardwareCounterCell::disposable();
        let hw_ref = hw_counter.ref_payload_index_io_write_counter();
        wrapper.set(0, true, hw_ref);
        wrapper.set(5, true, hw_ref);
        wrapper.set(9, true, hw_ref);

        assert_eq!(wrapper.count_flags(), 3);

        // Flush and verify count is still correct
        wrapper.flusher()().unwrap();
        assert_eq!(wrapper.count_flags(), 3);
    }

    #[test]
    fn test_iter_trues() {
        let dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        let mut flags = DynamicMmapFlags::open(dir.path(), false).unwrap();
        flags.set_len(10).unwrap();
        let mut wrapper = DynamicMmapFlagsBufferedUpdateWrapper::new(flags);

        // Set some flags
        let hw_counter = HardwareCounterCell::disposable();
        let hw_ref = hw_counter.ref_payload_index_io_write_counter();
        wrapper.set(1, true, hw_ref);
        wrapper.set(3, true, hw_ref);
        wrapper.set(7, true, hw_ref);

        let mut trues: Vec<u32> = wrapper.iter_trues().collect();
        trues.sort();
        assert_eq!(trues, vec![1, 3, 7]);

        // Flush and verify iter_trues still works correctly
        wrapper.flusher()().unwrap();
        let mut trues_after_flush: Vec<u32> = wrapper.iter_trues().collect();
        trues_after_flush.sort();
        assert_eq!(trues_after_flush, vec![1, 3, 7]);
    }
}
