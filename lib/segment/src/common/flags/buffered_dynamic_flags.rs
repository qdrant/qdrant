use std::path::PathBuf;
use std::sync::Arc;

use ahash::AHashMap;
use common::is_alive_lock::IsAliveLock;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};

use super::dynamic_stored_flags::DynamicStoredFlags;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};

/// A buffered wrapper around DynamicMmapFlags that provides manual flushing, without interface for reading.
///
/// Changes are buffered until explicitly flushed.
#[derive(Debug)]
pub(crate) struct BufferedDynamicFlags {
    /// Persisted flags.
    storage: Arc<Mutex<DynamicStoredFlags<MmapFile>>>,

    /// Pending changes to the storage flags.
    buffer: Arc<RwLock<AHashMap<PointOffsetType, bool>>>,

    /// Lock to prevent concurrent flush and drop
    is_alive_flush_lock: IsAliveLock,
}

impl BufferedDynamicFlags {
    pub fn new(mmap_flags: DynamicStoredFlags<MmapFile>) -> Self {
        let buffer = Arc::new(RwLock::new(AHashMap::new()));
        let is_alive_flush_lock = IsAliveLock::new();
        Self {
            storage: Arc::new(Mutex::new(mmap_flags)),
            buffer,
            is_alive_flush_lock,
        }
    }

    pub fn buffer_set(&self, index: PointOffsetType, value: bool) {
        // queue write in buffer
        self.buffer.write().insert(index, value);
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            storage,
            buffer: _,
            is_alive_flush_lock: _,
        } = self;
        storage.lock().clear_cache()?;
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.storage.lock().files()
    }

    pub fn flusher(&self) -> Flusher {
        let updates = {
            let buffer_guard = self.buffer.read();
            if buffer_guard.is_empty() {
                return Box::new(|| Ok(()));
            }
            buffer_guard.clone()
        };

        let Some(required_len) = updates.keys().max().map(|&max_id| max_id as usize + 1) else {
            return Box::new(|| Ok(()));
        };

        // Weak reference to detect when the storage has been deleted
        let flags_weak = Arc::downgrade(&self.storage);
        let buffer_weak = Arc::downgrade(&self.buffer);
        let is_alive_flush_lock = self.is_alive_flush_lock.handle();

        Box::new(move || {
            let (Some(is_alive_flush_guard), Some(flags_arc), Some(buffer_arc)) = (
                is_alive_flush_lock.lock_if_alive(),
                flags_weak.upgrade(),
                buffer_weak.upgrade(),
            ) else {
                log::trace!("BufferedDynamicFlags was dropped, cancelling flush");
                return Err(OperationError::cancelled(
                    "Aborted flushing on a dropped BufferedDynamicFlags instance",
                ));
            };

            // lock for the entire flushing process
            let mut flags_guard = flags_arc.lock();

            // resize if needed
            if required_len > flags_guard.len() {
                flags_guard.set_len(required_len)?;
            }

            flags_guard.set_ascending_bits(
                updates
                    .iter()
                    .map(|(index, value)| (u64::from(*index), *value))
                    .sorted_by_key(|(index, _value)| *index),
            )?;

            flags_guard.flusher()()?;

            // Keep the guard till here to prevent concurrent drop/flushes
            // We don't touch files from here on and can drop the alive guard
            drop(is_alive_flush_guard);

            reconcile_persisted_buffer(&buffer_arc, updates);

            Ok(())
        })
    }
}

/// Removes from `buffer` all results that are flushed.
/// If values in `pending_updates` are changed, do not remove them.
fn reconcile_persisted_buffer(
    buffer: &RwLock<AHashMap<u32, bool>>,
    persisted: AHashMap<u32, bool>,
) {
    buffer
        .write()
        .retain(|point_id, a| persisted.get(point_id).is_none_or(|b| a != b));
}

#[cfg(test)]
mod tests {

    use common::types::PointOffsetType;
    use common::universal_io::MmapFile;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use crate::common::flags::buffered_dynamic_flags::BufferedDynamicFlags;
    use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;

    #[test]
    fn test_buffered_flags_growth_persistence() {
        let dir = tempfile::Builder::new()
            .prefix("buffered_flags_growth")
            .tempdir()
            .unwrap();

        // Start with smaller flags
        {
            let mut mmap_flags = DynamicStoredFlags::<MmapFile>::open(dir.path(), false).unwrap();
            mmap_flags.set_len(3).unwrap();
            mmap_flags.set(0, true).unwrap();
            mmap_flags.set(2, true).unwrap();
            mmap_flags.flusher()().unwrap();
        }

        // Grow and update with BufferedDynamicFlags
        {
            let mmap_flags = DynamicStoredFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            let flags = buffered_flags.storage.lock();

            // Initial state should match
            assert_eq!(flags.count_flags().unwrap(), 2);
            assert_eq!(flags.len(), 3);

            drop(flags);

            // Set flags beyond current length - this should grow the length
            buffered_flags.buffer_set(5, true);
            buffered_flags.buffer_set(7, true);
            buffered_flags.buffer_set(8, false); // Also grows on false flag.
            buffered_flags.buffer_set(1, true); // Update existing

            // For this test, we need to simulate growth by setting flags beyond current length
            // The flusher will handle the growth when it's called

            // Flush changes
            let flusher = buffered_flags.flusher();
            flusher().unwrap();
        }

        // Verify growth persisted
        {
            let mmap_flags = DynamicStoredFlags::open(dir.path(), true).unwrap();
            assert_eq!(mmap_flags.len(), 9);

            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);
            let flags = buffered_flags.storage.lock();

            let expected_trues = vec![0, 1, 2, 5, 7];
            let actual_trues: Vec<_> = flags.iter_trues().unwrap().collect();
            assert_eq!(actual_trues, expected_trues);

            assert_eq!(flags.count_flags().unwrap(), 5);
            assert_eq!(flags.len() - flags.count_flags().unwrap(), 4);
        }
    }

    #[test]
    fn test_buffered_flags_large_dataset_persistence() {
        let dir = tempfile::Builder::new()
            .prefix("buffered_flags_large")
            .tempdir()
            .unwrap();
        let num_flags = 1000000;
        let mut rng = StdRng::seed_from_u64(42);

        // Generate random initial state
        let initial_flags: Vec<bool> = (0..num_flags).map(|_| rng.random()).collect();

        // Create initial flags
        {
            let mut mmap_flags = DynamicStoredFlags::<MmapFile>::open(dir.path(), false).unwrap();
            mmap_flags.set_len(num_flags).unwrap();

            for (i, &value) in initial_flags.iter().enumerate() {
                mmap_flags.set(i, value).unwrap();
            }

            mmap_flags.flusher()().unwrap();
        }

        // Generate random updates
        let num_updates = 1000;
        let updates: Vec<(PointOffsetType, bool)> = (0..num_updates)
            .map(|_| {
                (
                    rng.random_range(0..num_flags) as PointOffsetType,
                    rng.random(),
                )
            })
            .collect();

        // Apply updates and flush
        {
            let mmap_flags = DynamicStoredFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            // Verify initial state loaded correctly
            let initial_true_count = initial_flags.iter().filter(|&&b| b).count();
            assert_eq!(
                buffered_flags.storage.lock().count_flags().unwrap(),
                initial_true_count
            );

            // Apply updates
            for &(index, value) in &updates {
                buffered_flags.buffer_set(index, value);
            }

            // Flush
            let flusher = buffered_flags.flusher();
            flusher().unwrap();
        }

        // Verify persistence
        {
            let mmap_flags = DynamicStoredFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            // Calculate expected final state
            let mut expected_state = initial_flags.clone();
            for &(index, value) in &updates {
                expected_state[index as usize] = value;
            }

            let expected_true_count = expected_state.iter().filter(|&&b| b).count();
            let flags = buffered_flags.storage.lock();
            assert_eq!(flags.count_flags().unwrap(), expected_true_count);
            assert_eq!(flags.len(), num_flags);

            // Verify specific values for a sample
            for i in (0..num_flags).step_by(100) {
                let expected = expected_state[i];
                let actual = flags.get(i).unwrap();
                assert_eq!(actual, expected, "Mismatch at index {i}");
            }
        }
    }

    #[test]
    fn test_buffered_flags_multiple_flush_cycles() {
        let dir = tempfile::Builder::new()
            .prefix("buffered_flags_cycles")
            .tempdir()
            .unwrap();

        // Initial empty state
        {
            let mmap_flags = DynamicStoredFlags::<MmapFile>::open(dir.path(), false).unwrap();
            mmap_flags.flusher()().unwrap();
        }

        let cycles = [
            vec![(0, true), (1, true), (2, false)],
            vec![(1, false), (3, true), (4, true)],
            vec![(0, false), (2, true), (5, true)],
        ];

        let mut expected_state = [false; 6];

        for (cycle_num, updates) in cycles.iter().enumerate() {
            // Apply updates and flush
            {
                let mmap_flags = DynamicStoredFlags::open(dir.path(), true).unwrap();
                let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

                // The flusher will handle length expansion as needed

                for &(index, value) in updates {
                    buffered_flags.buffer_set(index, value);
                    expected_state[index as usize] = value;
                }

                let flusher = buffered_flags.flusher();
                flusher().unwrap();
            }

            // Verify state after each cycle
            {
                let mmap_flags = DynamicStoredFlags::open(dir.path(), true).unwrap();
                let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

                for (i, &expected) in expected_state.iter().enumerate() {
                    let actual = buffered_flags.storage.lock().get(i).unwrap();
                    assert_eq!(
                        actual, expected,
                        "Cycle {cycle_num}, index {i}: expected {expected}, got {actual}"
                    );
                }

                let expected_true_count = expected_state.iter().filter(|&&b| b).count();
                assert_eq!(
                    buffered_flags.storage.lock().count_flags().unwrap(),
                    expected_true_count
                );
            }
        }
    }

    #[test]
    fn test_buffered_flags_single_element_persistence() {
        let dir = tempfile::Builder::new()
            .prefix("buffered_flags_single")
            .tempdir()
            .unwrap();

        // Test with single true flag
        {
            let mmap_flags = DynamicStoredFlags::open(dir.path(), false).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            buffered_flags.buffer_set(0, true);

            let flusher = buffered_flags.flusher();
            flusher().unwrap();
        }

        // Verify single flag persisted
        {
            let mmap_flags = DynamicStoredFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            let flags = buffered_flags.storage.lock();

            assert_eq!(flags.len(), 1);
            assert_eq!(flags.count_flags().unwrap(), 1);
            assert_eq!(flags.len() - flags.count_flags().unwrap(), 0);
            assert!(flags.get(0).unwrap());

            let trues: Vec<_> = flags.iter_trues().unwrap().collect();
            assert_eq!(trues, vec![0]);
        }
    }

    #[test]
    fn test_buffered_flags_sparse_indices_persistence() {
        let dir = tempfile::Builder::new()
            .prefix("buffered_flags_sparse")
            .tempdir()
            .unwrap();

        // Test with very sparse indices (large gaps)
        {
            let mmap_flags = DynamicStoredFlags::open(dir.path(), false).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            // Set flags at sparse indices
            buffered_flags.buffer_set(0, true);
            buffered_flags.buffer_set(1000, true);
            buffered_flags.buffer_set(50000, true);
            buffered_flags.buffer_set(100000, true);

            let flusher = buffered_flags.flusher();
            flusher().unwrap();
        }

        // Verify sparse indices persisted correctly
        {
            let mmap_flags = DynamicStoredFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            let flags = buffered_flags.storage.lock();

            assert_eq!(flags.len(), 100001);
            assert_eq!(flags.count_flags().unwrap(), 4);

            // Verify specific indices
            assert!(flags.get(0).unwrap());
            assert!(flags.get(1000).unwrap());
            assert!(flags.get(50000).unwrap());
            assert!(flags.get(100000).unwrap());

            // Verify some gaps are false
            assert!(!flags.get(500).unwrap());
            assert!(!flags.get(25000).unwrap());
            assert!(!flags.get(75000).unwrap());

            let trues: Vec<_> = flags.iter_trues().unwrap().collect();
            assert_eq!(trues, vec![0, 1000, 50000, 100000]);
        }
    }

    #[test]
    fn test_buffered_flags_overwrite_persistence() {
        let dir = tempfile::Builder::new()
            .prefix("buffered_flags_overwrite")
            .tempdir()
            .unwrap();

        // Create initial state
        {
            let mut mmap_flags = DynamicStoredFlags::<MmapFile>::open(dir.path(), false).unwrap();
            mmap_flags.set_len(10).unwrap();
            for i in 0..10 {
                mmap_flags.set(i, i % 2 == 0).unwrap(); // Even indices true
            }
            mmap_flags.flusher()().unwrap();
        }

        // Test overwriting existing flags multiple times
        {
            let mmap_flags = DynamicStoredFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            // Initial state: [true, false, true, false, true, false, true, false, true, false]
            assert_eq!(buffered_flags.storage.lock().count_flags().unwrap(), 5);

            // First overwrite: flip all values
            for i in 0..10 {
                buffered_flags.buffer_set(i, i % 2 == 1); // Odd indices true
            }

            // Second overwrite: set all to true
            for i in 0..10 {
                buffered_flags.buffer_set(i, true);
            }

            // Third overwrite: set all to false
            for i in 0..10 {
                buffered_flags.buffer_set(i, false);
            }

            let flusher = buffered_flags.flusher();
            flusher().unwrap();
        }

        // Verify final state (all false) persisted
        {
            let mmap_flags = DynamicStoredFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            let flags = buffered_flags.storage.lock();

            assert_eq!(flags.count_flags().unwrap(), 0);
            assert_eq!(flags.len() - flags.count_flags().unwrap(), 10);

            for i in 0..10 {
                assert!(!flags.get(i).unwrap(), "Index {i} should be false");
            }

            let trues: Vec<_> = flags.iter_trues().unwrap().collect();
            assert!(trues.is_empty());
        }
    }
}
