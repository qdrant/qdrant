use std::path::PathBuf;
use std::sync::Arc;

use ahash::AHashMap;
use common::is_alive_lock::IsAliveLock;
use common::types::PointOffsetType;
use parking_lot::{Mutex, RwLock};

use super::dynamic_mmap_flags::DynamicMmapFlags;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};

/// A buffered wrapper around DynamicMmapFlags that provides manual flushing, without interface for reading.
///
/// Changes are buffered until explicitly flushed.
#[derive(Debug)]
pub(crate) struct BufferedDynamicFlags {
    /// Persisted flags.
    storage: Arc<Mutex<DynamicMmapFlags>>,

    /// Pending changes to the storage flags.
    buffer: Arc<RwLock<AHashMap<PointOffsetType, bool>>>,

    /// Lock to prevent concurrent flush and drop
    is_alive_flush_lock: IsAliveLock,
}

impl BufferedDynamicFlags {
    pub fn new(mmap_flags: DynamicMmapFlags) -> Self {
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
        self.storage.lock().clear_cache()?;
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.storage.lock().files()
    }

    pub fn flusher(&self) -> Flusher {
        // take pending changes
        let (updates, required_len) = {
            let mut buffer_guard = self.buffer.write();
            let updates = std::mem::take(&mut *buffer_guard);
            let Some(required_len) = updates.keys().max().map(|&max_id| max_id as usize + 1) else {
                return Box::new(|| Ok(()));
            };
            (updates, required_len)
        };

        // Weak reference to detect when the storage has been deleted
        let flags_arc = Arc::downgrade(&self.storage);
        let is_alive_flush_lock = self.is_alive_flush_lock.handle();

        Box::new(move || {
            let (Some(is_alive_flush_guard), Some(flags_arc)) =
                (is_alive_flush_lock.lock_if_alive(), flags_arc.upgrade())
            else {
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

            for (index, value) in updates {
                flags_guard.set(index as usize, value);
            }

            flags_guard.flusher()()?;

            // Keep the guard till the end of the flush to prevent concurrent drop/flushes
            drop(is_alive_flush_guard);

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {

    use common::types::PointOffsetType;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use crate::common::flags::buffered_dynamic_flags::BufferedDynamicFlags;
    use crate::common::flags::dynamic_mmap_flags::DynamicMmapFlags;

    #[test]
    fn test_buffered_flags_growth_persistence() {
        let dir = tempfile::Builder::new()
            .prefix("buffered_flags_growth")
            .tempdir()
            .unwrap();

        // Start with smaller flags
        {
            let mut mmap_flags = DynamicMmapFlags::open(dir.path(), false).unwrap();
            mmap_flags.set_len(3).unwrap();
            mmap_flags.set(0, true);
            mmap_flags.set(2, true);
            mmap_flags.flusher()().unwrap();
        }

        // Grow and update with BufferedDynamicFlags
        {
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            let flags = buffered_flags.storage.lock();

            // Initial state should match
            assert_eq!(flags.count_flags(), 2);
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
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            assert_eq!(mmap_flags.len(), 9);

            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);
            let flags = buffered_flags.storage.lock();

            let expected_trues = vec![0, 1, 2, 5, 7];
            let actual_trues: Vec<_> = flags.iter_trues().collect();
            assert_eq!(actual_trues, expected_trues);

            assert_eq!(flags.count_flags(), 5);
            assert_eq!(flags.len() - flags.count_flags(), 4);
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
            let mut mmap_flags = DynamicMmapFlags::open(dir.path(), false).unwrap();
            mmap_flags.set_len(num_flags).unwrap();

            for (i, &value) in initial_flags.iter().enumerate() {
                mmap_flags.set(i, value);
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
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            // Verify initial state loaded correctly
            let initial_true_count = initial_flags.iter().filter(|&&b| b).count();
            assert_eq!(
                buffered_flags.storage.lock().count_flags(),
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
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            // Calculate expected final state
            let mut expected_state = initial_flags.clone();
            for &(index, value) in &updates {
                expected_state[index as usize] = value;
            }

            let expected_true_count = expected_state.iter().filter(|&&b| b).count();
            let flags = buffered_flags.storage.lock();
            assert_eq!(flags.count_flags(), expected_true_count);
            assert_eq!(flags.len(), num_flags);

            // Verify specific values for a sample
            for i in (0..num_flags).step_by(100) {
                let expected = expected_state[i];
                let actual = flags.get(i);
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
            let mmap_flags = DynamicMmapFlags::open(dir.path(), false).unwrap();
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
                let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
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
                let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
                let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

                for (i, &expected) in expected_state.iter().enumerate() {
                    let actual = buffered_flags.storage.lock().get(i);
                    assert_eq!(
                        actual, expected,
                        "Cycle {cycle_num}, index {i}: expected {expected}, got {actual}"
                    );
                }

                let expected_true_count = expected_state.iter().filter(|&&b| b).count();
                assert_eq!(
                    buffered_flags.storage.lock().count_flags(),
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
            let mmap_flags = DynamicMmapFlags::open(dir.path(), false).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            buffered_flags.buffer_set(0, true);

            let flusher = buffered_flags.flusher();
            flusher().unwrap();
        }

        // Verify single flag persisted
        {
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            let flags = buffered_flags.storage.lock();

            assert_eq!(flags.len(), 1);
            assert_eq!(flags.count_flags(), 1);
            assert_eq!(flags.len() - flags.count_flags(), 0);
            assert!(flags.get(0));

            let trues: Vec<_> = flags.iter_trues().collect();
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
            let mmap_flags = DynamicMmapFlags::open(dir.path(), false).unwrap();
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
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            let flags = buffered_flags.storage.lock();

            assert_eq!(flags.len(), 100001);
            assert_eq!(flags.count_flags(), 4);

            // Verify specific indices
            assert!(flags.get(0));
            assert!(flags.get(1000));
            assert!(flags.get(50000));
            assert!(flags.get(100000));

            // Verify some gaps are false
            assert!(!flags.get(500));
            assert!(!flags.get(25000));
            assert!(!flags.get(75000));

            let trues: Vec<_> = flags.iter_trues().collect();
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
            let mut mmap_flags = DynamicMmapFlags::open(dir.path(), false).unwrap();
            mmap_flags.set_len(10).unwrap();
            for i in 0..10 {
                mmap_flags.set(i, i % 2 == 0); // Even indices true
            }
            mmap_flags.flusher()().unwrap();
        }

        // Test overwriting existing flags multiple times
        {
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            // Initial state: [true, false, true, false, true, false, true, false, true, false]
            assert_eq!(buffered_flags.storage.lock().count_flags(), 5);

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
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            let flags = buffered_flags.storage.lock();

            assert_eq!(flags.count_flags(), 0);
            assert_eq!(flags.len() - flags.count_flags(), 10);

            for i in 0..10 {
                assert!(!flags.get(i), "Index {i} should be false");
            }

            let trues: Vec<_> = flags.iter_trues().collect();
            assert!(trues.is_empty());
        }
    }
}
