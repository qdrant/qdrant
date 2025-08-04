use std::path::PathBuf;
use std::sync::Arc;

use ahash::AHashMap;
use common::types::PointOffsetType;
use parking_lot::{Mutex, RwLock};
use roaring::RoaringBitmap;

use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;

/// A buffered wrapper around DynamicMmapFlags that provides manual flushing and fast in-memory reads.
///
/// This provides a growable persistent "bitslice" which keeps true values in memory.
/// Use [`MmapBitSliceBufferedUpdateWrapper`][1] if you don't need appending functionality, nor fast iteration.
///
/// Changes are buffered until explicitly flushed.
///
/// [1]: super::mmap_bitslice_buffered_update_wrapper::MmapBitSliceBufferedUpdateWrapper
pub struct BufferedDynamicFlags {
    /// Persisted flags.
    storage: Arc<Mutex<DynamicMmapFlags>>,

    /// Pending changes to the storage flags.
    buffer: Arc<RwLock<AHashMap<PointOffsetType, bool>>>,

    /// In-memory bitmap of true flags.
    // Potential optimization: add a secondary bitmap for false values for faster iter_falses implementation.
    bitmap: RoaringBitmap,

    /// Total length of the flags, including the trailing ones which have been set to false
    len: usize,
}

impl BufferedDynamicFlags {
    pub fn new(mmap_flags: DynamicMmapFlags) -> Self {
        // load flags into memory
        let bitmap = RoaringBitmap::from_sorted_iter(mmap_flags.iter_trues())
            .expect("iter_trues iterates in sorted order");

        let buffer = Arc::new(RwLock::new(AHashMap::new()));

        Self {
            len: mmap_flags.len(),
            storage: Arc::new(Mutex::new(mmap_flags)),
            buffer,
            bitmap,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get(&self, index: PointOffsetType) -> bool {
        self.bitmap.contains(index)
    }

    #[cfg(test)]
    pub fn buffer_is_empty(&self) -> bool {
        self.buffer.read().is_empty()
    }

    pub fn iter_trues(&self) -> impl Iterator<Item = PointOffsetType> {
        self.bitmap.iter()
    }

    pub fn iter_falses(&self) -> impl Iterator<Item = PointOffsetType> {
        // potential optimization:
        //      Create custom iterator which leverages bitmap's iterator for knowing ranges where the flags are false.
        //      This will help by not checking the bitmap for indices that are already known to be false.
        (0..self.len as PointOffsetType).filter(|&i| !self.bitmap.contains(i))
    }

    pub fn count_trues(&self) -> usize {
        self.bitmap.len() as usize
    }

    pub fn count_falses(&self) -> usize {
        self.len.saturating_sub(self.count_trues())
    }

    pub fn set(&mut self, index: PointOffsetType, value: bool) {
        // queue write in buffer
        self.buffer.write().insert(index, value);

        // update cached length if needed
        let index_usize = index as usize;
        if index_usize >= self.len {
            self.len = index_usize + 1;
        }

        // update bitmap
        if value {
            self.bitmap.insert(index);
        } else {
            self.bitmap.remove(index);
        }
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
            let required_len = self.len();
            (updates, required_len)
        };

        let flags_arc = self.storage.clone();
        Box::new(move || {
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

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {

    use common::types::PointOffsetType;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use crate::common::buffered_dynamic_flags::BufferedDynamicFlags;
    use crate::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;

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
            let mut buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            // Initial state should match
            assert_eq!(buffered_flags.count_trues(), 2);
            assert_eq!(buffered_flags.len(), 3);

            // Set flags beyond current length - this should grow the cached length
            buffered_flags.set(5, true);
            buffered_flags.set(7, true);
            buffered_flags.set(1, true); // Update existing

            // For this test, we need to simulate growth by setting flags beyond current length
            // The flusher will handle the growth when it's called

            // Verify in-memory state
            assert_eq!(buffered_flags.count_trues(), 5); // 0, 1, 2, 5, 7
            assert_eq!(buffered_flags.count_falses(), 3); // 3, 4, 6

            // Flush changes
            let flusher = buffered_flags.flusher();
            flusher().unwrap();
        }

        // Verify growth persisted
        {
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            assert_eq!(mmap_flags.len(), 8);

            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            let expected_trues = vec![0, 1, 2, 5, 7];
            let actual_trues: Vec<_> = buffered_flags.iter_trues().collect();
            assert_eq!(actual_trues, expected_trues);

            assert_eq!(buffered_flags.count_trues(), 5);
            assert_eq!(buffered_flags.count_falses(), 3);
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
            let mut buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            // Verify initial state loaded correctly
            let initial_true_count = initial_flags.iter().filter(|&&b| b).count();
            assert_eq!(buffered_flags.count_trues(), initial_true_count);

            // Apply updates
            for &(index, value) in &updates {
                buffered_flags.set(index, value);
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
            assert_eq!(buffered_flags.count_trues(), expected_true_count);
            assert_eq!(
                buffered_flags.count_falses(),
                num_flags - expected_true_count
            );

            // Verify specific values for a sample
            for i in (0..num_flags).step_by(100) {
                let expected = expected_state[i];
                let actual = buffered_flags.get(i as PointOffsetType);
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
                let mut buffered_flags = BufferedDynamicFlags::new(mmap_flags);

                // The flusher will handle length expansion as needed

                for &(index, value) in updates {
                    buffered_flags.set(index, value);
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
                    let actual = buffered_flags.get(i as PointOffsetType);
                    assert_eq!(
                        actual, expected,
                        "Cycle {cycle_num}, index {i}: expected {expected}, got {actual}"
                    );
                }

                let expected_true_count = expected_state.iter().filter(|&&b| b).count();
                assert_eq!(buffered_flags.count_trues(), expected_true_count);
            }
        }
    }

    #[test]
    fn test_buffered_flags_bitmap_consistency_after_persistence() {
        let dir = tempfile::Builder::new()
            .prefix("buffered_flags_consistency")
            .tempdir()
            .unwrap();

        // Create and update flags
        {
            let mmap_flags = DynamicMmapFlags::open(dir.path(), false).unwrap();
            let mut buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            // Set various flags - we'll set up to index 19 to have a length of 20
            for i in 16..20 {
                buffered_flags.set(i, false); // Ensure we have length 20
            }
            buffered_flags.set(0, true);
            buffered_flags.set(5, true);
            buffered_flags.set(10, true);
            buffered_flags.set(15, true);
            buffered_flags.set(7, false); // This should be no-op since default is false

            // Verify iteration consistency before flush
            let iter_trues: Vec<_> = buffered_flags.iter_trues().collect();
            let expected_trues = vec![0, 5, 10, 15];
            assert_eq!(iter_trues, expected_trues);

            // Flush
            let flusher = buffered_flags.flusher();
            flusher().unwrap();
        }

        // Verify bitmap consistency after reload
        {
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            // Verify iteration consistency after reload
            let iter_trues: Vec<_> = buffered_flags.iter_trues().collect();

            // Verify expected values
            assert_eq!(iter_trues, vec![0, 5, 10, 15]);

            // Verify count consistency
            assert_eq!(buffered_flags.count_trues(), 4);
            assert_eq!(
                buffered_flags.count_falses(),
                buffered_flags.len() - buffered_flags.count_trues()
            );

            // Verify iteration covers all indices
            let all_trues: Vec<_> = buffered_flags.iter_trues().collect();
            let all_falses: Vec<_> = buffered_flags.iter_falses().collect();
            let mut all_indices = all_trues;
            all_indices.extend(all_falses);
            all_indices.sort();

            let expected_all: Vec<_> = (0..buffered_flags.len() as PointOffsetType).collect();
            assert_eq!(all_indices, expected_all);
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
            let mut buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            buffered_flags.set(0, true);

            assert_eq!(buffered_flags.len(), 1);
            assert_eq!(buffered_flags.count_trues(), 1);
            assert_eq!(buffered_flags.count_falses(), 0);
            assert!(buffered_flags.get(0));

            let flusher = buffered_flags.flusher();
            flusher().unwrap();
        }

        // Verify single flag persisted
        {
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            assert_eq!(buffered_flags.len(), 1);
            assert_eq!(buffered_flags.count_trues(), 1);
            assert_eq!(buffered_flags.count_falses(), 0);
            assert!(buffered_flags.get(0));

            let trues: Vec<_> = buffered_flags.iter_trues().collect();
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
            let mut buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            // Set flags at sparse indices
            buffered_flags.set(0, true);
            buffered_flags.set(1000, true);
            buffered_flags.set(50000, true);
            buffered_flags.set(100000, true);

            assert_eq!(buffered_flags.len(), 100001);
            assert_eq!(buffered_flags.count_trues(), 4);
            assert_eq!(buffered_flags.count_falses(), 100001 - 4);

            let flusher = buffered_flags.flusher();
            flusher().unwrap();
        }

        // Verify sparse indices persisted correctly
        {
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            assert_eq!(buffered_flags.len(), 100001);
            assert_eq!(buffered_flags.count_trues(), 4);

            // Verify specific indices
            assert!(buffered_flags.get(0));
            assert!(buffered_flags.get(1000));
            assert!(buffered_flags.get(50000));
            assert!(buffered_flags.get(100000));

            // Verify some gaps are false
            assert!(!buffered_flags.get(500));
            assert!(!buffered_flags.get(25000));
            assert!(!buffered_flags.get(75000));

            let trues: Vec<_> = buffered_flags.iter_trues().collect();
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
            let mut buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            // Initial state: [true, false, true, false, true, false, true, false, true, false]
            assert_eq!(buffered_flags.count_trues(), 5);

            // First overwrite: flip all values
            for i in 0..10 {
                buffered_flags.set(i, i % 2 == 1); // Odd indices true
            }

            assert_eq!(buffered_flags.count_trues(), 5);

            // Second overwrite: set all to true
            for i in 0..10 {
                buffered_flags.set(i, true);
            }

            assert_eq!(buffered_flags.count_trues(), 10);
            assert_eq!(buffered_flags.count_falses(), 0);

            // Third overwrite: set all to false
            for i in 0..10 {
                buffered_flags.set(i, false);
            }

            assert_eq!(buffered_flags.count_trues(), 0);
            assert_eq!(buffered_flags.count_falses(), 10);

            let flusher = buffered_flags.flusher();
            flusher().unwrap();
        }

        // Verify final state (all false) persisted
        {
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            let buffered_flags = BufferedDynamicFlags::new(mmap_flags);

            assert_eq!(buffered_flags.count_trues(), 0);
            assert_eq!(buffered_flags.count_falses(), 10);

            for i in 0..10 {
                assert!(!buffered_flags.get(i), "Index {i} should be false");
            }

            let trues: Vec<_> = buffered_flags.iter_trues().collect();
            let falses: Vec<_> = buffered_flags.iter_falses().collect();
            assert!(trues.is_empty());
            assert_eq!(falses, (0..10).collect::<Vec<_>>());
        }
    }
}
