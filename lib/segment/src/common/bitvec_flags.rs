use std::path::PathBuf;

use bitvec::slice::BitSlice;
use bitvec::vec::BitVec;
use common::types::PointOffsetType;

use crate::common::Flusher;
use crate::common::buffered_dynamic_flags::BufferedDynamicFlags;
use crate::common::operation_error::OperationResult;
use crate::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;

/// A buffered, growable, and persistent bitslice with fast in-memory roaring bitmap.
///
/// Use [`RoaringFlags`][1] if you need a reference to a bitmap.
///
/// Changes are buffered until explicitly flushed.
///
/// [1]: super::roaring_flags::RoaringFlags
pub struct BitvecFlags {
    /// Buffered persisted flags.
    storage: BufferedDynamicFlags,

    /// In-memory bitvec of true and false flags.
    bitvec: BitVec,

    /// Total length of the flags, including the trailing ones which have been set to false
    len: usize,
}

impl BitvecFlags {
    pub fn new(mmap_flags: DynamicMmapFlags) -> Self {
        // load flags into memory
        let bitvec = BitVec::from_bitslice(mmap_flags.get_bitslice());

        Self {
            len: mmap_flags.len(),
            storage: BufferedDynamicFlags::new(mmap_flags),
            bitvec,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get_bitslice(&self) -> &BitSlice {
        &self.bitvec
    }

    pub fn get(&self, index: PointOffsetType) -> bool {
        self.bitvec.get(index as usize).is_some_and(|bit| *bit)
    }

    pub fn iter_trues(&self) -> impl Iterator<Item = PointOffsetType> {
        self.bitvec
            .iter_ones()
            .map(|index| index as PointOffsetType)
    }

    pub fn iter_falses(&self) -> impl Iterator<Item = PointOffsetType> {
        self.bitvec
            .iter_zeros()
            .map(|index| index as PointOffsetType)
    }

    pub fn count_trues(&self) -> usize {
        self.bitvec.count_ones()
    }

    pub fn count_falses(&self) -> usize {
        self.bitvec.count_zeros()
    }

    /// Set the value of a flag at the given index.
    /// Returns the previous value of the flag.
    pub fn set(&mut self, index: PointOffsetType, value: bool) {
        // queue write in buffer
        self.storage.buffer_set(index, value);

        // update length if needed
        let index_usize = index as usize;
        if index_usize >= self.len {
            self.len = index_usize + 1;
            self.bitvec.resize_with(self.len, |_| false);
        }

        // update bitmap
        self.bitvec.set(index_usize, value)
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache()?;
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    pub fn flusher(&self) -> Flusher {
        self.storage.flusher()
    }
}

#[cfg(test)]
mod tests {
    use common::types::PointOffsetType;

    use crate::common::bitvec_flags::BitvecFlags;
    use crate::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;

    #[test]
    fn test_roaring_flags_consistency_after_persistence() {
        let dir = tempfile::Builder::new()
            .prefix("roaring_flags_consistency")
            .tempdir()
            .unwrap();

        // Create and update flags
        {
            let mmap_flags = DynamicMmapFlags::open(dir.path(), false).unwrap();
            let mut bitvec_flags = BitvecFlags::new(mmap_flags);

            // Set various flags - we'll set up to index 19 to have a length of 20
            for i in 16..20 {
                bitvec_flags.set(i, false); // Ensure we have length 20
            }
            bitvec_flags.set(0, true);
            bitvec_flags.set(5, true);
            bitvec_flags.set(10, true);
            bitvec_flags.set(15, true);
            bitvec_flags.set(7, false); // This should be no-op since default is false

            // Verify iteration consistency after reload
            let iter_trues: Vec<_> = bitvec_flags.iter_trues().collect();

            // Verify expected values
            assert_eq!(iter_trues, vec![0, 5, 10, 15]);

            // Verify count consistency
            assert_eq!(bitvec_flags.count_trues(), 4);

            // Flush
            let flusher = bitvec_flags.flusher();
            flusher().unwrap();
        }

        // Verify bitmap consistency after reload
        {
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            let bitvec_flags = BitvecFlags::new(mmap_flags);

            // Verify iteration consistency after reload
            let iter_trues: Vec<_> = bitvec_flags.iter_trues().collect();

            // Verify expected values
            assert_eq!(iter_trues, vec![0, 5, 10, 15]);

            // Verify count consistency
            assert_eq!(bitvec_flags.count_trues(), 4);
            assert_eq!(
                bitvec_flags.count_falses(),
                bitvec_flags.len() - bitvec_flags.count_trues()
            );

            // Verify iteration covers all indices
            let all_trues: Vec<_> = bitvec_flags.iter_trues().collect();
            let all_falses: Vec<_> = bitvec_flags.iter_falses().collect();
            let mut all_indices = all_trues;
            all_indices.extend(all_falses);
            all_indices.sort();

            let expected_all: Vec<_> = (0..bitvec_flags.len() as PointOffsetType).collect();
            assert_eq!(all_indices, expected_all);
        }
    }
}
