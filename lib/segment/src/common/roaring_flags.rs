use std::path::PathBuf;

use common::types::PointOffsetType;
use roaring::RoaringBitmap;

use crate::common::Flusher;
use crate::common::buffered_dynamic_flags::BufferedDynamicFlags;
use crate::common::operation_error::OperationResult;
use crate::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;

/// A buffered, growable, and persistent bitslice with fast in-memory roaring bitmap.
///
/// Use [`BitvecFlags`][1] if you need a reference to a bitslice.
///
/// Changes are buffered until explicitly flushed.
///
/// [1]: super::bitvec_flags::BitvecFlags
pub struct RoaringFlags {
    /// Buffered persisted flags.
    storage: BufferedDynamicFlags,

    /// In-memory bitmap of true flags.
    // Potential optimization: add a secondary bitmap for false values for faster iter_falses implementation.
    bitmap: RoaringBitmap,

    /// Total length of the flags, including the trailing ones which have been set to false
    len: usize,
}

impl RoaringFlags {
    pub fn new(mmap_flags: DynamicMmapFlags) -> Self {
        // load flags into memory
        let bitmap = RoaringBitmap::from_sorted_iter(mmap_flags.iter_trues())
            .expect("iter_trues iterates in sorted order");

        if let Err(err) = mmap_flags.clear_cache() {
            log::warn!("Failed to clear bitslice cache: {}", err);
        }

        Self {
            len: mmap_flags.len(),
            storage: BufferedDynamicFlags::new(mmap_flags),
            bitmap,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get_bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    pub fn get(&self, index: PointOffsetType) -> bool {
        self.bitmap.contains(index)
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

    /// Set the value of a flag at the given index.
    /// Returns the previous value of the flag.
    pub fn set(&mut self, index: PointOffsetType, value: bool) -> bool {
        // queue write in buffer
        self.storage.buffer_set(index, value);

        // update length if needed
        let index_usize = index as usize;
        if index_usize >= self.len {
            self.len = index_usize + 1;
        }

        // update bitmap
        if value {
            !self.bitmap.insert(index)
        } else {
            self.bitmap.remove(index)
        }
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

    use crate::common::roaring_flags::RoaringFlags;
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
            let mut roaring_flags = RoaringFlags::new(mmap_flags);

            // Set various flags - we'll set up to index 19 to have a length of 20
            for i in 16..20 {
                roaring_flags.set(i, false); // Ensure we have length 20
            }
            roaring_flags.set(0, true);
            roaring_flags.set(5, true);
            roaring_flags.set(10, true);
            roaring_flags.set(15, true);
            roaring_flags.set(7, false); // This should be no-op since default is false

            // Flush
            let flusher = roaring_flags.flusher();
            flusher().unwrap();
        }

        // Verify bitmap consistency after reload
        {
            let mmap_flags = DynamicMmapFlags::open(dir.path(), true).unwrap();
            let roaring_flags = RoaringFlags::new(mmap_flags);

            // Verify iteration consistency after reload
            let iter_trues: Vec<_> = roaring_flags.iter_trues().collect();

            // Verify expected values
            assert_eq!(iter_trues, vec![0, 5, 10, 15]);

            // Verify count consistency
            assert_eq!(roaring_flags.count_trues(), 4);
            assert_eq!(
                roaring_flags.count_falses(),
                roaring_flags.len() - roaring_flags.count_trues()
            );

            // Verify iteration covers all indices
            let all_trues: Vec<_> = roaring_flags.iter_trues().collect();
            let all_falses: Vec<_> = roaring_flags.iter_falses().collect();
            let mut all_indices = all_trues;
            all_indices.extend(all_falses);
            all_indices.sort();

            let expected_all: Vec<_> = (0..roaring_flags.len() as PointOffsetType).collect();
            assert_eq!(all_indices, expected_all);
        }
    }
}
