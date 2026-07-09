use std::path::PathBuf;

use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UniversalWrite};
use roaring::RoaringBitmap;

use super::buffered_dynamic_flags::BufferedDynamicFlags;
use super::dynamic_stored_flags::DynamicStoredFlags;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;

/// Shared read-only surface over a roaring-bitmap-backed flag set.
///
/// Implemented by both the writable [`RoaringFlags`] and the read-only
/// [`ReadOnlyRoaringFlags`](super::read_only_roaring_flags::ReadOnlyRoaringFlags),
/// so query logic (filter / cardinality / condition checks) can be written
/// once and parameterized over either.
pub trait RoaringFlagsRead {
    /// Total length of the flags, including trailing falses.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Underlying in-memory roaring bitmap of "true" positions.
    ///
    /// Fallible because a read-only variant may only materialize the bitmap on
    /// first use, by scanning its backing file — see
    /// [`ReadOnlyRoaringFlags`](super::read_only_roaring_flags::ReadOnlyRoaringFlags).
    /// Once materialized the bitmap is cached, so repeated calls are free.
    fn get_bitmap(&self) -> OperationResult<&RoaringBitmap>;

    /// The bitmap if it is already in RAM, without materializing it.
    ///
    /// Only for callers that must not pay for (or cannot fail on) a scan —
    /// [`ram_usage_bytes`][Self::ram_usage_bytes] being the motivating one, for
    /// which an unmaterialized bitmap genuinely occupies nothing.
    fn bitmap_if_materialized(&self) -> Option<&RoaringBitmap>;

    fn get(&self, index: PointOffsetType) -> OperationResult<bool> {
        Ok(self.get_bitmap()?.contains(index))
    }

    fn iter_trues(&self) -> OperationResult<roaring::bitmap::Iter<'_>> {
        Ok(self.get_bitmap()?.iter())
    }

    fn iter_falses(&self) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        // potential optimization:
        //      Create custom iterator which leverages bitmap's iterator for knowing ranges where the flags are false.
        //      This will help by not checking the bitmap for indices that are already known to be false.
        let len = self.len() as PointOffsetType;
        let bitmap = self.get_bitmap()?;
        Ok(Box::new((0..len).filter(move |i| !bitmap.contains(*i))))
    }

    fn count_trues(&self) -> OperationResult<usize> {
        Ok(self.get_bitmap()?.len() as usize)
    }

    fn count_falses(&self) -> OperationResult<usize> {
        Ok(self.len().saturating_sub(self.count_trues()?))
    }

    /// RAM held by the in-memory bitmap. Zero while it is not materialized.
    fn ram_usage_bytes(&self) -> usize {
        self.bitmap_if_materialized()
            .map_or(0, |bitmap| bitmap.serialized_size())
    }

    /// Fill RAM cache with the backing file pages.
    ///
    /// Default: no-op (the bitmap is already in RAM after construction; only
    /// backends that want to keep on-disk pages warm need to override).
    fn populate(&self) -> OperationResult<()> {
        Ok(())
    }

    /// Drop disk cache for the backing file.
    ///
    /// Default: no-op (mirrors [`populate`][Self::populate] — variants that
    /// hold everything in memory after open have no on-disk cache to drop).
    fn clear_cache(&self) -> OperationResult<()> {
        Ok(())
    }

    /// Paths of the on-disk files backing this storage.
    fn files(&self) -> Vec<PathBuf>;
}

/// A buffered, growable, and persistent bitslice with fast in-memory roaring bitmap.
///
/// Use [`BitvecFlags`][1] if you need a reference to a bitslice.
///
/// Changes are buffered until explicitly flushed.
///
/// [1]: super::bitvec_flags::BitvecFlags
pub struct RoaringFlags<S: UniversalRead> {
    /// Buffered persisted flags.
    storage: BufferedDynamicFlags<S>,

    /// In-memory bitmap of true flags.
    // Potential optimization: add a secondary bitmap for false values for faster iter_falses implementation.
    bitmap: RoaringBitmap,

    /// Total length of the flags, including the trailing ones which have been set to false
    len: usize,
}

impl<S> RoaringFlagsRead for RoaringFlags<S>
where
    S: UniversalWrite + Send + 'static,
    S::Fs: Send + Sync + 'static,
{
    fn len(&self) -> usize {
        self.len
    }

    fn get_bitmap(&self) -> OperationResult<&RoaringBitmap> {
        Ok(&self.bitmap)
    }

    fn bitmap_if_materialized(&self) -> Option<&RoaringBitmap> {
        // The writable variant materializes on construction and never drops it.
        Some(&self.bitmap)
    }

    fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            storage,
            bitmap: _,
            len: _,
        } = self;
        storage.clear_cache()?;
        Ok(())
    }

    fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }
}

impl<S> RoaringFlags<S>
where
    S: UniversalWrite + Send + 'static,
    S::Fs: Send + Sync + 'static,
{
    pub fn new(fs: S::Fs, dynamic_flags: DynamicStoredFlags<S>) -> OperationResult<Self> {
        // load flags into memory
        let bitmap = RoaringBitmap::from_sorted_iter(dynamic_flags.iter_trues()?)
            .expect("iter_trues iterates in sorted order");

        if let Err(err) = dynamic_flags.clear_cache() {
            log::warn!("Failed to clear bitslice cache: {err}");
        }

        Ok(Self {
            len: dynamic_flags.len(),
            storage: BufferedDynamicFlags::new(fs, dynamic_flags),
            bitmap,
        })
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

    /// Set the value of a flag at the given index without changing the underlying storage.
    /// Returns the previous value of the flag.
    pub fn set_immutable(&mut self, index: PointOffsetType, value: bool) -> bool {
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

    pub fn flusher(&self) -> Flusher {
        self.storage.flusher()
    }
}

#[allow(clippy::default_constructed_unit_structs)]
#[duplicate::duplicate_item(
    tests_mod       S               Fs              cfg_predicate;
    [tests_mmap]    [MmapFile]      [MmapFs]        [cfg(all())];
    [tests_uring]   [IoUringFile]   [IoUringFs]     [cfg(target_os = "linux")];
)]
#[cfg_predicate]
#[cfg(test)]
mod tests_mod {
    use common::types::PointOffsetType;
    use common::universal_io::Populate;
    #[cfg_predicate]
    use common::universal_io::{Fs, S};

    use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;
    use crate::common::flags::roaring_flags::{RoaringFlags, RoaringFlagsRead};

    #[test]
    fn test_roaring_flags_consistency_after_persistence() {
        let dir = tempfile::Builder::new()
            .prefix("roaring_flags_consistency")
            .tempdir()
            .unwrap();

        // Create and update flags
        {
            let dynamic_flags =
                DynamicStoredFlags::<S>::open(&Fs::default(), dir.path(), Populate::No).unwrap();
            let mut roaring_flags = RoaringFlags::new(Fs::default(), dynamic_flags).unwrap();

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
            let mmap_flags =
                DynamicStoredFlags::<S>::open(&Fs::default(), dir.path(), Populate::Blocking)
                    .unwrap();
            let roaring_flags = RoaringFlags::new(Fs::default(), mmap_flags).unwrap();

            // Verify iteration consistency after reload
            let iter_trues: Vec<_> = roaring_flags.iter_trues().unwrap().collect();

            // Verify expected values
            assert_eq!(iter_trues, vec![0, 5, 10, 15]);

            // Verify count consistency
            assert_eq!(roaring_flags.count_trues().unwrap(), 4);
            assert_eq!(
                roaring_flags.count_falses().unwrap(),
                roaring_flags.len() - roaring_flags.count_trues().unwrap()
            );

            // Verify iteration covers all indices
            let all_trues: Vec<_> = roaring_flags.iter_trues().unwrap().collect();
            let all_falses: Vec<_> = roaring_flags.iter_falses().unwrap().collect();
            let mut all_indices = all_trues;
            all_indices.extend(all_falses);
            all_indices.sort();

            let expected_all: Vec<_> = (0..roaring_flags.len() as PointOffsetType).collect();
            assert_eq!(all_indices, expected_all);
        }
    }
}
