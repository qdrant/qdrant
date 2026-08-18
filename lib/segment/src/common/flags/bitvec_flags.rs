use std::path::{Path, PathBuf};

use common::bitvec::{BitSlice, BitVec};
use common::types::PointOffsetType;
use common::universal_io::{Populate, UniversalRead, UniversalWrite};

use super::buffered_dynamic_flags::BufferedDynamicFlags;
use super::compact_stored_flags::CompactStoredFlags;
use super::dynamic_stored_flags::DynamicStoredFlags;
use super::mode::FlagsMode;
use super::storage::FlagsStorage;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;

/// A buffered, growable, and persistent bitslice with a separate in-memory bitvec.
///
/// Use [`RoaringFlags`][1] if you need a reference to a bitmap.
///
/// Changes are buffered until explicitly flushed.
///
/// [1]: super::roaring_flags::RoaringFlags
#[derive(Debug)]
pub struct BitvecFlags<S: UniversalRead> {
    /// Persisted flags, in either storage mode.
    storage: FlagsStorage<S>,

    /// In-memory bitvec of true and false flags.
    bitvec: BitVec,

    /// Total length of the flags, including the trailing ones which have been set to false
    len: usize,
}

impl<S> BitvecFlags<S>
where
    S: UniversalWrite + Send + 'static,
    S::Fs: Send + Sync + 'static,
{
    /// Open the flags in `directory`, or create them when none exist there yet.
    ///
    /// The mode of existing flags is detected automatically from the files
    /// present; `mode_if_create` only applies when creating fresh flags.
    pub fn open_or_create(
        fs: S::Fs,
        directory: &Path,
        mode_if_create: FlagsMode,
        populate: Populate,
    ) -> OperationResult<Self> {
        match FlagsMode::detect(&fs, directory)?.unwrap_or(mode_if_create) {
            FlagsMode::Dynamic => {
                let dynamic_flags = DynamicStoredFlags::open(&fs, directory, populate)?;
                Self::new(fs, dynamic_flags)
            }
            FlagsMode::Compact => {
                let compact_flags = CompactStoredFlags::open(fs, directory, populate)?;
                Ok(Self::from_compact(compact_flags))
            }
        }
    }

    pub fn new(fs: S::Fs, dynamic_flags: DynamicStoredFlags<S>) -> OperationResult<Self> {
        // load flags into memory
        let bitvec = BitVec::from_bitslice(&*dynamic_flags.get_bitslice()?);

        if let Err(err) = dynamic_flags.clear_cache() {
            log::warn!("Failed to clear bitslice cache: {err}");
        }

        Ok(Self {
            len: dynamic_flags.len(),
            storage: FlagsStorage::Dynamic(BufferedDynamicFlags::new(fs, dynamic_flags)),
            bitvec,
        })
    }

    fn from_compact(compact_flags: CompactStoredFlags<S>) -> Self {
        let len = compact_flags.len();

        // load flags into memory
        let mut bitvec = BitVec::repeat(false, len);
        for index in compact_flags.to_bitmap() {
            bitvec.set(index as usize, true);
        }

        Self {
            storage: FlagsStorage::Compact(compact_flags),
            bitvec,
            len,
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

    #[inline]
    pub fn count_trues(&self) -> usize {
        self.bitvec.count_ones()
    }

    #[inline]
    pub fn count_falses(&self) -> usize {
        self.bitvec.count_zeros()
    }

    /// Set the value of a flag at the given index, grows the bitvec if needed.
    /// Returns the previous value of the flag.
    pub fn set(&mut self, index: PointOffsetType, value: bool) -> bool {
        // record write in persisted storage
        self.storage.set(index, value);

        // update length if needed
        let index_usize = index as usize;
        if index_usize >= self.len {
            self.len = index_usize + 1;
            self.bitvec.resize(self.len, false);
        }

        // update bitmap
        self.bitvec.replace(index_usize, value)
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            storage,
            bitvec: _,
            len: _,
        } = self;
        storage.clear_cache()?;
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
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

    use crate::common::flags::FlagsMode;
    use crate::common::flags::bitvec_flags::BitvecFlags;
    use crate::common::flags::compact_stored_flags::COMPACT_FLAGS_FILE;
    use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;

    #[test]
    fn test_roaring_flags_consistency_after_persistence() {
        let dir = tempfile::Builder::new()
            .prefix("roaring_flags_consistency")
            .tempdir()
            .unwrap();

        // Create and update flags
        {
            let mmap_flags =
                DynamicStoredFlags::<S>::open(&Fs::default(), dir.path(), Populate::No).unwrap();
            let mut bitvec_flags = BitvecFlags::new(Fs::default(), mmap_flags).unwrap();

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
            let mmap_flags =
                DynamicStoredFlags::<S>::open(&Fs::default(), dir.path(), Populate::Blocking)
                    .unwrap();
            let bitvec_flags = BitvecFlags::new(Fs::default(), mmap_flags).unwrap();

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

    #[test]
    fn test_compact_mode_roundtrip() {
        let dir = tempfile::Builder::new()
            .prefix("bitvec_flags_compact")
            .tempdir()
            .unwrap();

        {
            let mut flags = BitvecFlags::<S>::open_or_create(
                Fs::default(),
                dir.path(),
                FlagsMode::Compact,
                Populate::No,
            )
            .unwrap();
            assert!(!flags.set(0, true));
            assert!(!flags.set(5, true));
            assert!(!flags.set(9, false)); // grows the flags to 10
            assert!(flags.set(5, true)); // previous value
            assert_eq!(flags.len(), 10);

            let files = flags.files();
            assert_eq!(files.len(), 1);
            assert!(files[0].ends_with(COMPACT_FLAGS_FILE));

            flags.flusher()().unwrap();
        }

        // Requesting dynamic mode on existing flags keeps the compact mode.
        {
            let flags = BitvecFlags::<S>::open_or_create(
                Fs::default(),
                dir.path(),
                FlagsMode::Dynamic,
                Populate::Blocking,
            )
            .unwrap();
            assert_eq!(flags.files().len(), 1);
            assert_eq!(flags.len(), 10);
            assert_eq!(flags.count_trues(), 2);
            assert_eq!(flags.iter_trues().collect::<Vec<_>>(), vec![0, 5]);
            assert!(flags.get(0));
            assert!(!flags.get(9));
            assert_eq!(flags.get_bitslice().count_ones(), 2);
        }
    }

    #[test]
    fn test_dynamic_mode_kept_when_compact_requested() {
        let dir = tempfile::Builder::new()
            .prefix("bitvec_flags_dynamic")
            .tempdir()
            .unwrap();

        {
            let mut flags = BitvecFlags::<S>::open_or_create(
                Fs::default(),
                dir.path(),
                FlagsMode::Dynamic,
                Populate::No,
            )
            .unwrap();
            flags.set(1, true);
            flags.flusher()().unwrap();

            // dynamic stack: flags file plus status file
            assert!(flags.files().len() > 1);
        }

        {
            let flags = BitvecFlags::<S>::open_or_create(
                Fs::default(),
                dir.path(),
                FlagsMode::Compact,
                Populate::Blocking,
            )
            .unwrap();
            assert!(flags.files().len() > 1);
            assert_eq!(flags.len(), 2);
            assert!(flags.get(1));
        }
    }
}
