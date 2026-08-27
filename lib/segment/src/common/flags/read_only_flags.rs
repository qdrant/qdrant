use std::path::{Path, PathBuf};

use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs};
use futures::future::BoxFuture;
use roaring::RoaringBitmap;

use super::mode::FlagsMode;
use super::read_only_compact_flags::ReadOnlyCompactFlags;
use super::read_only_roaring_flags::ReadOnlyRoaringFlags;
use super::roaring_flags::RoaringFlagsRead;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;

/// Read-only flags in either [storage mode](FlagsMode), serving the
/// [`RoaringFlagsRead`] surface.
///
/// Read-side counterpart of the mode dispatch inside the writable wrappers:
/// [`Self::open`] detects the mode from the files present and every operation
/// forwards to that variant.
pub enum ReadOnlyFlags<S: UniversalRead> {
    /// Flags in the dynamic (mmapped, mutated in place) format.
    Dynamic(ReadOnlyRoaringFlags<S>),

    /// Flags in the compact stored-bitmask format.
    Compact(ReadOnlyCompactFlags<S>),
}

impl<S: UniversalRead> ReadOnlyFlags<S> {
    /// Schedule background prefetch of the files [`Self::open`] reads, in the
    /// detected mode.
    ///
    /// Returns whether the flags exist.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        directory: &Path,
        populate: Populate,
    ) -> OperationResult<bool> {
        match FlagsMode::detect(fs, directory)? {
            None => Ok(false),
            Some(FlagsMode::Dynamic) => ReadOnlyRoaringFlags::<S>::preopen(fs, directory, populate),
            Some(FlagsMode::Compact) => ReadOnlyCompactFlags::<S>::preopen(fs, directory, populate),
        }
    }

    /// Open persisted flags read-only, in their detected mode.
    ///
    /// Returns [`Ok(None)`] when no flags exist in `directory`, matching the
    /// read path's never-create contract.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        directory: &Path,
    ) -> OperationResult<Option<Self>> {
        match FlagsMode::detect(fs, directory)? {
            None => Ok(None),
            Some(FlagsMode::Dynamic) => {
                Ok(ReadOnlyRoaringFlags::open(fs, directory)?.map(Self::Dynamic))
            }
            Some(FlagsMode::Compact) => {
                Ok(ReadOnlyCompactFlags::open(fs, directory)?.map(Self::Compact))
            }
        }
    }

    /// Refresh to the current on-disk state; see the variants' impls for the
    /// respective reload semantics.
    pub fn live_reload(&mut self, fs: &impl UniversalReadFs<File = S>) -> OperationResult<()> {
        match self {
            Self::Dynamic(flags) => flags.live_reload(fs),
            Self::Compact(flags) => flags.live_reload(fs),
        }
    }
}

impl<S: UniversalRead> LiveReload for ReadOnlyFlags<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = Self::File>>(
        &self,
        cached_fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        match self {
            ReadOnlyFlags::Dynamic(dynamic) => dynamic.live_preload(cached_fs),
            ReadOnlyFlags::Compact(compact) => compact.live_preload(cached_fs),
        };
        Ok(Vec::new())
    }

    fn live_reload<Fs: UniversalReadFs<File = Self::File>>(
        &mut self,
        fs: &Fs,
        _deleted_points: &common::sorted_slice::SortedSlice<'_, common::types::PointOffsetType>,
        _new_points: &common::sorted_slice::SortedSlice<'_, common::types::PointOffsetType>,
        _hw_counter: &common::counter::hardware_counter::HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyFlags::Dynamic(dynamic) => dynamic.live_reload(fs),
            ReadOnlyFlags::Compact(compact) => compact.live_reload(fs),
        }
    }
}

impl<S: UniversalRead> RoaringFlagsRead for ReadOnlyFlags<S> {
    fn len(&self) -> usize {
        match self {
            Self::Dynamic(flags) => flags.len(),
            Self::Compact(flags) => flags.len(),
        }
    }

    fn get_bitmap(&self) -> OperationResult<&RoaringBitmap> {
        match self {
            Self::Dynamic(flags) => flags.get_bitmap(),
            Self::Compact(flags) => flags.get_bitmap(),
        }
    }

    fn bitmap_if_materialized(&self) -> Option<&RoaringBitmap> {
        match self {
            Self::Dynamic(flags) => flags.bitmap_if_materialized(),
            Self::Compact(flags) => flags.bitmap_if_materialized(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            Self::Dynamic(flags) => flags.files(),
            Self::Compact(flags) => flags.files(),
        }
    }
}

#[cfg(test)]
mod tests {
    use common::universal_io::{MmapFile, MmapFs};
    use tempfile::TempDir;

    use super::*;
    use crate::common::flags::compact_stored_flags::CompactStoredFlags;
    use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;

    #[test]
    fn open_dispatches_on_detected_mode() {
        let tmp = TempDir::new().unwrap();

        // Nothing on disk: no flags.
        let missing = tmp.path().join("missing");
        assert!(
            ReadOnlyFlags::<MmapFile>::open(&MmapFs, &missing)
                .unwrap()
                .is_none()
        );

        // Dynamic-mode flags.
        let dynamic_dir = tmp.path().join("dynamic");
        {
            let mut writer =
                DynamicStoredFlags::<MmapFile>::open(&MmapFs, &dynamic_dir, Populate::No).unwrap();
            writer.set_len(&MmapFs, 10).unwrap();
            writer.set(3, true).unwrap();
            writer.flusher()().unwrap();
        }
        let flags = ReadOnlyFlags::<MmapFile>::open(&MmapFs, &dynamic_dir)
            .unwrap()
            .unwrap();
        assert!(matches!(flags, ReadOnlyFlags::Dynamic(_)));
        assert_eq!(flags.len(), 10);
        assert!(flags.get(3).unwrap());
        assert_eq!(flags.count_trues().unwrap(), 1);

        // Compact-mode flags.
        let compact_dir = tmp.path().join("compact");
        {
            let writer =
                CompactStoredFlags::<MmapFile>::open(MmapFs, &compact_dir, Populate::No).unwrap();
            writer.set(7, true);
            writer.flusher()().unwrap();
        }
        let flags = ReadOnlyFlags::<MmapFile>::open(&MmapFs, &compact_dir)
            .unwrap()
            .unwrap();
        assert!(matches!(flags, ReadOnlyFlags::Compact(_)));
        assert_eq!(flags.len(), 8);
        assert!(flags.get(7).unwrap());
        assert_eq!(flags.files().len(), 1);
    }
}
