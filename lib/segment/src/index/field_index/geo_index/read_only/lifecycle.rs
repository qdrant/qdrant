use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::universal_io::UniversalRead;

use super::super::mmap_geo_index::StoredGeoMapIndex;
use super::super::mutable_geo_index::read_only::ReadOnlyAppendableGeoMapIndex;
use super::ReadOnlyGeoMapIndex;
use crate::common::operation_error::OperationResult;

impl<S: UniversalRead> ReadOnlyGeoMapIndex<S> {
    /// Read-only mirror of [`GeoMapIndex::new_gridstore`][1]: open the
    /// appendable (Gridstore-backed) geo index read-only, threading every
    /// file open through the filesystem handle `fs`.
    ///
    /// Thin dispatcher over [`ReadOnlyAppendableGeoMapIndex::open`] — wraps
    /// the leaf in [`Self::Appendable`] so callers can hold the parent enum
    /// uniformly. No `create_if_missing`: the read path never creates.
    ///
    /// [1]: super::super::GeoMapIndex::new_gridstore
    pub fn open_gridstore(fs: &S::Fs, dir: PathBuf) -> OperationResult<Option<Self>> {
        Ok(ReadOnlyAppendableGeoMapIndex::open(fs, dir)?.map(Self::Appendable))
    }

    /// Read-only mirror of [`GeoMapIndex::new_mmap`][1]: open the immutable
    /// (mmap-backed) geo index read-only through [`StoredGeoMapIndex::open`].
    ///
    /// The writable enum has two mmap variants (`Storage` for on-disk lazy,
    /// `Immutable` for in-RAM with mmap backing); the read-only side collapses
    /// to a single [`Self::Immutable`] arm because `is_on_disk` (→ populate)
    /// already covers the lazy/eager distinction inside [`StoredGeoMapIndex`].
    /// `Ok(None)` propagates from the leaf when the on-disk index doesn't
    /// exist.
    ///
    /// Note: until the `deleted` bitslice open inside [`StoredGeoMapIndex::open`]
    /// stops requesting `writeable: true`, this path is exercisable on
    /// [`MmapFile`][2] but not on the write-enforced [`ReadOnly<MmapFile>`][3]
    /// backend.
    ///
    /// [1]: super::super::GeoMapIndex::new_mmap
    /// [2]: common::universal_io::MmapFile
    /// [3]: common::universal_io::ReadOnly
    pub fn open_mmap(
        fs: &S::Fs,
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let effective_is_on_disk =
            is_on_disk || common::low_memory::low_memory_mode().prefer_disk();

        let Some(mmap_index) =
            StoredGeoMapIndex::open(fs, path, effective_is_on_disk, deleted_points)?
        else {
            return Ok(None);
        };

        Ok(Some(Self::Immutable(mmap_index)))
    }
}
