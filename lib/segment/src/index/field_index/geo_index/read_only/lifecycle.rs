use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::universal_io::{Populate, UniversalRead};

use super::super::mutable_geo_index::read_only::ReadOnlyAppendableGeoIndex;
use super::super::on_disk_geo_index::OnDiskGeoIndex;
use super::ReadOnlyGeoIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::geo_index::immutable_geo_index::ImmutableGeoIndex;

impl<S: UniversalRead> ReadOnlyGeoIndex<S> {
    /// Read-only mirror of [`GeoIndex::new_mutable`][1]: open the
    /// appendable (Gridstore-backed) geo index read-only, threading every
    /// file open through the filesystem handle `fs`.
    ///
    /// Thin dispatcher over [`ReadOnlyAppendableGeoIndex::open`] — wraps
    /// the leaf in [`Self::Appendable`] so callers can hold the parent enum
    /// uniformly. No `create_if_missing`: the read path never creates.
    ///
    /// [1]: super::super::GeoIndex::new_mutable
    pub fn open_appendable(fs: &S::Fs, dir: PathBuf) -> OperationResult<Option<Self>> {
        Ok(ReadOnlyAppendableGeoIndex::open(fs, dir)?.map(Self::Appendable))
    }

    /// Read-only mirror of [`GeoIndex::new_immutable`][1]: open the immutable
    /// (mmap-backed) geo index read-only through [`OnDiskGeoIndex::open`].
    ///
    /// The writable enum has two mmap variants (`Storage` for on-disk lazy,
    /// `Immutable` for in-RAM with mmap backing); the read-only side collapses
    /// to a single [`Self::Immutable`] arm because `is_on_disk` (→ populate)
    /// already covers the lazy/eager distinction inside [`OnDiskGeoIndex`].
    /// `Ok(None)` propagates from the leaf when the on-disk index doesn't
    /// exist.
    ///
    /// Note: until the `deleted` bitslice open inside [`OnDiskGeoIndex::open`]
    /// stops requesting `writeable: true`, this path is exercisable on
    /// [`MmapFile`][2] but not on the write-enforced [`ReadOnly<MmapFile>`][3]
    /// backend.
    ///
    /// [1]: super::super::GeoIndex::new_immutable
    /// [2]: common::universal_io::MmapFile
    /// [3]: common::universal_io::ReadOnly
    pub fn open_immutable(
        fs: &S::Fs,
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let effective_is_on_disk =
            is_on_disk || common::low_memory::low_memory_mode().prefer_disk();

        let populate = Populate::from(!effective_is_on_disk);

        let Some(on_disk_index) = OnDiskGeoIndex::open(fs, path, populate, deleted_points)? else {
            return Ok(None);
        };

        let index = if is_on_disk {
            Self::OnDisk(on_disk_index)
        } else {
            Self::Immutable(ImmutableGeoIndex::load_from_on_disk(on_disk_index)?)
        };

        Ok(Some(index))
    }
}
