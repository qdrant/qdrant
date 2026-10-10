use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs};

use super::super::mutable_geo_index::read_only::ReadOnlyAppendableGeoIndex;
use super::super::on_disk_geo_index::OnDiskGeoIndex;
use super::ReadOnlyGeoIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::geo_index::immutable_geo_index::ImmutableGeoIndex;
use crate::types::Memory;

impl<S: UniversalRead> ReadOnlyGeoIndex<S> {
    /// Schedule background prefetch for the appendable (Gridstore) format.
    ///
    /// Returns `false` when nothing was scheduled (directory absent).
    pub fn preopen_appendable(
        fs: &impl CachedReadFs<File = S>,
        dir: PathBuf,
    ) -> OperationResult<bool> {
        ReadOnlyAppendableGeoIndex::preopen(fs, dir)
    }

    /// Schedule background prefetch for the immutable (mmap) format.
    ///
    /// Returns `false` when the on-disk index doesn't exist.
    pub fn preopen_immutable(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        memory: Memory,
    ) -> OperationResult<bool> {
        let populate = match memory.clamp_to_low_memory().populate_on_open() {
            true => Populate::PreferBackground,
            false => Populate::No,
        };

        OnDiskGeoIndex::preopen(fs, path, populate)
    }

    /// Read-only mirror of [`GeoIndex::new_mutable`][1]: open the
    /// appendable (Gridstore-backed) geo index read-only, threading every
    /// file open through the filesystem handle `fs`.
    ///
    /// Thin dispatcher over [`ReadOnlyAppendableGeoIndex::open`] — wraps
    /// the leaf in [`Self::Appendable`] so callers can hold the parent enum
    /// uniformly. No `create_if_missing`: the read path never creates.
    ///
    /// [1]: super::super::GeoIndex::new_mutable
    pub fn open_appendable(
        fs: &impl UniversalReadFs<File = S>,
        dir: PathBuf,
        max_point_offset: PointOffsetType,
    ) -> OperationResult<Option<Self>> {
        Ok(ReadOnlyAppendableGeoIndex::open(fs, dir, max_point_offset)?.map(Self::Appendable))
    }

    /// Read-only mirror of [`GeoIndex::new_immutable`][1]: open the immutable
    /// (mmap-backed) geo index read-only through [`OnDiskGeoIndex::open`].
    ///
    /// The writable enum has two mmap variants (`Storage` for on-disk lazy,
    /// `Immutable` for in-RAM with mmap backing); the read-only side collapses
    /// to a single [`Self::Immutable`] arm because the placement
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
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        memory: Memory,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        // Low-memory mode degrades the placement, as the writable open does.
        let memory = memory.clamp_to_low_memory();

        let populate = Populate::from(memory.populate_on_open());

        let Some(on_disk_index) = OnDiskGeoIndex::open(fs, path, populate, deleted_points)? else {
            return Ok(None);
        };

        let index = if memory.is_heap() {
            Self::Immutable(ImmutableGeoIndex::load_from_on_disk(on_disk_index)?)
        } else {
            Self::OnDisk(on_disk_index)
        };

        Ok(Some(index))
    }
}
