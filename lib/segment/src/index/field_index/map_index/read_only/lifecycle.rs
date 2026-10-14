use std::path::{Path, PathBuf};

use blobstore::Blob;
use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs};

use super::super::MapIndexKey;
use super::super::mutable_map_index::read_only::ReadOnlyAppendableMapIndex;
use super::super::on_disk_map_index::OnDiskMapIndex;
use super::ReadOnlyMapIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::map_index::immutable_map_index::ImmutableMapIndex;
use crate::index::payload_config::IndexMutability;
use crate::types::Memory;

impl<N: MapIndexKey + ?Sized, S: UniversalRead> ReadOnlyMapIndex<N, S>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub fn preopen_appendable(
        fs: &impl CachedReadFs<File = S>,
        dir: PathBuf,
    ) -> OperationResult<bool> {
        ReadOnlyAppendableMapIndex::<N, S>::preopen(fs, dir)
    }

    /// Read-only mirror of [`MapIndex::new_gridstore`][1]: open the appendable
    /// (Gridstore-backed) map index read-only, threading every file open
    /// through the filesystem handle `fs`.
    ///
    /// Thin dispatcher over [`ReadOnlyAppendableMapIndex::open`] — wraps the
    /// leaf in [`Self::Appendable`] so callers can hold the parent enum
    /// uniformly. No `create_if_missing`: the read path never creates;
    /// [`Ok(None)`] propagates from the leaf when the on-disk directory
    /// doesn't exist.
    ///
    /// [1]: super::super::MapIndex::new_gridstore
    pub fn open_appendable(
        fs: &impl UniversalReadFs<File = S>,
        dir: PathBuf,
        max_point_offset: PointOffsetType,
    ) -> OperationResult<Option<Self>> {
        Ok(ReadOnlyAppendableMapIndex::open(fs, dir, max_point_offset)?.map(Self::Appendable))
    }

    pub fn preopen_immutable(
        fs: &impl CachedReadFs<File = S>,
        dir: &Path,
        memory: Memory,
    ) -> OperationResult<bool> {
        let populate = match memory.clamp_to_low_memory().populate_on_open() {
            true => Populate::PreferBackground,
            false => Populate::No,
        };

        OnDiskMapIndex::<N, S>::preopen(fs, dir, populate)
    }

    /// Read-only mirror of [`MapIndex::new_mmap`][1]: open the immutable
    /// (mmap-format) map index read-only through [`OnDiskMapIndex::open`],
    /// threading every file open through the filesystem handle `fs`.
    ///
    /// The writable enum has two mmap variants (`Immutable` for in-RAM with
    /// mmap backing, `Mmap` for on-disk lazy); the read-only side collapses
    /// to a single [`Self::Immutable`] arm because the placement
    /// already covers the lazy/eager distinction inside [`OnDiskMapIndex`].
    /// `Ok(None)` propagates from the leaf when the on-disk index doesn't
    /// exist.
    ///
    /// [1]: super::super::MapIndex::new_mmap
    pub fn open_immutable(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        memory: Memory,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        // Low-memory mode degrades the placement, as the writable open does.
        let memory = memory.clamp_to_low_memory();

        let populate = Populate::from(memory.populate_on_open());
        let Some(on_disk_index) = OnDiskMapIndex::open(fs, path, populate, deleted_points)? else {
            return Ok(None);
        };

        if memory.is_heap() {
            Ok(Some(Self::Immutable(ImmutableMapIndex::load_from_on_disk(
                on_disk_index,
            )?)))
        } else {
            Ok(Some(Self::OnDisk(on_disk_index)))
        }
    }

    /// Reports the on-disk format's mutability, mirroring
    /// [`MapIndex::get_mutability_type`][1].
    ///
    /// The read-only enum has two variants where the writable side has three:
    /// `Appendable` corresponds to the writable `Mutable` arm, `Immutable`
    /// covers both writable `Immutable` (in-RAM with mmap backing) and
    /// writable `Mmap` (on-disk lazy) — both already report
    /// [`IndexMutability::Immutable`] on the writable side, so the read-only
    /// label matches even after the collapse.
    ///
    /// [1]: super::super::MapIndex::get_mutability_type
    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            Self::Appendable(_) => IndexMutability::Mutable,
            Self::Immutable(_) => IndexMutability::Immutable,
            Self::OnDisk(_) => IndexMutability::Immutable,
        }
    }

    pub fn is_cold(&self) -> bool {
        match self {
            Self::Appendable(_) => false,
            Self::Immutable(_) => false,
            Self::OnDisk(index) => index.is_cold(),
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            Self::Appendable(index) => index.files(),
            Self::Immutable(index) => index.files(),
            Self::OnDisk(index) => index.files(),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            Self::Appendable(_) => vec![],
            Self::Immutable(index) => index.immutable_files(),
            Self::OnDisk(index) => index.immutable_files(),
        }
    }

    /// Populate all pages in the mmap. Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            Self::Appendable(_) => Ok(()),
            Self::Immutable(_) => Ok(()),
            Self::OnDisk(index) => index.populate(),
        }
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            Self::Appendable(index) => index.clear_cache(),
            Self::Immutable(index) => index.clear_cache(),
            Self::OnDisk(index) => index.clear_cache(),
        }
    }
}
