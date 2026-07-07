use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::universal_io::{Populate, UniversalRead, UniversalReadFs};
use gridstore::Blob;

use super::super::MapIndexKey;
use super::super::mutable_map_index::read_only::ReadOnlyAppendableMapIndex;
use super::super::on_disk_map_index::OnDiskMapIndex;
use super::ReadOnlyMapIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::map_index::immutable_map_index::ImmutableMapIndex;
use crate::index::payload_config::IndexMutability;

impl<N: MapIndexKey + ?Sized, S: UniversalRead> ReadOnlyMapIndex<N, S>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
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
    ) -> OperationResult<Option<Self>> {
        Ok(ReadOnlyAppendableMapIndex::open(fs, dir)?.map(Self::Appendable))
    }

    /// Read-only mirror of [`MapIndex::new_mmap`][1]: open the immutable
    /// (mmap-format) map index read-only through [`UniversalMapIndex::open`],
    /// threading every file open through the filesystem handle `fs`.
    ///
    /// The writable enum has two mmap variants (`Immutable` for in-RAM with
    /// mmap backing, `Mmap` for on-disk lazy); the read-only side collapses
    /// to a single [`Self::Immutable`] arm because `is_on_disk` (→ populate)
    /// already covers the lazy/eager distinction inside [`UniversalMapIndex`].
    /// `Ok(None)` propagates from the leaf when the on-disk index doesn't
    /// exist.
    ///
    /// [1]: super::super::MapIndex::new_mmap
    pub fn open_immutable(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let effective_is_on_disk =
            is_on_disk || common::low_memory::low_memory_mode().prefer_disk();

        let populate = Populate::from(!effective_is_on_disk);
        let Some(on_disk_index) = OnDiskMapIndex::open(fs, path, populate, deleted_points)? else {
            return Ok(None);
        };

        if effective_is_on_disk {
            Ok(Some(Self::OnDisk(on_disk_index)))
        } else {
            Ok(Some(Self::Immutable(ImmutableMapIndex::load_from_on_disk(
                on_disk_index,
            )?)))
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
}
