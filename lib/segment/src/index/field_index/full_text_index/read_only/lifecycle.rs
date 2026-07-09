use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs};

use super::super::mutable_text_index::read_only::ReadOnlyAppendableFullTextIndex;
use super::super::on_disk_text_index::OnDiskFullTextIndex;
use super::ReadOnlyFullTextIndex;
use crate::common::operation_error::OperationResult;
use crate::data_types::index::TextIndexParams;
use crate::index::field_index::full_text_index::immutable_text_index::ImmutableFullTextIndex;
use crate::index::payload_config::IndexMutability;

impl<S: UniversalRead> ReadOnlyFullTextIndex<S> {
    /// Schedule background prefetch for the appendable (Gridstore) format.
    ///
    /// Returns `false` when nothing was scheduled (directory absent).
    pub fn preopen_appendable(
        fs: &impl CachedReadFs<File = S>,
        dir: PathBuf,
    ) -> OperationResult<bool> {
        ReadOnlyAppendableFullTextIndex::preopen(fs, dir)
    }

    /// Read-only mirror of [`FullTextIndex::new_gridstore`][1]: open the
    /// appendable (Gridstore-backed) full-text index read-only, threading
    /// every file open through the filesystem handle `fs`.
    ///
    /// Thin dispatcher over [`ReadOnlyAppendableFullTextIndex::open`] — wraps
    /// the leaf in [`Self::Appendable`] so callers can hold the parent enum
    /// uniformly. No `create_if_missing`: the read path never creates;
    /// [`Ok(None)`] propagates from the leaf when the on-disk directory
    /// doesn't exist.
    ///
    /// [1]: super::super::FullTextIndex::new_gridstore
    pub fn open_appendable(
        fs: &impl UniversalReadFs<File = S>,
        dir: PathBuf,
        config: TextIndexParams,
    ) -> OperationResult<Option<Self>> {
        Ok(ReadOnlyAppendableFullTextIndex::open(fs, dir, config)?.map(Self::Appendable))
    }

    /// Schedule background prefetch for the immutable (mmap) format.
    ///
    /// Returns `false` when the on-disk index doesn't exist.
    pub fn preopen_immutable(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        is_on_disk: bool,
    ) -> OperationResult<bool> {
        let effective_is_on_disk =
            is_on_disk || common::low_memory::low_memory_mode().prefer_disk();

        let populate = match effective_is_on_disk {
            true => Populate::No,
            false => Populate::PreferBackground,
        };

        OnDiskFullTextIndex::preopen(fs, path, populate)
    }

    /// Read-only mirror of [`FullTextIndex::new_mmap`][1]: open the immutable
    /// (mmap-format) full-text index read-only through [`MmapFullTextIndex::open`],
    /// threading every file open through the filesystem handle `fs`.
    ///
    /// The writable enum splits the mmap path into two variants (`Immutable`
    /// for in-RAM with mmap backing, `Mmap` for on-disk lazy); the read-only
    /// side collapses to a single [`Self::Immutable`] arm because
    /// `is_on_disk` (→ populate) already covers the lazy/eager distinction
    /// inside [`MmapFullTextIndex`]. `Ok(None)` propagates from the leaf when
    /// the on-disk index doesn't exist.
    ///
    /// [1]: super::super::FullTextIndex::new_mmap
    pub fn open_immutable(
        fs: &impl UniversalReadFs<File = S>,
        path: PathBuf,
        config: TextIndexParams,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let effective_is_on_disk =
            is_on_disk || common::low_memory::low_memory_mode().prefer_disk();

        let populate = Populate::from(!effective_is_on_disk);

        let Some(on_disk_index) =
            OnDiskFullTextIndex::open(fs, path, config, populate, deleted_points)?
        else {
            return Ok(None);
        };

        let index = if effective_is_on_disk {
            Self::OnDisk(on_disk_index)
        } else {
            Self::Immutable(ImmutableFullTextIndex::load_from_on_disk(on_disk_index)?)
        };

        Ok(Some(index))
    }

    /// Reports the on-disk format's mutability, mirroring
    /// [`FullTextIndex::get_mutability_type`][1].
    ///
    /// The read-only enum has two variants where the writable side has three:
    /// `Appendable` corresponds to the writable `Mutable` arm, `Immutable`
    /// covers both writable `Immutable` (in-RAM with mmap backing) and
    /// writable `Mmap` (on-disk lazy) — both already report
    /// [`IndexMutability::Immutable`] on the writable side, so the read-only
    /// label matches even after the collapse.
    ///
    /// [1]: super::super::FullTextIndex::get_mutability_type
    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            Self::Appendable(_) => IndexMutability::Mutable,
            Self::OnDisk(_) => IndexMutability::Immutable,
            Self::Immutable(_) => IndexMutability::Immutable,
        }
    }
}
