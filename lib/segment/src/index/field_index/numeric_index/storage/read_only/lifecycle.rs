use std::path::{Path, PathBuf};

use blobstore::Blob;
use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs};

use super::super::super::Encodable;
use super::super::super::mutable_numeric_index::read_only::ReadOnlyAppendableNumericIndex;
use super::ReadOnlyNumericIndexInner;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_index::immutable_numeric_index::ImmutableNumericIndex;
use crate::index::field_index::numeric_index::on_disk_numeric_index::OnDiskNumericIndex;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::index::payload_config::IndexMutability;
use crate::types::Memory;

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default, S: UniversalRead>
    ReadOnlyNumericIndexInner<T, S>
where
    Vec<T>: Blob,
{
    /// Schedule background prefetch for the appendable (Gridstore) format.
    ///
    /// Returns `false` when nothing was scheduled (directory absent).
    pub fn preopen_appendable(
        fs: &impl CachedReadFs<File = S>,
        dir: PathBuf,
    ) -> OperationResult<bool> {
        ReadOnlyAppendableNumericIndex::preopen(fs, dir)
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

        OnDiskNumericIndex::<T, S>::preopen(fs, path, populate)
    }

    /// Read-only mirror of [`NumericIndexInner::new_gridstore`][1]: open the
    /// appendable (Gridstore-backed) numeric index read-only, threading every
    /// file open through the filesystem handle `fs`.
    ///
    /// Thin dispatcher over [`ReadOnlyAppendableNumericIndex::open`] — wraps
    /// the leaf in [`Self::Appendable`] so callers can hold the parent enum
    /// uniformly. No `create_if_missing`: the read path never creates.
    ///
    /// [1]: super::super::NumericIndexInner::new_gridstore
    pub fn open_appendable(
        fs: &impl UniversalReadFs<File = S>,
        dir: PathBuf,
        max_point_offset: PointOffsetType,
    ) -> OperationResult<Option<Self>> {
        Ok(ReadOnlyAppendableNumericIndex::open(fs, dir, max_point_offset)?.map(Self::Appendable))
    }

    /// Read-only mirror of [`NumericIndexInner::new_mmap`][1]: open the
    /// immutable (mmap-format) numeric index read-only through
    /// [`UniversalNumericIndex::open`], threading every file open through the
    /// filesystem handle `fs`.
    ///
    /// The writable enum has three variants (`Mutable`, `Immutable`, `Mmap`);
    /// the read-only side collapses the latter two into [`Self::Immutable`]
    /// because [`UniversalNumericIndex`] reads on-demand from the mmap and
    /// the placement already covers the lazy/eager distinction.
    /// `Ok(None)` propagates from the leaf when the on-disk index doesn't
    /// exist.
    ///
    /// [1]: super::super::NumericIndexInner::new_mmap
    pub fn open_immutable(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        memory: Memory,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        // Low-memory mode degrades the placement, as the writable open does.
        let memory = memory.clamp_to_low_memory();

        let populate = Populate::from(memory.populate_on_open());
        let Some(mmap_index) = OnDiskNumericIndex::open(fs, path, populate, deleted_points)? else {
            return Ok(None);
        };

        let index = if memory.is_heap() {
            Self::Immutable(ImmutableNumericIndex::load_from_on_disk(mmap_index))
        } else {
            Self::OnDisk(mmap_index)
        };

        Ok(Some(index))
    }

    /// Reports the on-disk format's mutability, mirroring
    /// [`NumericIndex::get_mutability_type`][1].
    ///
    /// Reflects what the segment's payload-index config records about the
    /// storage format, NOT whether the runtime wrapper permits writes. The
    /// read-only wrapper always denies mutation; this value is what an
    /// equivalent writable open would report.
    ///
    /// - [`Self::Appendable`] mirrors the writable `Mutable` variant
    ///   (Gridstore-backed) → [`IndexMutability::Mutable`].
    /// - [`Self::Immutable`] mirrors the writable `Immutable` / `Mmap`
    ///   variants (mmap-backed) → [`IndexMutability::Immutable`].
    ///
    /// [1]: super::super::super::NumericIndex::get_mutability_type
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
}
