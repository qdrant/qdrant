use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::universal_io::{Populate, UniversalRead};
use gridstore::Blob;

use super::super::super::Encodable;
use super::super::super::mutable_numeric_index::read_only::ReadOnlyAppendableNumericIndex;
use super::ReadOnlyNumericIndexInner;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_index::on_disk_numeric_index::OnDiskNumericIndex;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::index::payload_config::IndexMutability;

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default, S: UniversalRead>
    ReadOnlyNumericIndexInner<T, S>
where
    Vec<T>: Blob,
{
    /// Read-only mirror of [`NumericIndexInner::new_gridstore`][1]: open the
    /// appendable (Gridstore-backed) numeric index read-only, threading every
    /// file open through the filesystem handle `fs`.
    ///
    /// Thin dispatcher over [`ReadOnlyAppendableNumericIndex::open`] — wraps
    /// the leaf in [`Self::Appendable`] so callers can hold the parent enum
    /// uniformly. No `create_if_missing`: the read path never creates.
    ///
    /// [1]: super::super::NumericIndexInner::new_gridstore
    pub fn open_appendable(fs: &S::Fs, dir: PathBuf) -> OperationResult<Option<Self>> {
        Ok(ReadOnlyAppendableNumericIndex::open(fs, dir)?.map(Self::Appendable))
    }

    /// Read-only mirror of [`NumericIndexInner::new_mmap`][1]: open the
    /// immutable (mmap-format) numeric index read-only through
    /// [`UniversalNumericIndex::open`], threading every file open through the
    /// filesystem handle `fs`.
    ///
    /// The writable enum has three variants (`Mutable`, `Immutable`, `Mmap`);
    /// the read-only side collapses the latter two into [`Self::Immutable`]
    /// because [`UniversalNumericIndex`] reads on-demand from the mmap and
    /// `is_on_disk` (→ populate) already covers the lazy/eager distinction.
    /// `Ok(None)` propagates from the leaf when the on-disk index doesn't
    /// exist.
    ///
    /// [1]: super::super::NumericIndexInner::new_mmap
    pub fn open_immutable(
        fs: &S::Fs,
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let effective_is_on_disk =
            is_on_disk || common::low_memory::low_memory_mode().prefer_disk();

        let populate = match effective_is_on_disk {
            true => Populate::No,
            false => Populate::PreferBackground,
        };

        let Some(on_disk_index) = OnDiskNumericIndex::open(fs, path, populate, deleted_points)? else {
            return Ok(None);
        };

        Ok(Some(Self::OnDisk(on_disk_index)))
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
            Self::OnDisk(_) => IndexMutability::Immutable,
        }
    }
}
