use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::universal_io::{MmapFile, UniversalRead};
use gridstore::Blob;

use super::super::super::Encodable;
use super::super::super::mmap_numeric_index::UniversalNumericIndex;
use super::super::super::mutable_numeric_index::read_only::ReadOnlyAppendableNumericIndex;
use super::ReadOnlyNumericIndexInner;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::stored_point_to_values::StoredValue;
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
    pub fn open_gridstore(fs: &S::Fs, dir: PathBuf) -> OperationResult<Self> {
        Ok(Self::Appendable(ReadOnlyAppendableNumericIndex::open(
            fs, dir,
        )?))
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
        }
    }
}

// `open_mmap` is concrete to `S = MmapFile` because the immutable leaf
// [`UniversalNumericIndex::open`] is not yet filesystem-generic (it hard-codes
// `&MmapFs` internally). When the leaf is refactored to take `fs: &S::Fs`,
// this impl block should fold into the generic one above.
impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default>
    ReadOnlyNumericIndexInner<T, MmapFile>
where
    Vec<T>: Blob,
{
    /// Read-only mirror of [`NumericIndexInner::new_mmap`][1]: open the
    /// immutable (mmap-backed) numeric index read-only through
    /// [`UniversalNumericIndex::open`].
    ///
    /// The writable enum has three variants (`Mutable`, `Immutable`, `Mmap`);
    /// the read-only side collapses the latter two into [`Self::Immutable`]
    /// because [`UniversalNumericIndex`] reads on-demand from the mmap and
    /// `is_on_disk` (→ populate) already covers the lazy/eager distinction.
    /// `Ok(None)` propagates from the leaf when the on-disk index doesn't
    /// exist.
    ///
    /// Note: this open is exercisable on [`MmapFile`] but not on the
    /// write-enforced [`ReadOnly<MmapFile>`][2] backend until the leaf
    /// becomes fs-generic.
    ///
    /// [1]: super::super::NumericIndexInner::new_mmap
    /// [2]: common::universal_io::ReadOnly
    pub fn open_mmap(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let effective_is_on_disk =
            is_on_disk || common::low_memory::low_memory_mode().prefer_disk();

        let Some(mmap_index) =
            UniversalNumericIndex::open(path, effective_is_on_disk, deleted_points)?
        else {
            return Ok(None);
        };

        Ok(Some(Self::Immutable(mmap_index)))
    }
}
