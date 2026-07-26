use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use blobstore::Blob;
use common::bitvec::BitSlice;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};

use super::super::Encodable;
use super::super::storage::read_only::ReadOnlyNumericIndexInner;
use super::ReadOnlyNumericIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::index::payload_config::IndexMutability;

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default, P, S: UniversalRead>
    ReadOnlyNumericIndex<T, P, S>
where
    Vec<T>: Blob,
{
    /// Schedule background prefetch for the appendable (Gridstore) format,
    /// forwarding to [`ReadOnlyNumericIndexInner::preopen_appendable`].
    pub fn preopen_appendable(
        fs: &impl CachedReadFs<File = S>,
        dir: PathBuf,
    ) -> OperationResult<bool> {
        ReadOnlyNumericIndexInner::preopen_appendable(fs, dir)
    }

    /// Schedule background prefetch for the immutable (mmap) format, forwarding
    /// to [`ReadOnlyNumericIndexInner::preopen_immutable`].
    pub fn preopen_immutable(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        is_on_disk: bool,
    ) -> OperationResult<bool> {
        ReadOnlyNumericIndexInner::preopen_immutable(fs, path, is_on_disk)
    }

    /// Read-only mirror of [`NumericIndex::new_gridstore`][1]: forwards to
    /// [`ReadOnlyNumericIndexInner::open_appendable`] and wraps the inner with
    /// the typed payload-value phantom `P`.
    ///
    /// [1]: super::super::NumericIndex::new_gridstore
    pub fn open_appendable(
        fs: &impl UniversalReadFs<File = S>,
        dir: PathBuf,
    ) -> OperationResult<Option<Self>> {
        Ok(
            ReadOnlyNumericIndexInner::open_appendable(fs, dir)?.map(|inner| Self {
                inner,
                _phantom: PhantomData,
            }),
        )
    }

    /// Read-only mirror of [`NumericIndex::new_mmap`][1]: forwards to
    /// [`ReadOnlyNumericIndexInner::open_immutable`] and wraps the inner with
    /// the typed payload-value phantom `P`. `Ok(None)` propagates from the
    /// inner when the on-disk index doesn't exist.
    ///
    /// [1]: super::super::NumericIndex::new_mmap
    pub fn open_immutable(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        Ok(
            ReadOnlyNumericIndexInner::open_immutable(fs, path, is_on_disk, deleted_points)?.map(
                |inner| Self {
                    inner,
                    _phantom: PhantomData,
                },
            ),
        )
    }

    /// Reports the on-disk format's mutability, forwarding to
    /// [`ReadOnlyNumericIndexInner::get_mutability_type`]. Mirrors the
    /// writable [`NumericIndex::get_mutability_type`][1].
    ///
    /// [1]: super::super::NumericIndex::get_mutability_type
    pub fn get_mutability_type(&self) -> IndexMutability {
        self.inner.get_mutability_type()
    }
}
