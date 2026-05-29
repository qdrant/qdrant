//! Construction, persistence, mutation, and cache-control forwarding for
//! [`NumericIndexInner`].

use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use common::universal_io::MmapFs;
use gridstore::Blob;

use super::super::Encodable;
use super::super::immutable_numeric_index::ImmutableNumericIndex;
use super::super::mutable_numeric_index::MutableNumericIndex;
use super::super::universal_numeric_index::UniversalNumericIndex;
use super::NumericIndexInner;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::stored_point_to_values::StoredValue;

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default> NumericIndexInner<T>
where
    Vec<T>: Blob,
{
    /// Load immutable mmap based index, either in RAM or on disk
    pub fn new_mmap(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        // Low-memory mode downgrades the in-RAM `Immutable` wrapper to the
        // pure-mmap `Storage` variant at load time. Files are shared between
        // variants; the persisted `is_on_disk` flag in `mmap_index` is
        // untouched.
        let effective_is_on_disk =
            is_on_disk || common::low_memory::low_memory_mode().prefer_disk();

        let Some(mmap_index) =
            UniversalNumericIndex::open(&MmapFs, path, effective_is_on_disk, deleted_points)?
        else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        if effective_is_on_disk {
            // Use on mmap directly
            Ok(Some(NumericIndexInner::Mmap(mmap_index)))
        } else {
            // Load into RAM, use mmap as backing storage
            Ok(Some(NumericIndexInner::Immutable(
                ImmutableNumericIndex::open_mmap(mmap_index),
            )))
        }
    }

    pub fn new_gridstore(dir: PathBuf, create_if_missing: bool) -> OperationResult<Option<Self>> {
        Ok(MutableNumericIndex::open_gridstore(dir, create_if_missing)?
            .map(NumericIndexInner::Mutable))
    }

    pub fn flusher(&self) -> Flusher {
        match self {
            NumericIndexInner::Mutable(index) => index.flusher(),
            NumericIndexInner::Immutable(index) => index.flusher(),
            NumericIndexInner::Mmap(index) => index.flusher(),
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            NumericIndexInner::Mutable(index) => index.files(),
            NumericIndexInner::Immutable(index) => index.files(),
            NumericIndexInner::Mmap(index) => index.files(),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            NumericIndexInner::Mutable(_) => vec![],
            NumericIndexInner::Immutable(index) => index.immutable_files(),
            NumericIndexInner::Mmap(index) => index.immutable_files(),
        }
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        match self {
            NumericIndexInner::Mutable(index) => index.remove_point(idx)?,
            NumericIndexInner::Immutable(index) => index.remove_point(idx),
            NumericIndexInner::Mmap(index) => index.remove_point(idx),
        }
        Ok(())
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            NumericIndexInner::Mutable(_) => {}   // Not a mmap
            NumericIndexInner::Immutable(_) => {} // Not a mmap
            NumericIndexInner::Mmap(index) => index.populate()?,
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            // Only clears backing mmap storage if used, not in-memory representation
            NumericIndexInner::Mutable(index) => index.clear_cache()?,
            // Only clears backing mmap storage if used, not in-memory representation
            NumericIndexInner::Immutable(index) => index.clear_cache()?,
            NumericIndexInner::Mmap(index) => index.clear_cache()?,
        }
        Ok(())
    }
}
