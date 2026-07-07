//! Construction, persistence, mutation, and cache-control forwarding for
//! [`NumericIndexInner`].

use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, Populate};
use gridstore::Blob;

use super::super::Encodable;
use super::super::immutable_numeric_index::ImmutableNumericIndex;
use super::super::mutable_numeric_index::MutableNumericIndex;
use super::super::on_disk_numeric_index::OnDiskNumericIndex;
use super::NumericIndexInner;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::types::Memory;

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default> NumericIndexInner<T>
where
    Vec<T>: Blob,
{
    /// Load immutable mmap based index, either in RAM or on disk
    pub fn new_mmap(
        path: &Path,
        memory: Memory,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        // Low-memory mode degrades the placement at load time (pinned falls back to the
        // pure-mmap `Storage` variant). Files are shared between variants; the persisted
        // configuration is untouched.
        let memory = memory.clamp_to_low_memory();

        let populate = Populate::from(memory.populate_on_open());
        let Some(on_disk_index) =
            OnDiskNumericIndex::open(&MmapFs, path, populate, deleted_points)?
        else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        if memory.is_heap() {
            // Load into RAM, use on-disk as backing storage
            Ok(Some(NumericIndexInner::Immutable(
                ImmutableNumericIndex::load_from_on_disk(on_disk_index),
            )))
        } else {
            // Use on-disk directly
            Ok(Some(NumericIndexInner::OnDisk(on_disk_index)))
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
            NumericIndexInner::OnDisk(index) => index.flusher(),
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self {
            NumericIndexInner::Mutable(index) => index.files(),
            NumericIndexInner::Immutable(index) => index.files(),
            NumericIndexInner::OnDisk(index) => index.files(),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            NumericIndexInner::Mutable(_) => vec![],
            NumericIndexInner::Immutable(index) => index.immutable_files(),
            NumericIndexInner::OnDisk(index) => index.immutable_files(),
        }
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        match self {
            NumericIndexInner::Mutable(index) => index.remove_point(idx)?,
            NumericIndexInner::Immutable(index) => index.remove_point(idx),
            NumericIndexInner::OnDisk(index) => index.remove_point(idx),
        }
        Ok(())
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            NumericIndexInner::Mutable(_) => {}   // Not a mmap
            NumericIndexInner::Immutable(_) => {} // Not a mmap
            NumericIndexInner::OnDisk(index) => index.populate()?,
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            // Only clears backing on-disk storage if used, not in-memory representation
            NumericIndexInner::Mutable(index) => index.clear_cache()?,
            // Only clears backing on-disk storage if used, not in-memory representation
            NumericIndexInner::Immutable(index) => index.clear_cache()?,
            NumericIndexInner::OnDisk(index) => index.clear_cache()?,
        }
        Ok(())
    }
}
