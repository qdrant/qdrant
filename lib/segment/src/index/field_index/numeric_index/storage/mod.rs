//! Storage-variant dispatch for [`NumericIndex`].
//!
//! `NumericIndexInner<T>` is the enum that selects across the three
//! storage backends (`Mutable`, `Immutable`, `Mmap`) and forwards each
//! operation to the active variant. Logic that does more than forward
//! (cardinality math, histogram-driven payload blocks, filter
//! evaluation) lives in the sibling modules:
//!
//! - [`statistics`]: histogram-based cardinality and point-count helpers.
//! - [`trait_impls`]: [`PayloadFieldIndex`], [`PayloadFieldIndexRead`],
//!   and [`StreamRange`] implementations.
//!
//! [`NumericIndex`]: super::NumericIndex
//! [`PayloadFieldIndex`]: crate::index::field_index::PayloadFieldIndex
//! [`PayloadFieldIndexRead`]: crate::index::field_index::PayloadFieldIndexRead
//! [`StreamRange`]: super::StreamRange

mod statistics;
mod trait_impls;

use std::ops::Bound;
use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;

use super::Encodable;
use super::immutable_numeric_index::ImmutableNumericIndex;
use super::mmap_numeric_index::MmapNumericIndex;
use super::mutable_numeric_index::MutableNumericIndex;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::stored_point_to_values::StoredValue;
use crate::telemetry::PayloadIndexTelemetry;

pub enum NumericIndexInner<T: Encodable + Numericable + StoredValue + Send + Sync + Default>
where
    Vec<T>: Blob,
{
    Mutable(MutableNumericIndex<T>),
    Immutable(ImmutableNumericIndex<T>),
    Mmap(MmapNumericIndex<T>),
}

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

        let Some(mmap_index) = MmapNumericIndex::open(path, effective_is_on_disk, deleted_points)?
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

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&T) -> bool,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        match self {
            NumericIndexInner::Mutable(index) => index.check_values_any(idx, check_fn),
            NumericIndexInner::Immutable(index) => index.check_values_any(idx, check_fn),
            // FIXME: don't silently ignore error, change output of this function and propagate
            NumericIndexInner::Mmap(index) => index
                .check_values_any(idx, check_fn, hw_counter)
                .unwrap_or(false),
        }
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        match self {
            NumericIndexInner::Mutable(index) => index.get_values(idx),
            NumericIndexInner::Immutable(index) => index.get_values(idx),
            NumericIndexInner::Mmap(index) => index.get_values(idx),
        }
    }

    pub fn point_ids_by_value<'a>(
        &'a self,
        value: T,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        let start = Bound::Included(Point::new(value, PointOffsetType::MIN));
        let end = Bound::Included(Point::new(value, PointOffsetType::MAX));
        Ok(match &self {
            NumericIndexInner::Mutable(mutable) => Box::new(mutable.values_range(start, end)),
            NumericIndexInner::Immutable(immutable) => Box::new(immutable.values_range(start, end)),
            NumericIndexInner::Mmap(mmap) => Box::new(mmap.values_range(start, end, hw_counter)?),
        })
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        match self {
            NumericIndexInner::Mutable(index) => index.values_count(idx).unwrap_or_default(),
            NumericIndexInner::Immutable(index) => index.values_count(idx).unwrap_or_default(),
            NumericIndexInner::Mmap(index) => index.values_count(idx).unwrap_or_default(),
        }
    }

    /// Maximum number of values per point
    ///
    /// # Warning
    ///
    /// Zero if the index is empty.
    pub fn max_values_per_point(&self) -> usize {
        match self {
            NumericIndexInner::Mutable(index) => index.get_max_values_per_point(),
            NumericIndexInner::Immutable(index) => index.get_max_values_per_point(),
            NumericIndexInner::Mmap(index) => index.get_max_values_per_point(),
        }
    }

    pub fn values_is_empty(&self, idx: PointOffsetType) -> bool {
        self.values_count(idx) == 0
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.get_points_count(),
            points_values_count: self.get_histogram().get_total_count(),
            histogram_bucket_size: Some(self.get_histogram().current_bucket_size()),
            index_type: match self {
                NumericIndexInner::Mutable(_) => "mutable_numeric",
                NumericIndexInner::Immutable(_) => "immutable_numeric",
                NumericIndexInner::Mmap(_) => "mmap_numeric",
            },
        }
    }

    /// Approximate RAM usage in bytes for in-memory structures.
    pub fn ram_usage_bytes(&self) -> usize {
        match self {
            NumericIndexInner::Mutable(index) => index.ram_usage_bytes(),
            NumericIndexInner::Immutable(index) => index.ram_usage_bytes(),
            NumericIndexInner::Mmap(index) => index.ram_usage_bytes(),
        }
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            NumericIndexInner::Mutable(_) => false,
            NumericIndexInner::Immutable(_) => false,
            NumericIndexInner::Mmap(index) => index.is_on_disk(),
        }
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
