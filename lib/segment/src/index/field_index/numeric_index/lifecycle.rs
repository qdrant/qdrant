//! Construction, cache control, and storage-introspection surface for
//! [`NumericIndex`], plus the histogram seed constants.

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;

use super::numeric_index_read::NumericIndexRead;
use super::storage::NumericIndexInner;
use super::{
    NumericIndex, NumericIndexGridstoreBuilder, NumericIndexIntoInnerValue,
    NumericIndexMmapBuilder, NumericIndexValue,
};
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{PayloadFieldIndex, ValueIndexer};
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::Memory;

pub(super) const HISTOGRAM_MAX_BUCKET_SIZE: usize = 10_000;
pub(super) const HISTOGRAM_PRECISION: f64 = 0.01;

impl<T: NumericIndexValue, P> NumericIndex<T, P>
where
    Vec<T>: Blob,
{
    /// Load immutable mmap based index, either in RAM or on disk
    pub fn new_immutable(
        path: &Path,
        memory: Memory,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let index = NumericIndexInner::new_mmap(path, memory, deleted_points)?;

        Ok(index.map(|inner| Self {
            inner,
            _phantom: PhantomData,
        }))
    }

    pub fn new_mutable(dir: PathBuf, create_if_missing: bool) -> OperationResult<Option<Self>> {
        let index = NumericIndexInner::new_gridstore(dir, create_if_missing)?;

        Ok(index.map(|inner| Self {
            inner,
            _phantom: PhantomData,
        }))
    }

    pub fn builder_mmap(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> NumericIndexMmapBuilder<T, P>
    where
        Self: ValueIndexer<ValueType = P> + NumericIndexIntoInnerValue<T, P>,
    {
        NumericIndexMmapBuilder::new(path.to_owned(), is_on_disk, deleted_points.to_owned())
    }

    pub fn builder_gridstore(dir: PathBuf) -> NumericIndexGridstoreBuilder<T, P>
    where
        Self: ValueIndexer<ValueType = P>,
    {
        NumericIndexGridstoreBuilder::new(dir)
    }

    pub fn inner(&self) -> &NumericIndexInner<T> {
        &self.inner
    }

    pub fn mut_inner(&mut self) -> &mut NumericIndexInner<T> {
        &mut self.inner
    }

    pub fn get_mutability_type(&self) -> IndexMutability {
        match &self.inner {
            NumericIndexInner::Mutable(_) => IndexMutability::Mutable,
            NumericIndexInner::Immutable(_) => IndexMutability::Immutable,
            NumericIndexInner::OnDisk(_) => IndexMutability::Immutable,
        }
    }

    pub fn get_storage_type(&self) -> StorageType {
        match &self.inner {
            NumericIndexInner::Mutable(index) => index.storage_type(),
            NumericIndexInner::Immutable(index) => index.storage_type(),
            NumericIndexInner::OnDisk(index) => StorageType::Mmap {
                is_on_disk: index.is_on_disk(),
            },
        }
    }

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&T) -> bool,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        self.inner.check_values_any(idx, check_fn, hw_counter)
    }

    pub fn wipe(self) -> OperationResult<()> {
        self.inner.wipe()
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        self.inner.get_telemetry_data()
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        self.inner.values_count(idx).unwrap_or_default()
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        self.inner.get_values(idx)
    }

    pub fn values_is_empty(&self, idx: PointOffsetType) -> bool {
        self.inner.values_is_empty(idx)
    }

    pub fn is_on_disk(&self) -> bool {
        self.inner.is_on_disk()
    }

    pub fn populate(&self) -> OperationResult<()> {
        self.inner.populate()
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        self.inner.clear_cache()
    }
}
