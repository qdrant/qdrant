use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;
use serde_json::Value;

use super::storage::NumericIndexInner;
use super::{Encodable, NumericIndexGridstoreBuilder, NumericIndexMmapBuilder};
use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::stored_point_to_values::StoredValue;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PayloadFieldIndexRead,
    ValueIndexer,
};
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, PayloadKeyType};

pub struct NumericIndex<T: Encodable + Numericable + StoredValue + Send + Sync + Default, P>
where
    Vec<T>: Blob,
{
    pub(super) inner: NumericIndexInner<T>,
    pub(super) _phantom: PhantomData<P>,
}

pub trait NumericIndexIntoInnerValue<T, P> {
    fn into_inner_value(value: P) -> T;
}

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default, P> NumericIndex<T, P>
where
    Vec<T>: Blob,
{
    /// Load immutable mmap based index, either in RAM or on disk
    pub fn new_mmap(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let index = NumericIndexInner::new_mmap(path, is_on_disk, deleted_points)?;

        Ok(index.map(|inner| Self {
            inner,
            _phantom: PhantomData,
        }))
    }

    pub fn new_gridstore(dir: PathBuf, create_if_missing: bool) -> OperationResult<Option<Self>> {
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
            NumericIndexInner::Mmap(_) => IndexMutability::Immutable,
        }
    }

    pub fn get_storage_type(&self) -> StorageType {
        match &self.inner {
            NumericIndexInner::Mutable(index) => index.storage_type(),
            NumericIndexInner::Immutable(index) => index.storage_type(),
            NumericIndexInner::Mmap(index) => StorageType::Mmap {
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
        self.inner.values_count(idx)
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

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default, P> PayloadFieldIndexRead
    for NumericIndex<T, P>
where
    Vec<T>: Blob,
{
    fn count_indexed_points(&self) -> usize {
        self.inner.count_indexed_points()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        self.inner.filter(condition, hw_counter)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        self.inner.estimate_cardinality(condition, hw_counter)
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.inner.for_each_payload_block(threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        self.inner.condition_checker(condition, hw_acc)
    }

    fn special_check_condition(
        &self,
        condition: &FieldCondition,
        payload_value: &Value,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<bool>> {
        self.inner
            .special_check_condition(condition, payload_value, hw_counter)
    }
}
