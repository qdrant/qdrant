use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use delegate::delegate;

use super::IdIter;
use super::mutable_bool_index::MutableBoolIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndex, ValueIndexer};
use crate::telemetry::PayloadIndexTelemetry;

pub struct ImmutableBoolIndex(MutableBoolIndex);

impl ImmutableBoolIndex {
    pub fn builder(path: &Path) -> OperationResult<ImmutableBoolIndexBuilder> {
        Ok(ImmutableBoolIndexBuilder(
            MutableBoolIndex::open(path, true)?.ok_or_else(|| {
                OperationError::service_error("Failed to create and open MutableBoolIndex")
            })?,
        ))
    }

    pub fn from_mutable(mutable_index: MutableBoolIndex) -> OperationResult<Self> {
        mutable_index.flusher()()?;
        Ok(Self(mutable_index))
    }

    pub fn open(path: &Path, deleted: &BitSlice) -> OperationResult<Option<Self>> {
        Ok(MutableBoolIndex::open_immutable(path, deleted)?.map(Self))
    }
}

impl ImmutableBoolIndex {
    // N.B.: these operations are immutable.
    delegate! {
        to self.0 {
            pub fn get_point_values(&self, point_id: PointOffsetType) -> Vec<bool>;
            pub fn iter_values_map<'a>(
                    &'a self,
                    hw_acc: &'a HardwareCounterCell,
            ) -> impl Iterator<Item = (bool, IdIter<'a>)> + 'a;
            pub fn iter_values(&self) -> impl Iterator<Item = bool> + '_;
            pub fn iter_counts_per_value(
                    &self,
                    deferred_internal_id: Option<PointOffsetType>,
            ) -> impl Iterator<Item = (bool, usize)> + '_ ;
            pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry;
            pub fn values_count(&self, point_id: PointOffsetType) -> usize;
            pub fn check_values_any(
                    &self,
                    point_id: PointOffsetType,
                    is_true: bool,
            ) -> bool;
            pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool;
            pub fn is_on_disk(&self) -> bool;
            pub fn populate(&self) -> OperationResult<()>;
            pub fn clear_cache(&self) -> OperationResult<()>;
        }
    }
}

impl ValueIndexer for ImmutableBoolIndex {
    type ValueType = bool;

    fn add_many(
        &mut self,
        _id: common::types::PointOffsetType,
        _values: Vec<Self::ValueType>,
        _hw_counter: &common::counter::hardware_counter::HardwareCounterCell,
    ) -> OperationResult<()> {
        unimplemented!("ImmutableBoolIndex is immutable, cannot add values");
    }

    #[inline]
    fn get_value(value: &serde_json::Value) -> Option<Self::ValueType> {
        value.as_bool()
    }

    #[inline]
    fn remove_point(&mut self, id: common::types::PointOffsetType) -> OperationResult<()> {
        self.0.set_or_insert_immutable(id, false, false);
        Ok(())
    }
}

impl PayloadFieldIndex for ImmutableBoolIndex {
    delegate! {
        to self.0 {
            fn count_indexed_points(&self) -> usize;
            fn wipe(self) -> OperationResult<()>;
            fn files(&self) -> Vec<PathBuf>;
            fn filter<'a>(
                    &'a self,
                    condition: &'a crate::types::FieldCondition,
                    hw_counter: &'a common::counter::hardware_counter::HardwareCounterCell,
            ) -> OperationResult<Option<Box<dyn Iterator<Item = common::types::PointOffsetType> + 'a>>>;
            fn estimate_cardinality(
                    &self,
                    condition: &crate::types::FieldCondition,
                    hw_counter: &common::counter::hardware_counter::HardwareCounterCell,
            ) -> OperationResult<Option<crate::index::field_index::CardinalityEstimation>>;
            fn payload_blocks(
                    &self,
                    threshold: usize,
                    key: crate::types::PayloadKeyType,
            ) -> Box<dyn Iterator<Item = OperationResult<crate::index::field_index::PayloadBlockCondition>> + '_>;
        }
    }

    #[inline]
    fn immutable_files(&self) -> Vec<PathBuf> {
        self.files() // All the files are immutable in this index.
    }

    #[inline]
    fn flusher(&self) -> crate::common::Flusher {
        Box::new(|| Ok(())) // No op for an immutable index.
    }
}

pub struct ImmutableBoolIndexBuilder(MutableBoolIndex);

impl ImmutableBoolIndexBuilder {}

impl FieldIndexBuilderTrait for ImmutableBoolIndexBuilder {
    type FieldIndexType = BoolIndex;

    fn init(&mut self) -> OperationResult<()> {
        // After Self is created, it is already initialized
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&serde_json::Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.0.add_point(id, payload, hw_counter)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(BoolIndex::Immutable(ImmutableBoolIndex(self.0)))
    }
}
