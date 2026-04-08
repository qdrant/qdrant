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
        _id: PointOffsetType,
        _values: Vec<Self::ValueType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(OperationError::service_error(
            "ImmutableBoolIndex is immutable, cannot add values",
        ))
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&serde_json::Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        Err(OperationError::service_error(
            "ImmutableBoolIndex is immutable, cannot add values",
        ))
    }

    #[inline]
    fn get_value(value: &serde_json::Value) -> Option<Self::ValueType> {
        value.as_bool()
    }

    #[inline]
    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
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
                    hw_counter: &'a HardwareCounterCell,
            ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>>;
            fn estimate_cardinality(
                    &self,
                    condition: &crate::types::FieldCondition,
                    hw_counter: &HardwareCounterCell,
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

impl FieldIndexBuilderTrait for ImmutableBoolIndexBuilder {
    type FieldIndexType = ImmutableBoolIndex;

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
        self.0.flusher()()?; // Immutable index has noop flusher, so we have to ensure the data is flushed now.
        Ok(ImmutableBoolIndex(self.0))
    }
}

#[cfg(test)]
mod tests {
    use common::bitvec::BitVec;
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_remove_idempotent() {
        let dir = TempDir::with_prefix("test_immutable_bool_index").unwrap();
        let mut builder = ImmutableBoolIndex::builder(dir.path()).unwrap();
        let hw_counter = HardwareCounterCell::new();
        builder.add_point(0, &[&json!(true)], &hw_counter).unwrap();
        builder.add_point(1, &[&json!(true)], &hw_counter).unwrap();
        builder.add_point(2, &[&json!(false)], &hw_counter).unwrap();

        let mut index = builder.finalize().unwrap();
        assert_eq!(index.get_point_values(1), vec![true; 1]);
        assert_eq!(index.count_indexed_points(), 3);

        index.remove_point(1).unwrap();
        assert_eq!(index.get_point_values(1), vec![true; 0]);
        assert_eq!(index.count_indexed_points(), 2);

        index.remove_point(1).unwrap();
        assert_eq!(index.get_point_values(1), vec![true; 0]);
        assert_eq!(index.count_indexed_points(), 2);
    }

    #[test]
    fn test_remove_reopen() {
        let dir = TempDir::with_prefix("test_immutable_bool_index").unwrap();
        let mut builder = ImmutableBoolIndex::builder(dir.path()).unwrap();
        let hw_counter = HardwareCounterCell::new();
        builder.add_point(0, &[&json!(true)], &hw_counter).unwrap();
        builder.add_point(1, &[&json!(true)], &hw_counter).unwrap();
        builder.add_point(2, &[&json!(false)], &hw_counter).unwrap();

        let mut index = builder.finalize().unwrap();

        let mut deleted = BitVec::repeat(false, 3);
        deleted.set(1, true);
        index.remove_point(1).unwrap();
        drop(index);

        let opened_index = ImmutableBoolIndex::open(dir.path(), &deleted)
            .unwrap()
            .unwrap();
        assert_eq!(opened_index.get_point_values(1), vec![true; 0]);
        assert_eq!(opened_index.count_indexed_points(), 2);
    }
}
