use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;

use super::mutable_null_index::MutableNullIndex;
use super::read_ops::{self, NullIndexRead};
use crate::common::flags::roaring_flags::RoaringFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    PayloadFieldIndexRead,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{FieldCondition, PayloadKeyType};

pub struct ImmutableNullIndex(MutableNullIndex);

impl ImmutableNullIndex {
    pub fn builder(
        path: &Path,
        total_point_count: usize,
    ) -> OperationResult<ImmutableNullIndexBuilder> {
        Ok(ImmutableNullIndexBuilder(
            MutableNullIndex::open(path, total_point_count, true)?.ok_or_else(|| {
                OperationError::service_error("Failed to create and open MutableNullIndex")
            })?,
        ))
    }

    pub fn from_mutable(mutable_index: MutableNullIndex) -> OperationResult<Self> {
        mutable_index.flusher()()?;
        Ok(Self(mutable_index))
    }

    pub fn open(
        path: &Path,
        total_point_count: usize,
        deleted: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        Ok(MutableNullIndex::open_immutable(path, total_point_count, deleted)?.map(Self))
    }

    #[inline]
    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.0.remove_point_immutable(id);
        Ok(())
    }
}

impl NullIndexRead for ImmutableNullIndex {
    type Flags = RoaringFlags<MmapFile>;

    fn has_values_flags(&self) -> &Self::Flags {
        self.0.has_values_flags()
    }

    fn is_null_flags(&self) -> &Self::Flags {
        self.0.is_null_flags()
    }

    fn total_point_count(&self) -> usize {
        self.0.total_point_count()
    }

    fn telemetry_index_type(&self) -> &'static str {
        "immutable_null_index"
    }
}

impl PayloadFieldIndexRead for ImmutableNullIndex {
    #[inline]
    fn count_indexed_points(&self) -> usize {
        self.indexed_points_count()
    }

    #[inline]
    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        _hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        Ok(read_ops::filter(self, condition))
    }

    #[inline]
    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        Ok(read_ops::estimate_cardinality(self, condition))
    }

    #[inline]
    fn for_each_payload_block(
        &self,
        _threshold: usize,
        _key: PayloadKeyType,
        _f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        // No payload blocks
        Ok(())
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        read_ops::condition_checker(self, condition, hw_acc)
    }
}

impl PayloadFieldIndex for ImmutableNullIndex {
    #[inline]
    fn wipe(self) -> OperationResult<()> {
        self.0.wipe()
    }

    #[inline]
    fn files(&self) -> Vec<PathBuf> {
        NullIndexRead::files(self)
    }

    #[inline]
    fn immutable_files(&self) -> Vec<PathBuf> {
        NullIndexRead::files(self) // All the files are immutable in this index.
    }

    #[inline]
    fn flusher(&self) -> crate::common::Flusher {
        Box::new(|| Ok(())) // No op for an immutable index.
    }
}

pub struct ImmutableNullIndexBuilder(MutableNullIndex);

impl FieldIndexBuilderTrait for ImmutableNullIndexBuilder {
    type FieldIndexType = ImmutableNullIndex;

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
        Ok(ImmutableNullIndex(self.0))
    }
}

#[cfg(test)]
mod tests {
    use common::bitvec::BitVec;
    use itertools::Itertools as _;
    use serde_json::{Value, json};
    use tempfile::TempDir;

    use super::*;
    use crate::json_path::JsonPath;
    use crate::types::FieldCondition;

    #[test]
    fn test_remove_idempotent() {
        let dir = TempDir::with_prefix("test_immutable_null_index").unwrap();
        let mut builder = ImmutableNullIndex::builder(dir.path(), 0).unwrap();
        let hw_counter = HardwareCounterCell::new();

        let null_value = Value::Null;
        let null_value_in_array =
            Value::Array(vec![Value::String("test".to_string()), Value::Null]);

        builder.add_point(0, &[&null_value], &hw_counter).unwrap();
        builder
            .add_point(1, &[&null_value_in_array], &hw_counter)
            .unwrap();
        builder.add_point(2, &[], &hw_counter).unwrap();
        builder
            .add_point(3, &[&Value::Bool(true)], &hw_counter)
            .unwrap();

        let mut index = builder.finalize().unwrap();

        let key = JsonPath::new("test");

        let filter_is_null = FieldCondition::new_is_null(key.clone(), true);

        let filter_is_not_empty = FieldCondition {
            key: key.clone(),
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
            is_empty: Some(false),
            is_null: None,
        };

        assert_eq!(
            index
                .filter(&filter_is_null, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![0, 1],
        );
        assert_eq!(
            index
                .filter(&filter_is_not_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![1, 3],
        );
        assert_eq!(
            index
                .estimate_cardinality(&filter_is_null, &hw_counter)
                .unwrap()
                .unwrap()
                .exp,
            2,
        );
        assert_eq!(
            index
                .estimate_cardinality(&filter_is_not_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .exp,
            2,
        );

        index.remove_point(1).unwrap();
        assert_eq!(
            index
                .filter(&filter_is_null, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![0],
        );
        assert_eq!(
            index
                .filter(&filter_is_not_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![3],
        );
        assert_eq!(
            index
                .estimate_cardinality(&filter_is_null, &hw_counter)
                .unwrap()
                .unwrap()
                .exp,
            1,
        );
        assert_eq!(
            index
                .estimate_cardinality(&filter_is_not_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .exp,
            1,
        );

        index.remove_point(1).unwrap();
        assert_eq!(
            index
                .filter(&filter_is_null, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![0],
        );
        assert_eq!(
            index
                .filter(&filter_is_not_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![3],
        );
        assert_eq!(
            index
                .estimate_cardinality(&filter_is_null, &hw_counter)
                .unwrap()
                .unwrap()
                .exp,
            1,
        );
        assert_eq!(
            index
                .estimate_cardinality(&filter_is_not_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .exp,
            1,
        );

        index.remove_point(3).unwrap();
        assert_eq!(
            index
                .filter(&filter_is_null, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![0],
        );
        assert!(
            index
                .filter(&filter_is_not_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec()
                .is_empty(),
        );
        assert_eq!(
            index
                .estimate_cardinality(&filter_is_null, &hw_counter)
                .unwrap()
                .unwrap()
                .exp,
            1,
        );
        assert_eq!(
            index
                .estimate_cardinality(&filter_is_not_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .exp,
            0,
        );

        index.remove_point(3).unwrap();
        assert_eq!(
            index
                .filter(&filter_is_null, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![0],
        );
        assert!(
            index
                .filter(&filter_is_not_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec()
                .is_empty(),
        );
        assert_eq!(
            index
                .estimate_cardinality(&filter_is_null, &hw_counter)
                .unwrap()
                .unwrap()
                .exp,
            1,
        );
        assert_eq!(
            index
                .estimate_cardinality(&filter_is_not_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .exp,
            0,
        );
    }

    #[test]
    fn test_remove_reopen() {
        let dir = TempDir::with_prefix("test_immutable_null_index").unwrap();
        let mut builder = ImmutableNullIndex::builder(dir.path(), 0).unwrap();
        let hw_counter = HardwareCounterCell::new();
        builder.add_point(0, &[&json!(true)], &hw_counter).unwrap();
        builder.add_point(1, &[&json!(true)], &hw_counter).unwrap();
        builder.add_point(2, &[&json!(false)], &hw_counter).unwrap();

        let index = builder.finalize().unwrap();

        let mut deleted = BitVec::repeat(false, 3);
        deleted.set(1, true);
        drop(index);

        let reopened_index = ImmutableNullIndex::open(dir.path(), 3, &deleted)
            .unwrap()
            .unwrap();

        let filter_is_not_empty = FieldCondition {
            key: JsonPath::new("test"),
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
            is_empty: Some(false),
            is_null: None,
        };

        assert_eq!(
            reopened_index
                .filter(&filter_is_not_empty, &hw_counter)
                .unwrap()
                .unwrap()
                .collect_vec(),
            vec![0, 2],
        );
        assert_eq!(reopened_index.count_indexed_points(), 3);

        // Direct API parity: deleted point reads as empty; live points don't.
        assert!(reopened_index.values_is_empty(1));
        assert!(!reopened_index.values_is_empty(0));
        assert!(!reopened_index.values_is_empty(2));
    }
}
