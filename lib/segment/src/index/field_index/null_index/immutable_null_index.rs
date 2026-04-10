use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use delegate::delegate;

use super::mutable_null_index::MutableNullIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndex};
use crate::telemetry::PayloadIndexTelemetry;

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
        self.0.remove_point_immutable(id)
    }
}

impl ImmutableNullIndex {
    // N.B.: these operations are immutable.
    delegate! {
        to self.0 {
            pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry;
            pub fn values_count(&self, point_id: PointOffsetType) -> usize;
            pub fn values_is_empty(&self, id: PointOffsetType) -> bool;
            pub fn values_is_null(&self, id: PointOffsetType) -> bool;
            pub fn is_on_disk(&self) -> bool;
            pub fn populate(&self) -> OperationResult<()>;
            pub fn clear_cache(&self) -> OperationResult<()>;
        }
    }
}

impl PayloadFieldIndex for ImmutableNullIndex {
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

        for i in 0..4 {
            match i % 4 {
                0 => builder.add_point(i, &[&null_value], &hw_counter).unwrap(),
                1 => builder
                    .add_point(i, &[&null_value_in_array], &hw_counter)
                    .unwrap(),
                2 => builder.add_point(i, &[], &hw_counter).unwrap(),
                3 => builder
                    .add_point(i, &[&Value::Bool(true)], &hw_counter)
                    .unwrap(),
                _ => unreachable!(),
            }
        }

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

        let mut index = builder.finalize().unwrap();

        let mut deleted = BitVec::repeat(false, 3);
        deleted.set(1, true);
        index.remove_point(1).unwrap();
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
    }
}
