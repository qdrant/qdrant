use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use mmap_bool_index::MmapBoolIndex;
use simple_bool_index::SimpleBoolIndex;

use super::facet_index::FacetIndex;
use super::map_index::IdIter;
use super::{PayloadFieldIndex, ValueIndexer};
use crate::common::operation_error::OperationResult;
use crate::data_types::facets::{FacetHit, FacetValueRef};
use crate::telemetry::PayloadIndexTelemetry;

pub mod mmap_bool_index;
pub mod simple_bool_index;

pub enum BoolIndex {
    Simple(SimpleBoolIndex),
    Mmap(MmapBoolIndex),
}

impl BoolIndex {
    pub fn get_point_values(&self, point_id: PointOffsetType) -> Vec<bool> {
        match self {
            BoolIndex::Simple(index) => index.get_point_values(point_id),
            BoolIndex::Mmap(index) => index.get_point_values(point_id),
        }
    }

    pub fn iter_values_map<'a>(
        &'a self,
        hw_acc: &'a HardwareCounterCell,
    ) -> Box<dyn Iterator<Item = (bool, IdIter<'a>)> + 'a> {
        match self {
            BoolIndex::Simple(index) => Box::new(index.iter_values_map()),
            BoolIndex::Mmap(index) => Box::new(index.iter_values_map(hw_acc)),
        }
    }

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = bool> + '_> {
        match self {
            BoolIndex::Simple(index) => Box::new(index.iter_values()),
            BoolIndex::Mmap(index) => Box::new(index.iter_values()),
        }
    }

    pub fn iter_counts_per_value(&self) -> Box<dyn Iterator<Item = (bool, usize)> + '_> {
        match self {
            BoolIndex::Simple(index) => Box::new(index.iter_counts_per_value()),
            BoolIndex::Mmap(index) => Box::new(index.iter_counts_per_value()),
        }
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        match self {
            BoolIndex::Simple(index) => index.get_telemetry_data(),
            BoolIndex::Mmap(index) => index.get_telemetry_data(),
        }
    }

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        match self {
            BoolIndex::Simple(index) => index.values_count(point_id),
            BoolIndex::Mmap(index) => index.values_count(point_id),
        }
    }

    pub fn check_values_any(
        &self,
        point_id: PointOffsetType,
        is_true: bool,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        match self {
            BoolIndex::Simple(index) => index.check_values_any(point_id, is_true),
            BoolIndex::Mmap(index) => index.check_values_any(point_id, is_true, hw_counter),
        }
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        match self {
            BoolIndex::Simple(index) => index.values_is_empty(point_id),
            BoolIndex::Mmap(index) => index.values_is_empty(point_id),
        }
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            BoolIndex::Simple(_) => false,
            BoolIndex::Mmap(index) => index.is_on_disk(),
        }
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            BoolIndex::Simple(_) => {} // Not a mmap
            BoolIndex::Mmap(index) => index.populate()?,
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            BoolIndex::Simple(_) => {} // Not a mmap
            BoolIndex::Mmap(index) => index.clear_cache()?,
        }
        Ok(())
    }
}

impl PayloadFieldIndex for BoolIndex {
    fn count_indexed_points(&self) -> usize {
        match self {
            BoolIndex::Simple(index) => index.count_indexed_points(),
            BoolIndex::Mmap(index) => index.count_indexed_points(),
        }
    }

    fn load(&mut self) -> crate::common::operation_error::OperationResult<bool> {
        match self {
            BoolIndex::Simple(index) => index.load(),
            BoolIndex::Mmap(index) => index.load(),
        }
    }

    fn cleanup(self) -> crate::common::operation_error::OperationResult<()> {
        match self {
            BoolIndex::Simple(index) => index.cleanup(),
            BoolIndex::Mmap(index) => index.cleanup(),
        }
    }

    fn flusher(&self) -> crate::common::Flusher {
        match self {
            BoolIndex::Simple(index) => index.flusher(),
            BoolIndex::Mmap(index) => index.flusher(),
        }
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        match self {
            BoolIndex::Simple(index) => index.files(),
            BoolIndex::Mmap(index) => index.files(),
        }
    }

    fn filter<'a>(
        &'a self,
        condition: &'a crate::types::FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match self {
            BoolIndex::Simple(index) => index.filter(condition, hw_counter),
            BoolIndex::Mmap(index) => index.filter(condition, hw_counter),
        }
    }

    fn estimate_cardinality(
        &self,
        condition: &crate::types::FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> Option<super::CardinalityEstimation> {
        match self {
            BoolIndex::Simple(index) => index.estimate_cardinality(condition, hw_counter),
            BoolIndex::Mmap(index) => index.estimate_cardinality(condition, hw_counter),
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: crate::types::PayloadKeyType,
    ) -> Box<dyn Iterator<Item = super::PayloadBlockCondition> + '_> {
        match self {
            BoolIndex::Simple(index) => index.payload_blocks(threshold, key),
            BoolIndex::Mmap(index) => index.payload_blocks(threshold, key),
        }
    }
}

impl FacetIndex for BoolIndex {
    fn get_point_values(
        &self,
        point_id: PointOffsetType,
    ) -> impl Iterator<Item = FacetValueRef> + '_ {
        self.get_point_values(point_id)
            .into_iter()
            .map(FacetValueRef::Bool)
    }

    fn iter_values(&self) -> impl Iterator<Item = FacetValueRef<'_>> + '_ {
        self.iter_values().map(FacetValueRef::Bool)
    }

    fn iter_values_map<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> impl Iterator<Item = (FacetValueRef<'a>, IdIter<'a>)> + 'a {
        self.iter_values_map(hw_counter)
            .map(|(value, iter)| (FacetValueRef::Bool(value), iter))
    }

    fn iter_counts_per_value(&self) -> impl Iterator<Item = FacetHit<FacetValueRef<'_>>> + '_ {
        self.iter_counts_per_value().map(|(value, count)| FacetHit {
            value: FacetValueRef::Bool(value),
            count,
        })
    }
}

impl ValueIndexer for BoolIndex {
    type ValueType = bool;

    fn add_many(
        &mut self,
        id: common::types::PointOffsetType,
        values: Vec<Self::ValueType>,
        hw_counter: &HardwareCounterCell,
    ) -> crate::common::operation_error::OperationResult<()> {
        match self {
            BoolIndex::Simple(index) => index.add_many(id, values, hw_counter),
            BoolIndex::Mmap(index) => index.add_many(id, values, hw_counter),
        }
    }

    fn get_value(value: &serde_json::Value) -> Option<Self::ValueType> {
        match value {
            serde_json::Value::Bool(value) => Some(*value),
            _ => None,
        }
    }

    fn remove_point(
        &mut self,
        id: common::types::PointOffsetType,
    ) -> crate::common::operation_error::OperationResult<()> {
        match self {
            BoolIndex::Simple(index) => index.remove_point(id),
            BoolIndex::Mmap(index) => index.remove_point(id),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use common::counter::hardware_counter::HardwareCounterCell;
    use itertools::Itertools;
    use rstest::rstest;
    use serde_json::json;
    use tempfile::Builder;

    use super::BoolIndex;
    use super::mmap_bool_index::MmapBoolIndex;
    use super::simple_bool_index::SimpleBoolIndex;
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
    use crate::index::field_index::{FieldIndexBuilderTrait as _, PayloadFieldIndex, ValueIndexer};
    use crate::json_path::JsonPath;

    const FIELD_NAME: &str = "bool_field";
    const DB_NAME: &str = "test_db";

    trait OpenIndex {
        fn open_at(path: &Path) -> BoolIndex;
    }

    impl OpenIndex for SimpleBoolIndex {
        fn open_at(path: &Path) -> BoolIndex {
            let db = open_db_with_existing_cf(path).unwrap();
            let mut index = SimpleBoolIndex::new(db.clone(), FIELD_NAME);
            // Try to load if it exists
            if index.load().unwrap() {
                return BoolIndex::Simple(index);
            }
            drop(index);

            // Otherwise create a new one
            SimpleBoolIndex::builder(db, FIELD_NAME)
                .make_empty()
                .unwrap()
        }
    }

    impl OpenIndex for MmapBoolIndex {
        fn open_at(path: &Path) -> BoolIndex {
            MmapBoolIndex::builder(path, false)
                .unwrap()
                .make_empty()
                .unwrap()
        }
    }

    fn match_bool(value: bool) -> crate::types::FieldCondition {
        crate::types::FieldCondition::new_match(
            JsonPath::new(FIELD_NAME),
            crate::types::Match::Value(crate::types::MatchValue {
                value: crate::types::ValueVariants::Bool(value),
            }),
        )
    }

    fn bools_fixture() -> Vec<serde_json::Value> {
        vec![
            json!(true),
            json!(false),
            json!([true, false]),
            json!([false, true]),
            json!([true, true]),
            json!([false, false]),
            json!([true, false, true]),
            serde_json::Value::Null,
            json!(1),
            json!("test"),
            json!([false]),
            json!([true]),
        ]
    }

    fn filter<I: OpenIndex>(given: serde_json::Value, match_on: bool, expected_count: usize) {
        let tmp_dir = Builder::new().prefix(DB_NAME).tempdir().unwrap();
        let mut index = I::open_at(tmp_dir.path());

        let hw_counter = HardwareCounterCell::new();

        index.add_point(0, &[&given], &hw_counter).unwrap();

        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();
        let count = index
            .filter(&match_bool(match_on), &hw_counter)
            .unwrap()
            .count();

        assert_eq!(count, expected_count);
    }

    #[rstest]
    #[case(json!(true), 1)]
    #[case(json!(false), 0)]
    #[case(json!([true]), 1)]
    #[case(json!([false]), 0)]
    #[case(json!([true, false]), 1)]
    #[case(json!([false, true]), 1)]
    #[case(json!([false, false]), 0)]
    #[case(json!([true, true]), 1)]
    fn test_filter_true(#[case] given: serde_json::Value, #[case] expected_count: usize) {
        filter::<SimpleBoolIndex>(given.clone(), true, expected_count);
        filter::<MmapBoolIndex>(given, true, expected_count);
    }

    #[rstest]
    #[case(json!(true), 0)]
    #[case(json!(false), 1)]
    #[case(json!([true]), 0)]
    #[case(json!([false]), 1)]
    #[case(json!([true, false]), 1)]
    #[case(json!([false, true]), 1)]
    #[case(json!([false, false]), 1)]
    #[case(json!([true, true]), 0)]
    fn test_filter_false(#[case] given: serde_json::Value, #[case] expected_count: usize) {
        filter::<SimpleBoolIndex>(given.clone(), false, expected_count);
        filter::<MmapBoolIndex>(given, false, expected_count);
    }

    #[test]
    fn test_load_from_disk() {
        load_from_disk::<SimpleBoolIndex>();
        load_from_disk::<MmapBoolIndex>();
    }

    fn load_from_disk<I: OpenIndex>() {
        let tmp_dir = Builder::new().prefix(DB_NAME).tempdir().unwrap();
        let mut index = I::open_at(tmp_dir.path());

        let hw_counter = HardwareCounterCell::new();

        bools_fixture()
            .into_iter()
            .enumerate()
            .for_each(|(i, value)| {
                index.add_point(i as u32, &[&value], &hw_counter).unwrap();
            });

        index.flusher()().unwrap();

        drop(index);

        let mut new_index = I::open_at(tmp_dir.path());
        assert!(new_index.load().unwrap());

        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();
        let point_offsets = new_index
            .filter(&match_bool(false), &hw_counter)
            .unwrap()
            .collect_vec();
        assert_eq!(point_offsets, vec![1, 2, 3, 5, 6, 10]);

        let point_offsets = new_index
            .filter(&match_bool(true), &hw_counter)
            .unwrap()
            .collect_vec();
        assert_eq!(point_offsets, vec![0, 2, 3, 4, 6, 11]);

        assert_eq!(new_index.count_indexed_points(), 9);
    }

    #[rstest]
    #[case(json!(false), json!(true))]
    #[case(json!([false, true]), json!(true))]
    fn test_modify_value(#[case] before: serde_json::Value, #[case] after: serde_json::Value) {
        modify_value::<SimpleBoolIndex>(before.clone(), after.clone());
        modify_value::<MmapBoolIndex>(before, after);
    }

    /// Try to modify from falsy to only true
    fn modify_value<I: OpenIndex>(before: serde_json::Value, after: serde_json::Value) {
        let tmp_dir = Builder::new().prefix(DB_NAME).tempdir().unwrap();
        let mut index = I::open_at(tmp_dir.path());

        let hw_cell = HardwareCounterCell::new();

        let idx = 1000;
        index.add_point(idx, &[&before], &hw_cell).unwrap();

        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();

        let point_offsets = index
            .filter(&match_bool(false), &hw_counter)
            .unwrap()
            .collect_vec();
        assert_eq!(point_offsets, vec![idx]);

        index.add_point(idx, &[&after], &hw_cell).unwrap();

        let point_offsets = index
            .filter(&match_bool(true), &hw_counter)
            .unwrap()
            .collect_vec();
        assert_eq!(point_offsets, vec![idx]);
        let point_offsets = index
            .filter(&match_bool(false), &hw_counter)
            .unwrap()
            .collect_vec();
        assert!(point_offsets.is_empty());
    }

    #[test]
    fn test_indexed_count() {
        indexed_count::<SimpleBoolIndex>();
        indexed_count::<MmapBoolIndex>();
    }

    fn indexed_count<I: OpenIndex>() {
        let tmp_dir = Builder::new().prefix(DB_NAME).tempdir().unwrap();
        let mut index = I::open_at(tmp_dir.path());

        let hw_counter = HardwareCounterCell::new();

        bools_fixture()
            .into_iter()
            .enumerate()
            .for_each(|(i, value)| {
                index.add_point(i as u32, &[&value], &hw_counter).unwrap();
            });

        assert_eq!(index.count_indexed_points(), 9);
    }

    #[test]
    fn test_payload_blocks() {
        payload_blocks::<SimpleBoolIndex>();
        payload_blocks::<MmapBoolIndex>();
    }

    fn payload_blocks<I: OpenIndex>() {
        let tmp_dir = Builder::new().prefix(DB_NAME).tempdir().unwrap();
        let mut index = I::open_at(tmp_dir.path());

        let hw_counter = HardwareCounterCell::new();

        bools_fixture()
            .into_iter()
            .enumerate()
            .for_each(|(i, value)| {
                index.add_point(i as u32, &[&value], &hw_counter).unwrap();
            });

        let blocks = index
            .payload_blocks(0, JsonPath::new(FIELD_NAME))
            .collect_vec();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].cardinality, 6);
        assert_eq!(blocks[1].cardinality, 6);
    }

    #[test]
    fn test_estimate_cardinality() {
        estimate_cardinality::<SimpleBoolIndex>();
        estimate_cardinality::<MmapBoolIndex>();
    }

    fn estimate_cardinality<I: OpenIndex>() {
        let tmp_dir = Builder::new().prefix(DB_NAME).tempdir().unwrap();
        let mut index = I::open_at(tmp_dir.path());

        let hw_counter = HardwareCounterCell::new();

        bools_fixture()
            .into_iter()
            .enumerate()
            .for_each(|(i, value)| {
                index.add_point(i as u32, &[&value], &hw_counter).unwrap();
            });

        let hw_counter = HardwareCounterCell::new();

        let cardinality = index
            .estimate_cardinality(&match_bool(true), &hw_counter)
            .unwrap();
        assert_eq!(cardinality.exp, 6);

        let cardinality = index
            .estimate_cardinality(&match_bool(false), &hw_counter)
            .unwrap();
        assert_eq!(cardinality.exp, 6);
    }
}
