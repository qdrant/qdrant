use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use immutable_bool_index::ImmutableBoolIndex;
use mutable_bool_index::MutableBoolIndex;

use super::facet_index::FacetIndex;
use super::map_index::IdIter;
use super::{PayloadFieldIndex, ValueIndexer};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::facets::{FacetHit, FacetValueRef};
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::telemetry::PayloadIndexTelemetry;

pub mod immutable_bool_index;
pub mod mutable_bool_index;

pub enum BoolIndex {
    Mmap(MutableBoolIndex),
    Immutable(ImmutableBoolIndex),
}

impl BoolIndex {
    pub fn get_point_values(&self, point_id: PointOffsetType) -> Vec<bool> {
        match self {
            BoolIndex::Mmap(index) => index.get_point_values(point_id),
            BoolIndex::Immutable(index) => index.get_point_values(point_id),
        }
    }

    pub fn iter_values_map<'a>(
        &'a self,
        hw_acc: &'a HardwareCounterCell,
    ) -> Box<dyn Iterator<Item = (bool, IdIter<'a>)> + 'a> {
        match self {
            BoolIndex::Mmap(index) => Box::new(index.iter_values_map(hw_acc)),
            BoolIndex::Immutable(index) => Box::new(index.iter_values_map(hw_acc)),
        }
    }

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = bool> + '_> {
        match self {
            BoolIndex::Mmap(index) => Box::new(index.iter_values()),
            BoolIndex::Immutable(index) => Box::new(index.iter_values()),
        }
    }

    pub fn iter_counts_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> Box<dyn Iterator<Item = (bool, usize)> + '_> {
        match self {
            BoolIndex::Mmap(index) => Box::new(index.iter_counts_per_value(deferred_internal_id)),
            BoolIndex::Immutable(index) => {
                Box::new(index.iter_counts_per_value(deferred_internal_id))
            }
        }
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        match self {
            BoolIndex::Mmap(index) => index.get_telemetry_data(),
            BoolIndex::Immutable(index) => index.get_telemetry_data(),
        }
    }

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        match self {
            BoolIndex::Mmap(index) => index.values_count(point_id),
            BoolIndex::Immutable(index) => index.values_count(point_id),
        }
    }

    pub fn check_values_any(
        &self,
        point_id: PointOffsetType,
        is_true: bool,
        _hw_counter: &HardwareCounterCell,
    ) -> bool {
        match self {
            BoolIndex::Mmap(index) => index.check_values_any(point_id, is_true),
            BoolIndex::Immutable(index) => index.check_values_any(point_id, is_true),
        }
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        match self {
            BoolIndex::Mmap(index) => index.values_is_empty(point_id),
            BoolIndex::Immutable(index) => index.values_is_empty(point_id),
        }
    }

    pub fn ram_usage_bytes(&self) -> usize {
        match self {
            BoolIndex::Mmap(index) => index.ram_usage_bytes(),
            BoolIndex::Immutable(index) => index.ram_usage_bytes(),
        }
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            BoolIndex::Mmap(index) => index.is_on_disk(),
            BoolIndex::Immutable(index) => index.is_on_disk(),
        }
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            BoolIndex::Mmap(index) => index.populate()?,
            BoolIndex::Immutable(index) => index.populate()?,
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            BoolIndex::Mmap(index) => index.clear_cache()?,
            BoolIndex::Immutable(index) => index.clear_cache()?,
        }
        Ok(())
    }

    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            // Mmap bool index can be both mutable and immutable, so we pick mutable
            BoolIndex::Mmap(_) => IndexMutability::Mutable,
            BoolIndex::Immutable(_) => IndexMutability::Immutable,
        }
    }

    pub fn get_storage_type(&self) -> StorageType {
        match self {
            BoolIndex::Mmap(index) => StorageType::Mmap {
                is_on_disk: index.is_on_disk(),
            },
            BoolIndex::Immutable(index) => StorageType::Mmap {
                is_on_disk: index.is_on_disk(),
            },
        }
    }
}

impl From<MutableBoolIndex> for BoolIndex {
    #[inline]
    fn from(index: MutableBoolIndex) -> Self {
        BoolIndex::Mmap(index)
    }
}

impl From<ImmutableBoolIndex> for BoolIndex {
    #[inline]
    fn from(index: ImmutableBoolIndex) -> Self {
        BoolIndex::Immutable(index)
    }
}

impl PayloadFieldIndex for BoolIndex {
    fn count_indexed_points(&self) -> usize {
        match self {
            BoolIndex::Mmap(index) => index.count_indexed_points(),
            BoolIndex::Immutable(index) => index.count_indexed_points(),
        }
    }

    fn wipe(self) -> OperationResult<()> {
        match self {
            BoolIndex::Mmap(index) => index.wipe(),
            BoolIndex::Immutable(index) => index.wipe(),
        }
    }

    fn flusher(&self) -> crate::common::Flusher {
        match self {
            BoolIndex::Mmap(index) => index.flusher(),
            BoolIndex::Immutable(index) => index.flusher(),
        }
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        match self {
            BoolIndex::Mmap(index) => index.files(),
            BoolIndex::Immutable(index) => index.files(),
        }
    }

    fn immutable_files(&self) -> Vec<std::path::PathBuf> {
        match self {
            BoolIndex::Mmap(index) => index.immutable_files(),
            BoolIndex::Immutable(index) => index.immutable_files(),
        }
    }

    fn filter<'a>(
        &'a self,
        condition: &'a crate::types::FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        match self {
            BoolIndex::Mmap(index) => index.filter(condition, hw_counter),
            BoolIndex::Immutable(index) => index.filter(condition, hw_counter),
        }
    }

    fn estimate_cardinality(
        &self,
        condition: &crate::types::FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<super::CardinalityEstimation>> {
        match self {
            BoolIndex::Mmap(index) => index.estimate_cardinality(condition, hw_counter),
            BoolIndex::Immutable(index) => index.estimate_cardinality(condition, hw_counter),
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: crate::types::PayloadKeyType,
    ) -> Box<dyn Iterator<Item = OperationResult<super::PayloadBlockCondition>> + '_> {
        match self {
            BoolIndex::Mmap(index) => index.payload_blocks(threshold, key),
            BoolIndex::Immutable(index) => index.payload_blocks(threshold, key),
        }
    }
}

impl FacetIndex for BoolIndex {
    fn get_point_values(
        &self,
        point_id: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> impl Iterator<Item = FacetValueRef<'_>> + '_ {
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

    fn iter_counts_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> impl Iterator<Item = FacetHit<FacetValueRef<'_>>> + '_ {
        self.iter_counts_per_value(deferred_internal_id)
            .map(|(value, count)| FacetHit {
                value: FacetValueRef::Bool(value),
                count,
            })
    }
}

impl ValueIndexer for BoolIndex {
    type ValueType = bool;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<Self::ValueType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            BoolIndex::Mmap(index) => index.add_many(id, values, hw_counter),
            BoolIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable bool index",
            )),
        }
    }

    fn get_value(value: &serde_json::Value) -> Option<Self::ValueType> {
        match value {
            serde_json::Value::Bool(value) => Some(*value),
            _ => None,
        }
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        match self {
            BoolIndex::Mmap(index) => index.remove_point(id),
            BoolIndex::Immutable(index) => index.remove_point(id),
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

    use super::immutable_bool_index::{ImmutableBoolIndex, ImmutableBoolIndexBuilder};
    use super::mutable_bool_index::{MutableBoolIndex, MutableBoolIndexBuilder};
    use crate::index::field_index::{FieldIndexBuilderTrait, PayloadFieldIndex, ValueIndexer};
    use crate::json_path::JsonPath;

    const FIELD_NAME: &str = "bool_field";
    const DB_NAME: &str = "test_db";

    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum IndexType {
        Mutable,
        Immutable,
    }

    trait BuildableIndex: PayloadFieldIndex {
        type BuilderType: FieldIndexBuilderTrait<FieldIndexType = Self>;

        fn builder(path: &Path) -> Self::BuilderType;
        fn open_at(path: &Path) -> Self;
    }

    impl BuildableIndex for MutableBoolIndex {
        type BuilderType = MutableBoolIndexBuilder;

        fn builder(path: &Path) -> Self::BuilderType {
            MutableBoolIndex::builder(path).unwrap()
        }

        fn open_at(path: &Path) -> Self {
            MutableBoolIndex::builder(path)
                .unwrap()
                .make_empty()
                .unwrap()
        }
    }

    impl BuildableIndex for ImmutableBoolIndex {
        type BuilderType = ImmutableBoolIndexBuilder;

        fn builder(path: &Path) -> Self::BuilderType {
            ImmutableBoolIndex::builder(path).unwrap()
        }

        fn open_at(path: &Path) -> Self {
            let mutable_index = MutableBoolIndex::builder(path)
                .unwrap()
                .make_empty()
                .unwrap();
            ImmutableBoolIndex::from_mutable(mutable_index).unwrap()
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

    fn filter<I: BuildableIndex>(given: serde_json::Value, match_on: bool, expected_count: usize) {
        let tmp_dir = Builder::new().prefix(DB_NAME).tempdir().unwrap();
        let mut builder = I::builder(tmp_dir.path());

        let hw_counter = HardwareCounterCell::new();

        builder.add_point(0, &[&given], &hw_counter).unwrap();

        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();
        let index = builder.finalize().unwrap();
        let count = index
            .filter(&match_bool(match_on), &hw_counter)
            .unwrap()
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
    fn test_filter_true(
        #[case] given: serde_json::Value,
        #[case] expected_count: usize,
        #[values(IndexType::Mutable, IndexType::Immutable)] index_type: IndexType,
    ) {
        match index_type {
            IndexType::Mutable => filter::<MutableBoolIndex>(given, true, expected_count),
            IndexType::Immutable => filter::<ImmutableBoolIndex>(given, true, expected_count),
        }
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
    fn test_filter_false(
        #[case] given: serde_json::Value,
        #[case] expected_count: usize,
        #[values(IndexType::Mutable, IndexType::Immutable)] index_type: IndexType,
    ) {
        match index_type {
            IndexType::Mutable => filter::<MutableBoolIndex>(given.clone(), false, expected_count),
            IndexType::Immutable => {
                filter::<ImmutableBoolIndex>(given.clone(), false, expected_count)
            }
        }
    }

    #[rstest]
    fn test_load_from_disk(
        #[values(IndexType::Mutable, IndexType::Immutable)] index_type: IndexType,
    ) {
        match index_type {
            IndexType::Mutable => load_from_disk::<MutableBoolIndex>(),
            IndexType::Immutable => load_from_disk::<ImmutableBoolIndex>(),
        }
    }

    fn load_from_disk<I: BuildableIndex>() {
        let tmp_dir = Builder::new().prefix(DB_NAME).tempdir().unwrap();
        let mut builder = I::builder(tmp_dir.path());

        let hw_counter = HardwareCounterCell::new();

        bools_fixture()
            .into_iter()
            .enumerate()
            .for_each(|(i, value)| {
                builder.add_point(i as u32, &[&value], &hw_counter).unwrap();
            });

        let index = builder.finalize().unwrap();
        index.flusher()().unwrap();
        drop(index);

        let new_index = I::open_at(tmp_dir.path());

        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();
        let point_offsets = new_index
            .filter(&match_bool(false), &hw_counter)
            .unwrap()
            .unwrap()
            .collect_vec();
        assert_eq!(point_offsets, vec![1, 2, 3, 5, 6, 10]);

        let point_offsets = new_index
            .filter(&match_bool(true), &hw_counter)
            .unwrap()
            .unwrap()
            .collect_vec();
        assert_eq!(point_offsets, vec![0, 2, 3, 4, 6, 11]);

        assert_eq!(new_index.count_indexed_points(), 9);
    }

    #[rstest]
    #[case(json!(false), json!(true))]
    #[case(json!([false, true]), json!(true))]
    fn test_modify_value(#[case] before: serde_json::Value, #[case] after: serde_json::Value) {
        modify_value::<MutableBoolIndex>(before, after);
    }

    /// Try to modify from falsy to only true
    fn modify_value<I: BuildableIndex + ValueIndexer>(
        before: serde_json::Value,
        after: serde_json::Value,
    ) {
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
            .unwrap()
            .collect_vec();
        assert_eq!(point_offsets, vec![idx]);

        index.add_point(idx, &[&after], &hw_cell).unwrap();

        let point_offsets = index
            .filter(&match_bool(true), &hw_counter)
            .unwrap()
            .unwrap()
            .collect_vec();
        assert_eq!(point_offsets, vec![idx]);
        let point_offsets = index
            .filter(&match_bool(false), &hw_counter)
            .unwrap()
            .unwrap()
            .collect_vec();
        assert!(point_offsets.is_empty());
    }

    #[rstest]
    fn test_indexed_count(
        #[values(IndexType::Mutable, IndexType::Immutable)] index_type: IndexType,
    ) {
        match index_type {
            IndexType::Mutable => indexed_count::<MutableBoolIndex>(),
            IndexType::Immutable => indexed_count::<ImmutableBoolIndex>(),
        }
    }

    fn indexed_count<I: BuildableIndex + PayloadFieldIndex>() {
        let tmp_dir = Builder::new().prefix(DB_NAME).tempdir().unwrap();
        let mut builder = I::builder(tmp_dir.path());

        let hw_counter = HardwareCounterCell::new();

        bools_fixture()
            .into_iter()
            .enumerate()
            .for_each(|(i, value)| {
                builder.add_point(i as u32, &[&value], &hw_counter).unwrap();
            });

        let index = builder.finalize().unwrap();

        assert_eq!(index.count_indexed_points(), 9);
    }

    #[test]
    fn test_payload_blocks() {
        payload_blocks::<MutableBoolIndex>();
    }

    fn payload_blocks<I: BuildableIndex + ValueIndexer>() {
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
            .map(Result::unwrap)
            .collect_vec();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].cardinality, 6);
        assert_eq!(blocks[1].cardinality, 6);
    }

    #[rstest]
    fn test_estimate_cardinality(
        #[values(IndexType::Mutable, IndexType::Immutable)] index_type: IndexType,
    ) {
        match index_type {
            IndexType::Mutable => estimate_cardinality::<MutableBoolIndex>(),
            IndexType::Immutable => estimate_cardinality::<ImmutableBoolIndex>(),
        }
    }

    fn estimate_cardinality<I: BuildableIndex>() {
        let tmp_dir = Builder::new().prefix(DB_NAME).tempdir().unwrap();
        let mut builder = I::builder(tmp_dir.path());

        let hw_counter = HardwareCounterCell::new();

        bools_fixture()
            .into_iter()
            .enumerate()
            .for_each(|(i, value)| {
                builder.add_point(i as u32, &[&value], &hw_counter).unwrap();
            });

        let hw_counter = HardwareCounterCell::new();

        let index = builder.finalize().unwrap();
        let cardinality = index
            .estimate_cardinality(&match_bool(true), &hw_counter)
            .unwrap()
            .unwrap();
        assert_eq!(cardinality.exp, 6);

        let cardinality = index
            .estimate_cardinality(&match_bool(false), &hw_counter)
            .unwrap()
            .unwrap();
        assert_eq!(cardinality.exp, 6);
    }
}
