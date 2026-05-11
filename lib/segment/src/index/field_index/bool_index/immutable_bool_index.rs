use std::path::{Path, PathBuf};

use common::bitvec::BitSlice;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::mutable_bool_index::MutableBoolIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    PayloadFieldIndexRead, ValueIndexer,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    AnyVariants, FieldCondition, Match, MatchAny, MatchExcept, MatchValue, PayloadKeyType,
    ValueVariants,
};

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

    #[inline]
    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.0.set_or_insert_immutable(id, false, false);
        Ok(())
    }
}

impl ImmutableBoolIndex {
    // N.B.: these operations are immutable.

    #[inline]
    pub fn get_point_values(&self, point_id: PointOffsetType) -> Vec<bool> {
        self.0.get_point_values(point_id)
    }

    #[inline]
    pub fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(bool, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.0.for_each_value_map(hw_counter, f)
    }

    #[inline]
    pub fn iter_values(&self) -> impl Iterator<Item = bool> + '_ {
        self.0.iter_values()
    }

    #[inline]
    pub fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(bool, usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.0.for_each_count_per_value(deferred_internal_id, f)
    }

    #[inline]
    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        self.0.get_telemetry_data()
    }

    #[inline]
    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        self.0.values_count(point_id)
    }

    #[inline]
    pub fn check_values_any(&self, point_id: PointOffsetType, is_true: bool) -> bool {
        self.0.check_values_any(point_id, is_true)
    }

    #[inline]
    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.0.values_is_empty(point_id)
    }

    #[inline]
    pub fn ram_usage_bytes(&self) -> usize {
        self.0.ram_usage_bytes()
    }

    #[inline]
    pub fn is_on_disk(&self) -> bool {
        self.0.is_on_disk()
    }

    #[inline]
    pub fn populate(&self) -> OperationResult<()> {
        self.0.populate()
    }

    #[inline]
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.0.clear_cache()
    }
}

impl PayloadFieldIndexRead for ImmutableBoolIndex {
    #[inline]
    fn count_indexed_points(&self) -> usize {
        self.0.count_indexed_points()
    }

    #[inline]
    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        self.0.filter(condition, hw_counter)
    }

    #[inline]
    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        self.0.estimate_cardinality(condition, hw_counter)
    }

    #[inline]
    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.0.for_each_payload_block(threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        _hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        // Destructure explicitly (no `..`) so a new field added to
        // `FieldCondition` forces this method to be revisited.
        let FieldCondition {
            key: _,
            r#match,
            range: _,
            geo_radius: _,
            geo_bounding_box: _,
            geo_polygon: _,
            values_count: _,
            is_empty: _,
            is_null: _,
        } = condition;

        let cond_match = r#match.as_ref()?;
        match cond_match {
            Match::Value(MatchValue {
                value: ValueVariants::Bool(is_true),
            }) => {
                let is_true = *is_true;
                Some(Box::new(move |point_id: PointOffsetType| {
                    self.check_values_any(point_id, is_true)
                }))
            }
            Match::Value(MatchValue {
                value: ValueVariants::String(_) | ValueVariants::Integer(_),
            })
            | Match::Any(MatchAny {
                any: AnyVariants::Strings(_) | AnyVariants::Integers(_),
            })
            | Match::Except(MatchExcept {
                except: AnyVariants::Strings(_) | AnyVariants::Integers(_),
            })
            | Match::Text(_)
            | Match::TextAny(_)
            | Match::Phrase(_) => None,
        }
    }
}

impl PayloadFieldIndex for ImmutableBoolIndex {
    #[inline]
    fn wipe(self) -> OperationResult<()> {
        self.0.wipe()
    }

    #[inline]
    fn files(&self) -> Vec<PathBuf> {
        self.0.files()
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
