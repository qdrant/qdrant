use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use serde_json::Value;

use crate::common::operation_error::OperationResult;
use crate::index::field_index::bool_index::BoolIndexRead;
use crate::index::field_index::field_index_base::read_only::ReadOnlyFieldIndex;
use crate::index::field_index::full_text_index::read_ops::FullTextIndexRead;
use crate::index::field_index::geo_index::GeoMapIndexRead;
use crate::index::field_index::map_index::read_ops::MapIndexRead;
use crate::index::field_index::null_index::NullIndexRead;
use crate::index::field_index::numeric_index::{
    NumericFieldIndexRead, NumericIndexRead, ReadOnlyNumericFieldIndex,
};
use crate::index::field_index::{
    CardinalityEstimation, FacetIndex, FieldIndexRead, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, PayloadKeyType};

impl<S: UniversalRead> PayloadFieldIndexRead for ReadOnlyFieldIndex<S> {
    fn count_indexed_points(&self) -> usize {
        match self {
            ReadOnlyFieldIndex::IntIndex(idx) => idx.count_indexed_points(),
            ReadOnlyFieldIndex::DatetimeIndex(idx) => idx.count_indexed_points(),
            ReadOnlyFieldIndex::IntMapIndex(idx) => idx.count_indexed_points(),
            ReadOnlyFieldIndex::KeywordIndex(idx) => idx.count_indexed_points(),
            ReadOnlyFieldIndex::FloatIndex(idx) => idx.count_indexed_points(),
            ReadOnlyFieldIndex::BoolIndex(idx) => idx.count_indexed_points(),
            ReadOnlyFieldIndex::GeoIndex(idx) => idx.count_indexed_points(),
            ReadOnlyFieldIndex::UuidIndex(idx) => idx.count_indexed_points(),
            ReadOnlyFieldIndex::FullTextIndex(idx) => idx.count_indexed_points(),
            ReadOnlyFieldIndex::UuidMapIndex(idx) => idx.count_indexed_points(),
            ReadOnlyFieldIndex::NullIndex(idx) => idx.count_indexed_points(),
        }
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        match self {
            ReadOnlyFieldIndex::IntIndex(idx) => idx.filter(condition, hw_counter),
            ReadOnlyFieldIndex::DatetimeIndex(idx) => idx.filter(condition, hw_counter),
            ReadOnlyFieldIndex::IntMapIndex(idx) => idx.filter(condition, hw_counter),
            ReadOnlyFieldIndex::KeywordIndex(idx) => idx.filter(condition, hw_counter),
            ReadOnlyFieldIndex::FloatIndex(idx) => idx.filter(condition, hw_counter),
            ReadOnlyFieldIndex::BoolIndex(idx) => idx.filter(condition, hw_counter),
            ReadOnlyFieldIndex::GeoIndex(idx) => idx.filter(condition, hw_counter),
            ReadOnlyFieldIndex::UuidIndex(idx) => idx.filter(condition, hw_counter),
            ReadOnlyFieldIndex::FullTextIndex(idx) => idx.filter(condition, hw_counter),
            ReadOnlyFieldIndex::UuidMapIndex(idx) => idx.filter(condition, hw_counter),
            ReadOnlyFieldIndex::NullIndex(idx) => idx.filter(condition, hw_counter),
        }
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        match self {
            ReadOnlyFieldIndex::IntIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            ReadOnlyFieldIndex::DatetimeIndex(idx) => {
                idx.estimate_cardinality(condition, hw_counter)
            }
            ReadOnlyFieldIndex::IntMapIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            ReadOnlyFieldIndex::KeywordIndex(idx) => {
                idx.estimate_cardinality(condition, hw_counter)
            }
            ReadOnlyFieldIndex::FloatIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            ReadOnlyFieldIndex::BoolIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            ReadOnlyFieldIndex::GeoIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            ReadOnlyFieldIndex::UuidIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            ReadOnlyFieldIndex::FullTextIndex(idx) => {
                idx.estimate_cardinality(condition, hw_counter)
            }
            ReadOnlyFieldIndex::UuidMapIndex(idx) => {
                idx.estimate_cardinality(condition, hw_counter)
            }
            ReadOnlyFieldIndex::NullIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
        }
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyFieldIndex::IntIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            ReadOnlyFieldIndex::DatetimeIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            ReadOnlyFieldIndex::IntMapIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            ReadOnlyFieldIndex::KeywordIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            ReadOnlyFieldIndex::FloatIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            ReadOnlyFieldIndex::BoolIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            ReadOnlyFieldIndex::GeoIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            ReadOnlyFieldIndex::UuidIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            ReadOnlyFieldIndex::FullTextIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            ReadOnlyFieldIndex::UuidMapIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            ReadOnlyFieldIndex::NullIndex(idx) => idx.for_each_payload_block(threshold, key, f),
        }
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        match self {
            ReadOnlyFieldIndex::IntIndex(idx) => idx.condition_checker(condition, hw_acc),
            ReadOnlyFieldIndex::DatetimeIndex(idx) => idx.condition_checker(condition, hw_acc),
            ReadOnlyFieldIndex::IntMapIndex(idx) => idx.condition_checker(condition, hw_acc),
            ReadOnlyFieldIndex::KeywordIndex(idx) => idx.condition_checker(condition, hw_acc),
            ReadOnlyFieldIndex::FloatIndex(idx) => idx.condition_checker(condition, hw_acc),
            ReadOnlyFieldIndex::BoolIndex(idx) => idx.condition_checker(condition, hw_acc),
            ReadOnlyFieldIndex::GeoIndex(idx) => idx.condition_checker(condition, hw_acc),
            ReadOnlyFieldIndex::UuidIndex(idx) => idx.condition_checker(condition, hw_acc),
            ReadOnlyFieldIndex::FullTextIndex(idx) => idx.condition_checker(condition, hw_acc),
            ReadOnlyFieldIndex::UuidMapIndex(idx) => idx.condition_checker(condition, hw_acc),
            ReadOnlyFieldIndex::NullIndex(idx) => idx.condition_checker(condition, hw_acc),
        }
    }

    fn special_check_condition(
        &self,
        condition: &FieldCondition,
        payload_value: &Value,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<bool>> {
        match self {
            ReadOnlyFieldIndex::IntIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            ReadOnlyFieldIndex::DatetimeIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            ReadOnlyFieldIndex::IntMapIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            ReadOnlyFieldIndex::KeywordIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            ReadOnlyFieldIndex::FloatIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            ReadOnlyFieldIndex::BoolIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            ReadOnlyFieldIndex::GeoIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            ReadOnlyFieldIndex::UuidIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            ReadOnlyFieldIndex::FullTextIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            ReadOnlyFieldIndex::UuidMapIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            ReadOnlyFieldIndex::NullIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
        }
    }
}

impl<S: UniversalRead> FieldIndexRead for ReadOnlyFieldIndex<S> {
    fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        match self {
            ReadOnlyFieldIndex::IntIndex(idx) => idx.get_telemetry_data(),
            ReadOnlyFieldIndex::DatetimeIndex(idx) => idx.get_telemetry_data(),
            ReadOnlyFieldIndex::IntMapIndex(idx) => idx.get_telemetry_data(),
            ReadOnlyFieldIndex::KeywordIndex(idx) => idx.get_telemetry_data(),
            ReadOnlyFieldIndex::FloatIndex(idx) => idx.get_telemetry_data(),
            ReadOnlyFieldIndex::BoolIndex(idx) => idx.get_telemetry_data(),
            ReadOnlyFieldIndex::GeoIndex(idx) => idx.get_telemetry_data(),
            ReadOnlyFieldIndex::UuidIndex(idx) => idx.get_telemetry_data(),
            ReadOnlyFieldIndex::FullTextIndex(idx) => idx.get_telemetry_data(),
            ReadOnlyFieldIndex::UuidMapIndex(idx) => idx.get_telemetry_data(),
            ReadOnlyFieldIndex::NullIndex(idx) => idx.get_telemetry_data(),
        }
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        match self {
            ReadOnlyFieldIndex::IntIndex(idx) => {
                NumericIndexRead::values_count(idx, point_id).unwrap_or(0)
            }
            ReadOnlyFieldIndex::DatetimeIndex(idx) => {
                NumericIndexRead::values_count(idx, point_id).unwrap_or(0)
            }
            ReadOnlyFieldIndex::IntMapIndex(idx) => {
                MapIndexRead::values_count(idx, point_id).unwrap_or(0)
            }
            ReadOnlyFieldIndex::KeywordIndex(idx) => {
                MapIndexRead::values_count(idx, point_id).unwrap_or(0)
            }
            ReadOnlyFieldIndex::FloatIndex(idx) => {
                NumericIndexRead::values_count(idx, point_id).unwrap_or(0)
            }
            ReadOnlyFieldIndex::BoolIndex(idx) => BoolIndexRead::values_count(idx, point_id),
            ReadOnlyFieldIndex::GeoIndex(idx) => GeoMapIndexRead::values_count(idx, point_id),
            ReadOnlyFieldIndex::UuidIndex(idx) => {
                NumericIndexRead::values_count(idx, point_id).unwrap_or(0)
            }
            ReadOnlyFieldIndex::FullTextIndex(idx) => {
                FullTextIndexRead::values_count(idx, point_id)
            }
            ReadOnlyFieldIndex::UuidMapIndex(idx) => {
                MapIndexRead::values_count(idx, point_id).unwrap_or(0)
            }
            ReadOnlyFieldIndex::NullIndex(idx) => NullIndexRead::values_count(idx, point_id),
        }
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        match self {
            ReadOnlyFieldIndex::IntIndex(idx) => NumericIndexRead::values_is_empty(idx, point_id),
            ReadOnlyFieldIndex::DatetimeIndex(idx) => {
                NumericIndexRead::values_is_empty(idx, point_id)
            }
            ReadOnlyFieldIndex::IntMapIndex(idx) => MapIndexRead::values_is_empty(idx, point_id),
            ReadOnlyFieldIndex::KeywordIndex(idx) => MapIndexRead::values_is_empty(idx, point_id),
            ReadOnlyFieldIndex::FloatIndex(idx) => NumericIndexRead::values_is_empty(idx, point_id),
            ReadOnlyFieldIndex::BoolIndex(idx) => BoolIndexRead::values_is_empty(idx, point_id),
            ReadOnlyFieldIndex::GeoIndex(idx) => GeoMapIndexRead::values_is_empty(idx, point_id),
            ReadOnlyFieldIndex::UuidIndex(idx) => NumericIndexRead::values_is_empty(idx, point_id),
            ReadOnlyFieldIndex::FullTextIndex(idx) => {
                FullTextIndexRead::values_is_empty(idx, point_id)
            }
            ReadOnlyFieldIndex::UuidMapIndex(idx) => MapIndexRead::values_is_empty(idx, point_id),
            ReadOnlyFieldIndex::NullIndex(idx) => NullIndexRead::values_is_empty(idx, point_id),
        }
    }

    fn value_retriever<'a, 'q>(
        &'a self,
        hw_counter: &'q HardwareCounterCell,
    ) -> Option<VariableRetrieverFn<'q>>
    where
        'a: 'q,
    {
        // Mirrors `FieldIndex::value_retriever`: NullIndex has no underlying
        // values to return, and FullTextIndex stores tokenized documents
        // rather than raw payload values — neither has a value retriever yet.
        // The numeric, map, bool and geo variants build their per-variant
        // closure via an inherent `value_retriever` method.
        match self {
            ReadOnlyFieldIndex::IntIndex(idx) => Some(idx.value_retriever(hw_counter)),
            ReadOnlyFieldIndex::DatetimeIndex(idx) => Some(idx.value_retriever(hw_counter)),
            ReadOnlyFieldIndex::IntMapIndex(idx) => Some(idx.value_retriever(hw_counter)),
            ReadOnlyFieldIndex::KeywordIndex(idx) => Some(idx.value_retriever(hw_counter)),
            ReadOnlyFieldIndex::FloatIndex(idx) => Some(idx.value_retriever(hw_counter)),
            ReadOnlyFieldIndex::BoolIndex(idx) => Some(idx.value_retriever(hw_counter)),
            ReadOnlyFieldIndex::GeoIndex(idx) => Some(idx.value_retriever(hw_counter)),
            ReadOnlyFieldIndex::UuidIndex(idx) => Some(idx.value_retriever(hw_counter)),
            ReadOnlyFieldIndex::UuidMapIndex(idx) => Some(idx.value_retriever(hw_counter)),
            ReadOnlyFieldIndex::FullTextIndex(_) | ReadOnlyFieldIndex::NullIndex(_) => None,
        }
    }

    fn as_numeric(&self) -> Option<impl NumericFieldIndexRead + '_> {
        match self {
            ReadOnlyFieldIndex::IntIndex(index) => {
                Some(ReadOnlyNumericFieldIndex::IntIndex(index.inner()))
            }
            ReadOnlyFieldIndex::DatetimeIndex(index) => {
                Some(ReadOnlyNumericFieldIndex::IntIndex(index.inner()))
            }
            ReadOnlyFieldIndex::FloatIndex(index) => {
                Some(ReadOnlyNumericFieldIndex::FloatIndex(index.inner()))
            }
            // UUIDs aren't meaningfully order-by-able as numbers, matching
            // `FieldIndex::as_numeric`.
            ReadOnlyFieldIndex::UuidIndex(_)
            | ReadOnlyFieldIndex::IntMapIndex(_)
            | ReadOnlyFieldIndex::KeywordIndex(_)
            | ReadOnlyFieldIndex::BoolIndex(_)
            | ReadOnlyFieldIndex::GeoIndex(_)
            | ReadOnlyFieldIndex::FullTextIndex(_)
            | ReadOnlyFieldIndex::UuidMapIndex(_)
            | ReadOnlyFieldIndex::NullIndex(_) => None::<ReadOnlyNumericFieldIndex<'_, S>>,
        }
    }

    fn as_facet_index(&self) -> Option<impl FacetIndex + '_> {
        use crate::index::field_index::facet_index::FacetIndexEnum;
        match self {
            ReadOnlyFieldIndex::IntMapIndex(index) => Some(FacetIndexEnum::IntReadOnly(index)),
            ReadOnlyFieldIndex::KeywordIndex(index) => Some(FacetIndexEnum::KeywordReadOnly(index)),
            ReadOnlyFieldIndex::BoolIndex(index) => Some(FacetIndexEnum::BoolReadOnly(index)),
            ReadOnlyFieldIndex::UuidMapIndex(index) => Some(FacetIndexEnum::UuidReadOnly(index)),
            // Numeric, geo and full-text variants don't carry facet-able
            // values; NullIndex carries none either.
            ReadOnlyFieldIndex::IntIndex(_)
            | ReadOnlyFieldIndex::DatetimeIndex(_)
            | ReadOnlyFieldIndex::FloatIndex(_)
            | ReadOnlyFieldIndex::UuidIndex(_)
            | ReadOnlyFieldIndex::GeoIndex(_)
            | ReadOnlyFieldIndex::FullTextIndex(_)
            | ReadOnlyFieldIndex::NullIndex(_) => None,
        }
    }
}
