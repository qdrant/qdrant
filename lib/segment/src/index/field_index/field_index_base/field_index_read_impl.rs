use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use super::field_index::FieldIndex;
use super::field_index_read::FieldIndexRead;
use super::payload_field_index::PayloadFieldIndexRead;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::facet_index::{FacetIndex, FacetIndexEnum};
use crate::index::field_index::numeric_index::{NumericFieldIndex, NumericFieldIndexRead};
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, PayloadKeyType};

impl PayloadFieldIndexRead for FieldIndex {
    fn count_indexed_points(&self) -> usize {
        match self {
            FieldIndex::IntIndex(idx) => idx.count_indexed_points(),
            FieldIndex::DatetimeIndex(idx) => idx.count_indexed_points(),
            FieldIndex::IntMapIndex(idx) => idx.count_indexed_points(),
            FieldIndex::KeywordIndex(idx) => idx.count_indexed_points(),
            FieldIndex::FloatIndex(idx) => idx.count_indexed_points(),
            FieldIndex::GeoIndex(idx) => idx.count_indexed_points(),
            FieldIndex::BoolIndex(idx) => idx.count_indexed_points(),
            FieldIndex::FullTextIndex(idx) => idx.count_indexed_points(),
            FieldIndex::UuidIndex(idx) => idx.count_indexed_points(),
            FieldIndex::UuidMapIndex(idx) => idx.count_indexed_points(),
            FieldIndex::NullIndex(idx) => idx.count_indexed_points(),
        }
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        match self {
            FieldIndex::IntIndex(idx) => idx.filter(condition, hw_counter),
            FieldIndex::DatetimeIndex(idx) => idx.filter(condition, hw_counter),
            FieldIndex::IntMapIndex(idx) => idx.filter(condition, hw_counter),
            FieldIndex::KeywordIndex(idx) => idx.filter(condition, hw_counter),
            FieldIndex::FloatIndex(idx) => idx.filter(condition, hw_counter),
            FieldIndex::GeoIndex(idx) => idx.filter(condition, hw_counter),
            FieldIndex::BoolIndex(idx) => idx.filter(condition, hw_counter),
            FieldIndex::FullTextIndex(idx) => idx.filter(condition, hw_counter),
            FieldIndex::UuidIndex(idx) => idx.filter(condition, hw_counter),
            FieldIndex::UuidMapIndex(idx) => idx.filter(condition, hw_counter),
            FieldIndex::NullIndex(idx) => idx.filter(condition, hw_counter),
        }
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        match self {
            FieldIndex::IntIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            FieldIndex::DatetimeIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            FieldIndex::IntMapIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            FieldIndex::KeywordIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            FieldIndex::FloatIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            FieldIndex::GeoIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            FieldIndex::BoolIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            FieldIndex::FullTextIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            FieldIndex::UuidIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            FieldIndex::UuidMapIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
            FieldIndex::NullIndex(idx) => idx.estimate_cardinality(condition, hw_counter),
        }
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            FieldIndex::IntIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            FieldIndex::DatetimeIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            FieldIndex::IntMapIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            FieldIndex::KeywordIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            FieldIndex::FloatIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            FieldIndex::GeoIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            FieldIndex::BoolIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            FieldIndex::FullTextIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            FieldIndex::UuidIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            FieldIndex::UuidMapIndex(idx) => idx.for_each_payload_block(threshold, key, f),
            FieldIndex::NullIndex(idx) => idx.for_each_payload_block(threshold, key, f),
        }
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        match self {
            FieldIndex::IntIndex(idx) => idx.condition_checker(condition, hw_acc),
            FieldIndex::DatetimeIndex(idx) => idx.condition_checker(condition, hw_acc),
            FieldIndex::IntMapIndex(idx) => idx.condition_checker(condition, hw_acc),
            FieldIndex::KeywordIndex(idx) => idx.condition_checker(condition, hw_acc),
            FieldIndex::FloatIndex(idx) => idx.condition_checker(condition, hw_acc),
            FieldIndex::GeoIndex(idx) => idx.condition_checker(condition, hw_acc),
            FieldIndex::BoolIndex(idx) => idx.condition_checker(condition, hw_acc),
            FieldIndex::FullTextIndex(idx) => idx.condition_checker(condition, hw_acc),
            FieldIndex::UuidIndex(idx) => idx.condition_checker(condition, hw_acc),
            FieldIndex::UuidMapIndex(idx) => idx.condition_checker(condition, hw_acc),
            FieldIndex::NullIndex(idx) => idx.condition_checker(condition, hw_acc),
        }
    }

    fn special_check_condition(
        &self,
        condition: &FieldCondition,
        payload_value: &Value,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<bool>> {
        match self {
            FieldIndex::IntIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            FieldIndex::DatetimeIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            FieldIndex::IntMapIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            FieldIndex::KeywordIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            FieldIndex::FloatIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            FieldIndex::GeoIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            FieldIndex::BoolIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            FieldIndex::FullTextIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            FieldIndex::UuidIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            FieldIndex::UuidMapIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
            FieldIndex::NullIndex(idx) => {
                idx.special_check_condition(condition, payload_value, hw_counter)
            }
        }
    }
}

impl FieldIndexRead for FieldIndex {
    fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        match self {
            FieldIndex::IntIndex(index) => index.get_telemetry_data(),
            FieldIndex::DatetimeIndex(index) => index.get_telemetry_data(),
            FieldIndex::IntMapIndex(index) => index.get_telemetry_data(),
            FieldIndex::KeywordIndex(index) => index.get_telemetry_data(),
            FieldIndex::FloatIndex(index) => index.get_telemetry_data(),
            FieldIndex::GeoIndex(index) => index.get_telemetry_data(),
            FieldIndex::BoolIndex(index) => index.get_telemetry_data(),
            FieldIndex::FullTextIndex(index) => index.get_telemetry_data(),
            FieldIndex::UuidIndex(index) => index.get_telemetry_data(),
            FieldIndex::UuidMapIndex(index) => index.get_telemetry_data(),
            FieldIndex::NullIndex(index) => index.get_telemetry_data(),
        }
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        match self {
            FieldIndex::IntIndex(index) => index.values_count(point_id),
            FieldIndex::DatetimeIndex(index) => index.values_count(point_id),
            FieldIndex::IntMapIndex(index) => index.values_count(point_id),
            FieldIndex::KeywordIndex(index) => index.values_count(point_id),
            FieldIndex::FloatIndex(index) => index.values_count(point_id),
            FieldIndex::GeoIndex(index) => index.values_count(point_id),
            FieldIndex::BoolIndex(index) => index.values_count(point_id),
            FieldIndex::FullTextIndex(index) => index.values_count(point_id),
            FieldIndex::UuidIndex(index) => index.values_count(point_id),
            FieldIndex::UuidMapIndex(index) => index.values_count(point_id),
            FieldIndex::NullIndex(index) => index.values_count(point_id),
        }
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        match self {
            FieldIndex::IntIndex(index) => index.values_is_empty(point_id),
            FieldIndex::DatetimeIndex(index) => index.values_is_empty(point_id),
            FieldIndex::IntMapIndex(index) => index.values_is_empty(point_id),
            FieldIndex::KeywordIndex(index) => index.values_is_empty(point_id),
            FieldIndex::FloatIndex(index) => index.values_is_empty(point_id),
            FieldIndex::GeoIndex(index) => index.values_is_empty(point_id),
            FieldIndex::BoolIndex(index) => index.values_is_empty(point_id),
            FieldIndex::FullTextIndex(index) => index.values_is_empty(point_id),
            FieldIndex::UuidIndex(index) => index.values_is_empty(point_id),
            FieldIndex::UuidMapIndex(index) => index.values_is_empty(point_id),
            FieldIndex::NullIndex(index) => index.values_is_empty(point_id),
        }
    }

    fn value_retriever<'a, 'q>(
        &'a self,
        hw_counter: &'q HardwareCounterCell,
    ) -> Option<VariableRetrieverFn<'q>>
    where
        'a: 'q,
    {
        // Per-variant value-to-`Value` conversion lives on each typed
        // index as an inherent `value_retriever` method (see e.g.
        // `NumericIndex<IntPayloadType, DateTimePayloadType>::value_retriever`).
        // This dispatch is mechanical — adding a `FieldIndex` variant
        // forces a compile error here.
        match self {
            FieldIndex::IntIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::DatetimeIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::IntMapIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::KeywordIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::FloatIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::GeoIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::BoolIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::UuidIndex(index) => Some(index.value_retriever(hw_counter)),
            FieldIndex::UuidMapIndex(index) => Some(index.value_retriever(hw_counter)),
            // FullTextIndex: caller falls back to payload — text values
            // are easier to read directly than reconstruct from the
            // inverted index.
            FieldIndex::FullTextIndex(_) => None,
            // NullIndex: no underlying values to return; another index
            // on the same field is expected to provide them.
            FieldIndex::NullIndex(_) => None,
        }
    }

    fn as_numeric(&self) -> Option<impl NumericFieldIndexRead + '_> {
        match self {
            FieldIndex::IntIndex(index) => Some(NumericFieldIndex::IntIndex(index.inner())),
            FieldIndex::DatetimeIndex(index) => Some(NumericFieldIndex::IntIndex(index.inner())),
            FieldIndex::FloatIndex(index) => Some(NumericFieldIndex::FloatIndex(index.inner())),
            FieldIndex::IntMapIndex(_)
            | FieldIndex::KeywordIndex(_)
            | FieldIndex::GeoIndex(_)
            | FieldIndex::BoolIndex(_)
            | FieldIndex::UuidMapIndex(_)
            | FieldIndex::UuidIndex(_)
            | FieldIndex::FullTextIndex(_)
            | FieldIndex::NullIndex(_) => None,
        }
    }

    fn as_facet_index(&self) -> Option<impl FacetIndex + '_> {
        match self {
            FieldIndex::KeywordIndex(index) => Some(FacetIndexEnum::Keyword(index)),
            FieldIndex::IntMapIndex(index) => Some(FacetIndexEnum::Int(index)),
            FieldIndex::UuidMapIndex(index) => Some(FacetIndexEnum::Uuid(index)),
            FieldIndex::BoolIndex(index) => Some(FacetIndexEnum::Bool(index)),
            FieldIndex::UuidIndex(_)
            | FieldIndex::IntIndex(_)
            | FieldIndex::DatetimeIndex(_)
            | FieldIndex::FloatIndex(_)
            | FieldIndex::GeoIndex(_)
            | FieldIndex::FullTextIndex(_)
            | FieldIndex::NullIndex(_) => None,
        }
    }
}
