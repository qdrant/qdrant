use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UserData};

use super::super::full_text_index_read::FullTextIndexRead;
use super::super::inverted_index::{ParsedQuery, TokenId};
use super::super::read_ops;
use super::super::tokenizers::Tokenizer;
use super::ReadOnlyFullTextIndex;
use crate::common::operation_error::OperationResult;
use crate::index::UniversalReadExt;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::payload_config::StorageType;
use crate::types::{FieldCondition, PayloadKeyType};

/// Dispatcher impl: forwards every [`FullTextIndexRead`] method to the active
/// variant. The default trait methods (`parse_*`, `check_payload_match`,
/// `get_telemetry_data`) are picked up automatically — they only depend on the
/// required methods, so no per-variant dispatch is needed for them.
impl<S: UniversalRead> FullTextIndexRead for ReadOnlyFullTextIndex<S> {
    fn tokenizer(&self) -> &Tokenizer {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => index.tokenizer(),
            ReadOnlyFullTextIndex::OnDisk(index) => index.tokenizer(),
            ReadOnlyFullTextIndex::Immutable(index) => index.tokenizer(),
        }
    }

    fn telemetry_index_type(&self) -> &'static str {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => index.telemetry_index_type(),
            ReadOnlyFullTextIndex::OnDisk(index) => index.telemetry_index_type(),
            ReadOnlyFullTextIndex::Immutable(index) => index.telemetry_index_type(),
        }
    }

    fn points_count(&self) -> usize {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => index.points_count(),
            ReadOnlyFullTextIndex::OnDisk(index) => index.points_count(),
            ReadOnlyFullTextIndex::Immutable(index) => index.points_count(),
        }
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => index.values_count(point_id),
            ReadOnlyFullTextIndex::OnDisk(index) => index.values_count(point_id),
            ReadOnlyFullTextIndex::Immutable(index) => index.values_count(point_id),
        }
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => index.values_is_empty(point_id),
            ReadOnlyFullTextIndex::OnDisk(index) => index.values_is_empty(point_id),
            ReadOnlyFullTextIndex::Immutable(index) => index.values_is_empty(point_id),
        }
    }

    fn for_each_token_id<'a, U: UserData>(
        &self,
        iter: impl Iterator<Item = (U, &'a str)>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(U, Option<TokenId>),
    ) -> OperationResult<()> {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => {
                index.for_each_token_id(iter, hw_counter, f)
            }
            ReadOnlyFullTextIndex::OnDisk(index) => index.for_each_token_id(iter, hw_counter, f),
            ReadOnlyFullTextIndex::Immutable(index) => index.for_each_token_id(iter, hw_counter, f),
        }
    }

    fn filter_query<'a>(
        &'a self,
        query: ParsedQuery,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => index.filter_query(query, hw_counter),
            ReadOnlyFullTextIndex::OnDisk(index) => index.filter_query(query, hw_counter),
            ReadOnlyFullTextIndex::Immutable(index) => index.filter_query(query, hw_counter),
        }
    }

    fn estimate_query_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => {
                index.estimate_query_cardinality(query, condition, hw_counter)
            }
            ReadOnlyFullTextIndex::OnDisk(index) => {
                index.estimate_query_cardinality(query, condition, hw_counter)
            }
            ReadOnlyFullTextIndex::Immutable(index) => {
                index.estimate_query_cardinality(query, condition, hw_counter)
            }
        }
    }

    fn check_match(&self, query: &ParsedQuery, point_id: PointOffsetType) -> OperationResult<bool> {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => index.check_match(query, point_id),
            ReadOnlyFullTextIndex::OnDisk(index) => index.check_match(query, point_id),
            ReadOnlyFullTextIndex::Immutable(index) => index.check_match(query, point_id),
        }
    }

    fn check_match_batch<U: UserData>(
        &self,
        query: &ParsedQuery,
        items: impl Iterator<Item = (U, PointOffsetType)>,
        on_match: impl FnMut(U, bool),
    ) -> OperationResult<()> {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => {
                index.check_match_batch(query, items, on_match)
            }
            ReadOnlyFullTextIndex::OnDisk(index) => index.check_match_batch(query, items, on_match),
            ReadOnlyFullTextIndex::Immutable(index) => {
                index.check_match_batch(query, items, on_match)
            }
        }
    }

    fn for_each_payload_block_inner(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => {
                index.for_each_payload_block_inner(threshold, key, f)
            }
            ReadOnlyFullTextIndex::OnDisk(index) => {
                index.for_each_payload_block_inner(threshold, key, f)
            }
            ReadOnlyFullTextIndex::Immutable(index) => {
                index.for_each_payload_block_inner(threshold, key, f)
            }
        }
    }

    fn get_storage_type(&self) -> StorageType {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => index.get_storage_type(),
            ReadOnlyFullTextIndex::OnDisk(index) => index.get_storage_type(),
            ReadOnlyFullTextIndex::Immutable(index) => index.get_storage_type(),
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => index.ram_usage_bytes(),
            ReadOnlyFullTextIndex::OnDisk(index) => index.ram_usage_bytes(),
            ReadOnlyFullTextIndex::Immutable(index) => index.ram_usage_bytes(),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            ReadOnlyFullTextIndex::Appendable(index) => index.is_on_disk(),
            ReadOnlyFullTextIndex::OnDisk(index) => index.is_on_disk(),
            ReadOnlyFullTextIndex::Immutable(index) => index.is_on_disk(),
        }
    }
}

impl<S: UniversalReadExt> PayloadFieldIndexRead for ReadOnlyFullTextIndex<S> {
    fn count_indexed_points(&self) -> OperationResult<usize> {
        Ok(FullTextIndexRead::points_count(self))
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        read_ops::filter(self, condition, hw_counter)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        read_ops::estimate_cardinality(self, condition, hw_counter)
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        read_ops::for_each_payload_block(self, threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> OperationResult<Option<ConditionCheckerEnum<'a>>> {
        read_ops::condition_checker(self, condition, hw_acc, S::condition_checker_full_text)
    }

    fn special_check_condition(
        &self,
        condition: &FieldCondition,
        payload_value: &serde_json::Value,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<bool>> {
        read_ops::special_check_condition(self, condition, payload_value, hw_counter)
    }
}
