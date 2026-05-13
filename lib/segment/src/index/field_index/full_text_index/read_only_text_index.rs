use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UserData};

use super::inverted_index::{InvertedIndex, ParsedQuery, TokenId};
use super::mmap_text_index::MmapFullTextIndex;
use super::read_ops::{self, FullTextIndexRead};
use super::tokenizers::Tokenizer;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{FieldCondition, PayloadKeyType};

/// Read-only counterpart of [`MutableFullTextIndex`][1] / [`ImmutableFullTextIndex`][2].
///
/// Thin wrapper over an already-immutable [`MmapFullTextIndex<S>`][3] backing
/// format, parameterised by a [`UniversalRead`] storage. The
/// [`PayloadFieldIndexRead`] body (filter / cardinality / payload blocks /
/// condition checker) is shared with the writable variant through the
/// [`FullTextIndexRead`] trait and the free functions in [`read_ops`][4]; the
/// wrapper just re-tags telemetry and surfaces the `S`-generic version of the
/// same data on the read-only side of `ReadOnlyFieldIndex`.
///
/// Not yet constructible — lifecycle (open / build) lands in the follow-up PR
/// that wires up the rest of [`ReadOnlyFieldIndex`][5], matching the dead-code
/// state of `ReadOnlyNullIndex` / `ReadOnlyBoolIndex` / `ReadOnlyMapIndex` /
/// `ReadOnlyGeoMapIndex`.
///
/// [1]: super::mutable_text_index::MutableFullTextIndex
/// [2]: super::immutable_text_index::ImmutableFullTextIndex
/// [3]: super::mmap_text_index::MmapFullTextIndex
/// [4]: super::read_ops
/// [5]: crate::index::field_index::field_index_base::read_only::ReadOnlyFieldIndex
pub struct ReadOnlyFullTextIndex<S: UniversalRead> {
    #[allow(dead_code)]
    inner: MmapFullTextIndex<S>,
}

impl<S: UniversalRead> FullTextIndexRead for ReadOnlyFullTextIndex<S> {
    fn tokenizer(&self) -> &Tokenizer {
        &self.inner.tokenizer
    }

    fn telemetry_index_type(&self) -> &'static str {
        "read_only_full_text"
    }

    fn points_count(&self) -> usize {
        self.inner.inverted_index.points_count()
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        self.inner.inverted_index.values_count(point_id)
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.inner.inverted_index.values_is_empty(point_id)
    }

    fn for_each_token_id<'a, U: UserData>(
        &self,
        iter: impl Iterator<Item = (U, &'a str)>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(U, Option<TokenId>),
    ) -> OperationResult<()> {
        self.inner
            .inverted_index
            .for_each_token_id(iter, hw_counter, f)
    }

    fn filter_query<'a>(
        &'a self,
        query: ParsedQuery,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        self.inner.inverted_index.filter(query, hw_counter)
    }

    fn estimate_query_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        self.inner
            .inverted_index
            .estimate_cardinality(query, condition, hw_counter)
    }

    fn check_match(&self, query: &ParsedQuery, point_id: PointOffsetType) -> OperationResult<bool> {
        self.inner.inverted_index.check_match(query, point_id)
    }

    fn for_each_payload_block_inner(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.inner
            .inverted_index
            .for_each_payload_block(threshold, key, f)
    }
}

impl<S: UniversalRead> PayloadFieldIndexRead for ReadOnlyFullTextIndex<S> {
    fn count_indexed_points(&self) -> usize {
        FullTextIndexRead::points_count(self)
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
    ) -> Option<ConditionCheckerFn<'a>> {
        read_ops::condition_checker(self, condition, hw_acc)
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
