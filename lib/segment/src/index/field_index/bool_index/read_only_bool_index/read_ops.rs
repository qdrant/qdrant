use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::super::read_ops::{self, BoolIndexRead};
use super::ReadOnlyBoolIndex;
use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;
use crate::common::operation_error::OperationResult;
use crate::data_types::facets::{FacetHit, FacetValueRef};
use crate::index::field_index::facet_index::FacetIndex;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::query_optimization::optimized_filter::DynConditionChecker;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::types::{FieldCondition, PayloadKeyType};

impl<S: UniversalRead> ReadOnlyBoolIndex<S> {
    /// Produce a closure that maps a point id to its indexed bool
    /// values as JSON `Value`s. Used by `ReadOnlyFieldIndex::value_retriever`.
    pub fn value_retriever<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        read_ops::value_retriever(self, hw_counter)
    }
}

impl<S: UniversalRead> BoolIndexRead for ReadOnlyBoolIndex<S> {
    type Flags = ReadOnlyRoaringFlags<S>;

    fn trues_flags(&self) -> &Self::Flags {
        &self.storage.trues_flags
    }

    fn falses_flags(&self) -> &Self::Flags {
        &self.storage.falses_flags
    }

    fn indexed_count(&self) -> usize {
        self.indexed_count
    }

    fn telemetry_index_type(&self) -> &'static str {
        "read_only_bool_index"
    }

    fn trues_count(&self) -> usize {
        self.trues_count
    }

    fn falses_count(&self) -> usize {
        self.falses_count
    }
}

impl<S: UniversalRead> PayloadFieldIndexRead for ReadOnlyBoolIndex<S> {
    fn count_indexed_points(&self) -> usize {
        self.indexed_count()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        Ok(read_ops::filter(self, condition, hw_counter))
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        Ok(read_ops::estimate_cardinality(self, condition, hw_counter))
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
    ) -> OperationResult<Option<DynConditionChecker<'a>>> {
        Ok(read_ops::condition_checker(self, condition, hw_acc))
    }
}

/// Faceting over the read-only bool index mirrors the `FacetIndex` impl for
/// `BoolIndex`: every method delegates to a [`BoolIndexRead`] default, so the
/// body is identical — only the `Self` type differs.
impl<S: UniversalRead> FacetIndex for ReadOnlyBoolIndex<S> {
    fn unique_values_count(&self) -> usize {
        // Upper bound; see `BoolIndex::unique_values_count` for rationale.
        2
    }

    fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(PointOffsetType, &mut dyn Iterator<Item = FacetValueRef<'_>>),
    ) -> OperationResult<()> {
        points.for_each(|point_id| {
            let values = self.get_point_values(point_id);
            f(point_id, &mut values.into_iter().map(FacetValueRef::Bool));
        });
        Ok(())
    }

    fn for_each_value(
        &self,
        mut f: impl FnMut(FacetValueRef<'_>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        BoolIndexRead::iter_values(self).try_for_each(|v| f(FacetValueRef::Bool(v)))
    }

    fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(
            FacetValueRef<'_>,
            &mut dyn Iterator<Item = PointOffsetType>,
        ) -> OperationResult<()>,
    ) -> OperationResult<()> {
        BoolIndexRead::for_each_value_map(self, hw_counter, |value, iter| {
            f(FacetValueRef::Bool(value), iter)
        })
    }

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        mut f: impl FnMut(FacetHit<FacetValueRef<'_>>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        BoolIndexRead::for_each_count_per_value(self, deferred_internal_id, |value, count| {
            f(FacetHit {
                value: FacetValueRef::Bool(value),
                count,
            })
        })
    }
}
