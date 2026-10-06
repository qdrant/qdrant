use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::super::read_ops::{self, BoolIndexRead};
use super::ReadOnlyBoolIndex;
use crate::common::flags::read_only_flags::ReadOnlyFlags;
use crate::common::operation_error::OperationResult;
use crate::data_types::facets::{FacetHit, FacetValue, FacetValueRef};
use crate::index::UniversalReadExt;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::facet_index::FacetIndex;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::types::{FieldCondition, PayloadKeyType};

impl<S: UniversalReadExt> ReadOnlyBoolIndex<S> {
    /// Produce a closure that maps a point id to its indexed bool
    /// values as JSON `Value`s. Used by `ReadOnlyFieldIndex::value_retriever`.
    pub fn value_retriever<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<VariableRetrieverFn<'a>> {
        read_ops::value_retriever(self, hw_counter)
    }
}

impl<S: UniversalReadExt> BoolIndexRead for ReadOnlyBoolIndex<S> {
    type Flags = ReadOnlyFlags<S>;

    fn trues_flags(&self) -> &Self::Flags {
        &self.storage.trues_flags
    }

    fn falses_flags(&self) -> &Self::Flags {
        &self.storage.falses_flags
    }

    fn indexed_count(&self, hw_counter: &HardwareCounterCell) -> OperationResult<usize> {
        Ok(self.counts(hw_counter)?.indexed)
    }

    fn telemetry_index_type(&self) -> &'static str {
        "read_only_bool_index"
    }

    fn trues_count(&self, hw_counter: &HardwareCounterCell) -> OperationResult<usize> {
        Ok(self.counts(hw_counter)?.trues)
    }

    fn falses_count(&self, hw_counter: &HardwareCounterCell) -> OperationResult<usize> {
        Ok(self.counts(hw_counter)?.falses)
    }
}

impl<S: UniversalReadExt> PayloadFieldIndexRead for ReadOnlyBoolIndex<S> {
    fn count_indexed_points(&self, hw_counter: &HardwareCounterCell) -> OperationResult<usize> {
        self.indexed_count(hw_counter)
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
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        read_ops::for_each_payload_block(self, threshold, key, f, hw_counter)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> OperationResult<Option<ConditionCheckerEnum<'a>>> {
        Ok(read_ops::condition_checker(self, condition, hw_acc)
            .map(|x| UniversalReadExt::condition_checker_bool(x)))
    }
}

/// Faceting over the read-only bool index mirrors the `FacetIndex` impl for
/// `BoolIndex`: every method delegates to a [`BoolIndexRead`] default, so the
/// body is identical — only the `Self` type differs.
impl<S: UniversalReadExt> FacetIndex for ReadOnlyBoolIndex<S> {
    fn unique_values_count(&self) -> usize {
        // Upper bound; see `BoolIndex::unique_values_count` for rationale.
        2
    }

    fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(PointOffsetType, &mut dyn Iterator<Item = FacetValueRef<'_>>),
    ) -> OperationResult<()> {
        for point_id in points {
            let values = self.get_point_values(point_id, hw_counter)?;
            f(point_id, &mut values.into_iter().map(FacetValueRef::Bool));
        }
        Ok(())
    }

    fn for_each_value(
        &self,
        mut f: impl FnMut(FacetValueRef<'_>) -> OperationResult<()>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        BoolIndexRead::iter_values(self, hw_counter)?.try_for_each(|v| f(FacetValueRef::Bool(v)))
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

    fn for_values_map(
        &self,
        values: impl Iterator<Item = FacetValue>,
        hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(FacetValue, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        let bools = values.filter_map(|value| match value {
            FacetValue::Bool(b) => Some(b),
            FacetValue::Keyword(_) | FacetValue::Int(_) | FacetValue::Uuid(_) => None,
        });
        BoolIndexRead::for_values_map(self, bools, hw_counter, |b, iter| {
            f(FacetValue::Bool(b), iter)
        })
    }

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        mut f: impl FnMut(FacetHit<FacetValueRef<'_>>) -> OperationResult<()>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        BoolIndexRead::for_each_count_per_value(
            self,
            deferred_internal_id,
            |value, count| {
                f(FacetHit {
                    value: FacetValueRef::Bool(value),
                    count,
                })
            },
            hw_counter,
        )
    }
}
