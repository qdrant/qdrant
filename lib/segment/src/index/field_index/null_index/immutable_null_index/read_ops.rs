use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;

use super::super::read_ops::{self, NullIndexRead};
use super::ImmutableNullIndex;
use crate::common::flags::roaring_flags::RoaringFlags;
use crate::common::operation_error::OperationResult;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::types::{FieldCondition, PayloadKeyType};

impl NullIndexRead for ImmutableNullIndex {
    type Flags = RoaringFlags<MmapFile>;

    fn has_values_flags(&self) -> &Self::Flags {
        self.0.has_values_flags()
    }

    fn is_null_flags(&self) -> &Self::Flags {
        self.0.is_null_flags()
    }

    fn total_point_count(&self) -> usize {
        self.0.total_point_count()
    }

    fn telemetry_index_type(&self) -> &'static str {
        "immutable_null_index"
    }
}

impl PayloadFieldIndexRead for ImmutableNullIndex {
    #[inline]
    fn count_indexed_points(&self) -> OperationResult<usize> {
        Ok(self.indexed_points_count())
    }

    #[inline]
    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        _hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        read_ops::filter(self, condition)
    }

    #[inline]
    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        read_ops::estimate_cardinality(self, condition)
    }

    #[inline]
    fn for_each_payload_block(
        &self,
        _threshold: usize,
        _key: PayloadKeyType,
        _f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        // No payload blocks
        Ok(())
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> OperationResult<Option<ConditionCheckerEnum<'a>>> {
        Ok(read_ops::condition_checker(self, condition, hw_acc)
            .map(ConditionCheckerEnum::NullImmutable))
    }
}
