use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;

use super::super::read_ops::{self, BoolIndexRead};
use super::ImmutableBoolIndex;
use crate::common::flags::roaring_flags::RoaringFlags;
use crate::common::operation_error::OperationResult;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::types::{FieldCondition, PayloadKeyType};

impl BoolIndexRead for ImmutableBoolIndex {
    type Flags = RoaringFlags<MmapFile>;

    fn trues_flags(&self) -> &Self::Flags {
        self.0.trues_flags()
    }

    fn falses_flags(&self) -> &Self::Flags {
        self.0.falses_flags()
    }

    fn indexed_count(&self, hw_counter: &HardwareCounterCell) -> OperationResult<usize> {
        self.0.indexed_count(hw_counter)
    }

    fn telemetry_index_type(&self) -> &'static str {
        self.0.telemetry_index_type()
    }

    fn trues_count(&self, hw_counter: &HardwareCounterCell) -> OperationResult<usize> {
        self.0.trues_count(hw_counter)
    }

    fn falses_count(&self, hw_counter: &HardwareCounterCell) -> OperationResult<usize> {
        self.0.falses_count(hw_counter)
    }
}

impl PayloadFieldIndexRead for ImmutableBoolIndex {
    #[inline]
    fn count_indexed_points(&self, hw_counter: &HardwareCounterCell) -> OperationResult<usize> {
        self.indexed_count(hw_counter)
    }

    #[inline]
    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        read_ops::filter(self, condition, hw_counter)
    }

    #[inline]
    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        read_ops::estimate_cardinality(self, condition, hw_counter)
    }

    #[inline]
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
            .map(ConditionCheckerEnum::BoolImmutable))
    }
}
