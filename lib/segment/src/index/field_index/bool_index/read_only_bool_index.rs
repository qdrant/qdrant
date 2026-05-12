use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::read_ops::{self, BoolIndexRead};
use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{FieldCondition, PayloadKeyType};

/// Read-only counterpart of [`MutableBoolIndex`][1] / [`ImmutableBoolIndex`][2].
///
/// All flags are loaded into in-memory roaring bitmaps on open. The backing
/// storage is bound to [`UniversalRead`] only — no buffer, no flusher,
/// no write path. Query logic (filter / cardinality / payload blocks /
/// condition checker) is shared with the writable variants via [`read_ops`][3].
///
/// [1]: super::mutable_bool_index::MutableBoolIndex
/// [2]: super::immutable_bool_index::ImmutableBoolIndex
/// [3]: super::read_ops
pub struct ReadOnlyBoolIndex<S: UniversalRead> {
    #[allow(dead_code)]
    _base_dir: PathBuf,
    storage: ReadOnlyStorage<S>,
    indexed_count: usize,
}

struct ReadOnlyStorage<S: UniversalRead> {
    /// Points which have at least one `true` value
    trues_flags: ReadOnlyRoaringFlags<S>,
    /// Points which have at least one `false` value
    falses_flags: ReadOnlyRoaringFlags<S>,
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
    ) -> Option<ConditionCheckerFn<'a>> {
        read_ops::condition_checker(self, condition, hw_acc)
    }
}
