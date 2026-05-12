use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::read_ops::{self, NullIndexRead};
use crate::common::flags::read_only_roaring_flags::ReadOnlyRoaringFlags;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndexRead,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::{FieldCondition, PayloadKeyType};

/// Read-only counterpart of [`MutableNullIndex`][1] / [`ImmutableNullIndex`][2].
///
/// All flags are loaded into in-memory roaring bitmaps on open. The backing
/// storage is bound to [`UniversalRead`] only — no buffer, no flusher,
/// no write path. Query logic (filter / cardinality / condition checker) is
/// shared with the writable variant via [`read_logic`][3].
///
/// [1]: super::mutable_null_index::MutableNullIndex
/// [2]: super::immutable_null_index::ImmutableNullIndex
/// [3]: super::shared
pub struct ReadOnlyNullIndex<S: UniversalRead> {
    #[allow(dead_code)]
    _base_dir: PathBuf,
    storage: ReadOnlyStorage<S>,
    total_point_count: usize,
}

struct ReadOnlyStorage<S: UniversalRead> {
    /// Points which have at least one value
    has_values_flags: ReadOnlyRoaringFlags<S>,
    /// Points which have null values
    is_null_flags: ReadOnlyRoaringFlags<S>,
}

impl<S: UniversalRead> NullIndexRead for ReadOnlyNullIndex<S> {
    type Flags = ReadOnlyRoaringFlags<S>;

    fn has_values_flags(&self) -> &Self::Flags {
        &self.storage.has_values_flags
    }

    fn is_null_flags(&self) -> &Self::Flags {
        &self.storage.is_null_flags
    }

    fn total_point_count(&self) -> usize {
        self.total_point_count
    }

    fn telemetry_index_type(&self) -> &'static str {
        "read_only_null_index"
    }
}

impl<S: UniversalRead> PayloadFieldIndexRead for ReadOnlyNullIndex<S> {
    fn count_indexed_points(&self) -> usize {
        self.indexed_points_count()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        _hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        Ok(read_ops::filter(self, condition))
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        Ok(read_ops::estimate_cardinality(self, condition))
    }

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
    ) -> Option<ConditionCheckerFn<'a>> {
        read_ops::condition_checker(self, condition, hw_acc)
    }
}
