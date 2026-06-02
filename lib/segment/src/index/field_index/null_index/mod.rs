pub mod immutable_null_index;
pub mod mutable_null_index;
pub mod read_only_null_index;
mod read_ops;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;
pub use immutable_null_index::ImmutableNullIndex;
pub use mutable_null_index::MutableNullIndex;
pub use read_only_null_index::ReadOnlyNullIndex;
pub use read_ops::NullIndexRead;
use serde_json::Value;

use super::{PayloadFieldIndex, PayloadFieldIndexRead};
use crate::common::flags::roaring_flags::RoaringFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::payload_config::IndexMutability;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::types::FieldCondition;

pub enum NullIndex {
    Mutable(MutableNullIndex),
    Immutable(ImmutableNullIndex),
}

impl From<MutableNullIndex> for NullIndex {
    fn from(value: MutableNullIndex) -> Self {
        NullIndex::Mutable(value)
    }
}

impl From<ImmutableNullIndex> for NullIndex {
    fn from(value: ImmutableNullIndex) -> Self {
        NullIndex::Immutable(value)
    }
}

impl NullIndex {
    pub fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            NullIndex::Mutable(mutable) => mutable.add_point(id, payload, hw_counter),
            NullIndex::Immutable(_immutable) => Err(OperationError::service_error(
                "Can't add values to immutable null index",
            )),
        }
    }

    pub fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        match self {
            NullIndex::Mutable(mutable) => mutable.remove_point(id),
            NullIndex::Immutable(immutable) => immutable.remove_point(id),
        }
    }

    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            NullIndex::Mutable(_) => IndexMutability::Mutable,
            NullIndex::Immutable(_) => IndexMutability::Immutable,
        }
    }
}

impl NullIndexRead for NullIndex {
    type Flags = RoaringFlags<MmapFile>;

    fn has_values_flags(&self) -> &Self::Flags {
        match self {
            NullIndex::Mutable(m) => m.has_values_flags(),
            NullIndex::Immutable(i) => i.has_values_flags(),
        }
    }

    fn is_null_flags(&self) -> &Self::Flags {
        match self {
            NullIndex::Mutable(m) => m.is_null_flags(),
            NullIndex::Immutable(i) => i.is_null_flags(),
        }
    }

    fn total_point_count(&self) -> usize {
        match self {
            NullIndex::Mutable(m) => m.total_point_count(),
            NullIndex::Immutable(i) => i.total_point_count(),
        }
    }

    fn telemetry_index_type(&self) -> &'static str {
        match self {
            NullIndex::Mutable(m) => m.telemetry_index_type(),
            NullIndex::Immutable(i) => i.telemetry_index_type(),
        }
    }
}

impl PayloadFieldIndexRead for NullIndex {
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
    ) -> OperationResult<Option<super::CardinalityEstimation>> {
        Ok(read_ops::estimate_cardinality(self, condition))
    }

    fn for_each_payload_block(
        &self,
        _threshold: usize,
        _key: crate::types::PayloadKeyType,
        _f: &mut dyn FnMut(super::PayloadBlockCondition) -> OperationResult<()>,
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

impl PayloadFieldIndex for NullIndex {
    fn wipe(self) -> OperationResult<()> {
        match self {
            NullIndex::Mutable(mutable) => mutable.wipe(),
            NullIndex::Immutable(immutable) => immutable.wipe(),
        }
    }

    fn flusher(&self) -> crate::common::Flusher {
        match self {
            NullIndex::Mutable(mutable) => mutable.flusher(),
            NullIndex::Immutable(immutable) => immutable.flusher(),
        }
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        NullIndexRead::files(self)
    }

    fn immutable_files(&self) -> Vec<std::path::PathBuf> {
        match self {
            NullIndex::Mutable(_) => Vec::new(),
            NullIndex::Immutable(immutable) => immutable.immutable_files(),
        }
    }
}
