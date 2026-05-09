pub mod immutable_null_index;
pub mod mutable_null_index;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
pub use immutable_null_index::ImmutableNullIndex;
pub use mutable_null_index::MutableNullIndex;
use serde_json::Value;

use super::{PayloadFieldIndex, PayloadFieldIndexRead};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::telemetry::PayloadIndexTelemetry;

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

    pub fn values_count(&self, id: PointOffsetType) -> usize {
        match self {
            NullIndex::Mutable(mutable) => mutable.values_count(id),
            NullIndex::Immutable(immutable) => immutable.values_count(id),
        }
    }

    pub fn values_is_empty(&self, id: PointOffsetType) -> bool {
        match self {
            NullIndex::Mutable(mutable) => mutable.values_is_empty(id),
            NullIndex::Immutable(immutable) => immutable.values_is_empty(id),
        }
    }

    pub fn values_is_null(&self, id: PointOffsetType) -> bool {
        match self {
            NullIndex::Mutable(mutable) => mutable.values_is_null(id),
            NullIndex::Immutable(immutable) => immutable.values_is_null(id),
        }
    }

    pub fn populate(&self) -> OperationResult<()> {
        match self {
            NullIndex::Mutable(mutable) => mutable.populate(),
            NullIndex::Immutable(immutable) => immutable.populate(),
        }
    }

    /// Approximate RAM usage in bytes.
    pub fn ram_usage_bytes(&self) -> usize {
        match self {
            NullIndex::Mutable(mutable) => mutable.ram_usage_bytes(),
            NullIndex::Immutable(immutable) => immutable.ram_usage_bytes(),
        }
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            NullIndex::Mutable(mutable) => mutable.is_on_disk(),
            NullIndex::Immutable(immutable) => immutable.is_on_disk(),
        }
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            NullIndex::Mutable(mutable) => mutable.clear_cache(),
            NullIndex::Immutable(immutable) => immutable.clear_cache(),
        }
    }
    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            NullIndex::Mutable(_) => IndexMutability::Mutable,
            NullIndex::Immutable(_) => IndexMutability::Immutable,
        }
    }

    pub fn get_storage_type(&self) -> StorageType {
        match self {
            NullIndex::Mutable(mutable) => mutable.get_storage_type(),
            NullIndex::Immutable(immutable) => immutable.get_storage_type(),
        }
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        match self {
            NullIndex::Mutable(mutable) => mutable.get_telemetry_data(),
            NullIndex::Immutable(immutable) => immutable.get_telemetry_data(),
        }
    }
}

impl PayloadFieldIndexRead for NullIndex {
    fn count_indexed_points(&self) -> usize {
        match self {
            NullIndex::Mutable(mutable) => mutable.count_indexed_points(),
            NullIndex::Immutable(immutable) => immutable.count_indexed_points(),
        }
    }

    fn filter<'a>(
        &'a self,
        condition: &'a crate::types::FieldCondition,
        hw_counter: &'a common::counter::hardware_counter::HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        match self {
            NullIndex::Mutable(mutable) => mutable.filter(condition, hw_counter),
            NullIndex::Immutable(immutable) => immutable.filter(condition, hw_counter),
        }
    }

    fn estimate_cardinality(
        &self,
        condition: &crate::types::FieldCondition,
        hw_counter: &common::counter::hardware_counter::HardwareCounterCell,
    ) -> OperationResult<Option<super::CardinalityEstimation>> {
        match self {
            NullIndex::Mutable(mutable) => mutable.estimate_cardinality(condition, hw_counter),
            NullIndex::Immutable(immutable) => {
                immutable.estimate_cardinality(condition, hw_counter)
            }
        }
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: crate::types::PayloadKeyType,
        f: &mut dyn FnMut(super::PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            NullIndex::Mutable(mutable) => mutable.for_each_payload_block(threshold, key, f),
            NullIndex::Immutable(immutable) => immutable.for_each_payload_block(threshold, key, f),
        }
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
        match self {
            NullIndex::Mutable(mutable) => mutable.files(),
            NullIndex::Immutable(immutable) => immutable.files(),
        }
    }

    fn immutable_files(&self) -> Vec<std::path::PathBuf> {
        match self {
            NullIndex::Mutable(mutable) => mutable.immutable_files(),
            NullIndex::Immutable(immutable) => immutable.immutable_files(),
        }
    }
}
