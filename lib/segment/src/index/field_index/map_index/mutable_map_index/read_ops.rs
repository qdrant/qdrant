use std::borrow::Cow;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;

use super::super::read_ops::MapIndexRead;
use super::super::{IdIter, MapIndexKey};
use super::MutableMapIndex;
use crate::common::operation_error::OperationResult;
use crate::index::payload_config::StorageType;

impl<'a, N: MapIndexKey + ?Sized + 'a> MapIndexRead<'a, N> for MutableMapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&N) -> bool,
    ) -> OperationResult<bool> {
        self.in_memory_index
            .check_values_any(idx, hw_counter, check_fn)
    }

    fn get_values(
        &'a self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<impl Iterator<Item = Cow<'a, N>> + 'a> {
        self.in_memory_index.get_values(idx, hw_counter)
    }

    fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.in_memory_index.values_count(idx)
    }

    fn get_indexed_points(&self) -> usize {
        self.in_memory_index.get_indexed_points()
    }

    fn get_values_count(&self) -> usize {
        self.in_memory_index.get_values_count()
    }

    fn get_unique_values_count(&self) -> usize {
        self.in_memory_index.get_unique_values_count()
    }

    fn get_count_for_value(&self, value: &N, hw_counter: &HardwareCounterCell) -> Option<usize> {
        self.in_memory_index.get_count_for_value(value, hw_counter)
    }

    fn get_iterator(&self, value: &N, hw_counter: &HardwareCounterCell) -> IdIter<'_> {
        self.in_memory_index.get_iterator(value, hw_counter)
    }

    fn for_each_value(&self, f: impl FnMut(&N) -> OperationResult<()>) -> OperationResult<()> {
        self.in_memory_index.for_each_value(f)
    }

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(&N, usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.in_memory_index
            .for_each_count_per_value(deferred_internal_id, f)
    }

    fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.in_memory_index.for_each_value_map(hw_counter, f)
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Gridstore
    }

    fn ram_usage_bytes(&self) -> usize {
        self.in_memory_index.ram_usage_bytes()
    }

    fn telemetry_index_type(&self) -> &'static str {
        "mutable_map"
    }
}

impl<N: MapIndexKey + ?Sized> MutableMapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        f: impl FnMut(PointOffsetType, &[<N as MapIndexKey>::Owned]),
    ) {
        self.in_memory_index.for_points_values(points, f);
    }
}
