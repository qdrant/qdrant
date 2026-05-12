use std::borrow::Cow;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use gridstore::Blob;

use super::super::super::read_ops::MapIndexRead;
use super::super::super::{IdIter, MapIndexKey};
use super::ReadOnlyAppendableMapIndex;
use crate::common::operation_error::OperationResult;
use crate::index::payload_config::StorageType;

impl<N: MapIndexKey + ?Sized, S: UniversalRead> MapIndexRead<N> for ReadOnlyAppendableMapIndex<N, S>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&N) -> bool,
    ) -> bool {
        self.inner.check_values_any(idx, hw_counter, check_fn)
    }

    fn get_values<'a>(
        &'a self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<impl Iterator<Item = Cow<'a, N>> + 'a>
    where
        N: 'a,
    {
        self.inner.get_values(idx, hw_counter)
    }

    fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.inner.values_count(idx)
    }

    fn get_indexed_points(&self) -> usize {
        self.inner.get_indexed_points()
    }

    fn get_values_count(&self) -> usize {
        self.inner.get_values_count()
    }

    fn get_unique_values_count(&self) -> usize {
        self.inner.get_unique_values_count()
    }

    fn get_count_for_value(&self, value: &N, hw_counter: &HardwareCounterCell) -> Option<usize> {
        self.inner.get_count_for_value(value, hw_counter)
    }

    fn get_iterator(&self, value: &N, hw_counter: &HardwareCounterCell) -> IdIter<'_> {
        self.inner.get_iterator(value, hw_counter)
    }

    fn for_each_value(&self, f: impl FnMut(&N) -> OperationResult<()>) -> OperationResult<()> {
        self.inner.for_each_value(f)
    }

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(&N, usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.inner.for_each_count_per_value(deferred_internal_id, f)
    }

    fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.inner.for_each_value_map(hw_counter, f)
    }

    fn storage_type(&self) -> StorageType {
        // The on-disk format is Gridstore; the read-only-vs-mutable
        // distinction is not yet reflected in [`StorageType`].
        StorageType::Gridstore
    }

    fn ram_usage_bytes(&self) -> usize {
        self.inner.ram_usage_bytes()
    }
}

impl<N: MapIndexKey + ?Sized, S: UniversalRead> ReadOnlyAppendableMapIndex<N, S>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        f: impl FnMut(PointOffsetType, &[<N as MapIndexKey>::Owned]),
    ) {
        self.inner.for_points_values(points, f);
    }
}
