use std::borrow::{Borrow as _, Cow};
use std::iter;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;

use super::super::read_ops::MapIndexRead;
use super::super::{IdIter, MapIndexKey};
use super::{ContainerSegment, ImmutableMapIndex, Storage};
use crate::common::operation_error::OperationResult;
use crate::index::payload_config::StorageType;

impl<N: MapIndexKey + ?Sized> MapIndexRead<N> for ImmutableMapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&N) -> bool,
    ) -> bool {
        self.point_to_values
            .check_values_any(idx, |v| check_fn(v.borrow()))
    }

    fn get_values<'a>(
        &'a self,
        idx: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> Option<impl Iterator<Item = Cow<'a, N>> + 'a>
    where
        N: 'a,
    {
        Some(
            self.point_to_values
                .get_values(idx)?
                .map(|v| Cow::Borrowed(v.borrow())),
        )
    }

    fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        Some(self.point_to_values.get_values(idx)?.count())
    }

    fn get_indexed_points(&self) -> usize {
        self.indexed_points
    }

    fn get_values_count(&self) -> usize {
        self.values_count
    }

    fn get_unique_values_count(&self) -> usize {
        self.value_to_points.len()
    }

    fn get_count_for_value(&self, value: &N, _hw_counter: &HardwareCounterCell) -> Option<usize> {
        self.value_to_points
            .get(value)
            .map(|entry| entry.count as usize)
    }

    fn get_iterator(&self, value: &N, _hw_counter: &HardwareCounterCell) -> IdIter<'_> {
        if let Some(entry) = self.value_to_points.get(value) {
            Box::new(self.get_entry_iterator(entry))
        } else {
            Box::new(iter::empty::<PointOffsetType>())
        }
    }

    fn for_each_value(&self, mut f: impl FnMut(&N) -> OperationResult<()>) -> OperationResult<()> {
        self.value_to_points.keys().try_for_each(|v| f(v.borrow()))
    }

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        mut f: impl FnMut(&N, usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        // Immutable indexes don't support deferred filtering; callers must
        // pass `None`. See `MapIndex::for_each_count_per_value` for context.
        debug_assert!(deferred_internal_id.is_none());
        let _ = deferred_internal_id;
        self.value_to_points
            .iter()
            .try_for_each(|(k, entry)| f(k.borrow(), entry.count as usize))
    }

    fn for_each_value_map(
        &self,
        _hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.value_to_points
            .iter()
            .try_for_each(|(k, entry)| f(k.borrow(), &mut self.get_entry_iterator(entry)))
    }

    fn storage_type(&self) -> StorageType {
        match &self.storage {
            Storage::Mmap(index) => StorageType::Mmap {
                is_on_disk: index.is_on_disk(),
            },
        }
    }

    /// Approximate RAM usage in bytes (cached at construction).
    fn ram_usage_bytes(&self) -> usize {
        self.cached_ram_usage_bytes
    }
}

impl<N: MapIndexKey + ?Sized> ImmutableMapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        mut f: impl FnMut(PointOffsetType, &[<N as MapIndexKey>::Owned]),
    ) {
        points.for_each(|idx| {
            if let Some(values) = self.point_to_values.get_values_slice(idx) {
                f(idx, values);
            }
        });
    }

    fn get_entry_iterator(
        &self,
        entry: &ContainerSegment,
    ) -> impl Iterator<Item = PointOffsetType> {
        let range = entry.range.start as usize..entry.range.end as usize;

        let deleted_flags = self
            .deleted_value_to_points_container
            .iter()
            .by_vals()
            .skip(range.start)
            .chain(std::iter::repeat(false));

        self.value_to_points_container[range]
            .iter()
            .zip(deleted_flags)
            .filter(|(_, is_deleted)| !is_deleted)
            .map(|(idx, _)| *idx)
    }

    pub(super) fn compute_ram_usage_bytes(&self) -> usize {
        let Self {
            value_to_points,
            value_to_points_container,
            deleted_value_to_points_container,
            point_to_values,
            indexed_points: _,
            values_count: _,
            storage: _,
            cached_ram_usage_bytes: _,
        } = self;

        let hashmap_entry_overhead = size_of::<u64>() + size_of::<usize>();
        let vtp_base_bytes: usize = value_to_points.capacity()
            * (size_of::<<N as MapIndexKey>::Owned>()
                + size_of::<ContainerSegment>()
                + hashmap_entry_overhead);
        // Account for heap-allocated key data (e.g., long strings)
        let vtp_heap_bytes: usize = value_to_points.keys().map(|k| N::owned_heap_bytes(k)).sum();
        let container_bytes = value_to_points_container.capacity() * size_of::<PointOffsetType>();
        let deleted_bytes = deleted_value_to_points_container
            .capacity()
            .div_ceil(u8::BITS as usize);
        vtp_base_bytes
            + vtp_heap_bytes
            + container_bytes
            + deleted_bytes
            + point_to_values.ram_usage_bytes()
    }
}
