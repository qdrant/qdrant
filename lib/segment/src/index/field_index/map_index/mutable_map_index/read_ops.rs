use std::borrow::{Borrow, Cow};
use std::iter;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;
use roaring::RoaringBitmap;

use super::super::read_ops::MapIndexRead;
use super::super::{IdIter, MapIndexKey};
use super::MutableMapIndex;
use crate::common::operation_error::OperationResult;
use crate::index::payload_config::StorageType;

impl<N: MapIndexKey + ?Sized> MapIndexRead<N> for MutableMapIndex<N>
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
            .get(idx as usize)
            .map(|values| values.iter().any(|v| check_fn(v.borrow())))
            .unwrap_or(false)
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
                .get(idx as usize)?
                .iter()
                .map(|v| Cow::Borrowed(v.borrow())),
        )
    }

    fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.point_to_values.get(idx as usize).map(Vec::len)
    }

    fn get_indexed_points(&self) -> usize {
        self.indexed_points
    }

    fn get_values_count(&self) -> usize {
        self.values_count
    }

    fn get_unique_values_count(&self) -> usize {
        self.map.len()
    }

    fn get_count_for_value(&self, value: &N, _hw_counter: &HardwareCounterCell) -> Option<usize> {
        self.map.get(value).map(|p| p.len() as usize)
    }

    fn get_iterator(&self, value: &N, _hw_counter: &HardwareCounterCell) -> IdIter<'_> {
        self.map
            .get(value)
            .map(|ids| Box::new(ids.iter()) as IdIter)
            .unwrap_or_else(|| Box::new(iter::empty::<PointOffsetType>()))
    }

    fn for_each_value(&self, mut f: impl FnMut(&N) -> OperationResult<()>) -> OperationResult<()> {
        self.map.keys().try_for_each(|v| f(v.borrow()))
    }

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        mut f: impl FnMut(&N, usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.map.iter().try_for_each(|(k, v)| {
            let count = match deferred_internal_id {
                Some(deferred_internal_id) => v.range_cardinality(..deferred_internal_id) as usize,
                None => v.len() as usize,
            };
            f(k.borrow(), count)
        })
    }

    fn for_each_value_map(
        &self,
        _hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.map
            .iter()
            .try_for_each(|(k, v)| f(k.borrow(), &mut v.iter()))
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Gridstore
    }

    /// Approximate RAM usage in bytes for in-memory index structures.
    fn ram_usage_bytes(&self) -> usize {
        let Self {
            map,
            point_to_values,
            indexed_points: _,
            values_count: _,
            storage: _, // disk-backed, accounted via files
        } = self;

        let hashmap_entry_overhead = std::mem::size_of::<u64>() + std::mem::size_of::<usize>();
        let map_base_bytes = map.capacity()
            * (std::mem::size_of::<<N as MapIndexKey>::Owned>()
                + std::mem::size_of::<RoaringBitmap>()
                + hashmap_entry_overhead);
        // Account for heap-allocated key data (e.g., long strings)
        let map_key_heap_bytes: usize = map.keys().map(|k| N::owned_heap_bytes(k)).sum();
        let map_bitmap_bytes: usize = map.values().map(|bitmap| bitmap.serialized_size()).sum();
        let map_bytes = map_base_bytes + map_key_heap_bytes + map_bitmap_bytes;
        let ptv_bytes: usize = point_to_values.capacity()
            * std::mem::size_of::<Vec<<N as MapIndexKey>::Owned>>()
            + point_to_values
                .iter()
                .map(|v| v.capacity() * std::mem::size_of::<<N as MapIndexKey>::Owned>())
                .sum::<usize>();
        map_bytes + ptv_bytes
    }
}

impl<N: MapIndexKey + ?Sized> MutableMapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        mut f: impl FnMut(PointOffsetType, &[<N as MapIndexKey>::Owned]),
    ) {
        points.for_each(|idx| {
            if let Some(values) = self.point_to_values.get(idx as usize) {
                f(idx, values);
            }
        });
    }
}
