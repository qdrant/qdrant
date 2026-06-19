use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::iter;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;
use roaring::RoaringBitmap;

use super::super::read_ops::MapIndexRead;
use super::super::{IdIter, MapIndexKey};
use crate::common::operation_error::OperationResult;
use crate::index::payload_config::StorageType;

/// In-memory state shared by `MutableMapIndex` and `ReadOnlyAppendableMapIndex`.
///
/// Both wrappers add a different backing storage (`Gridstore` vs
/// `GridstoreReader`); the in-memory layout that serves every
/// [`MapIndexRead`] method is the same, so it lives here once.
pub(in crate::index::field_index::map_index) struct InMemoryMapIndex<N: MapIndexKey + ?Sized>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub(in crate::index::field_index::map_index) map:
        HashMap<<N as MapIndexKey>::Owned, RoaringBitmap>,
    pub(in crate::index::field_index::map_index) point_to_values:
        Vec<Vec<<N as MapIndexKey>::Owned>>,
    /// Amount of point which have at least one indexed payload value
    pub(in crate::index::field_index::map_index) indexed_points: usize,
    pub(in crate::index::field_index::map_index) values_count: usize,
}

impl<N: MapIndexKey + ?Sized> InMemoryMapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub(in crate::index::field_index::map_index) fn empty() -> Self {
        Self {
            map: HashMap::new(),
            point_to_values: Vec::new(),
            indexed_points: 0,
            values_count: 0,
        }
    }

    pub fn add_many_to_map(&mut self, idx: u32, values: Vec<<N as MapIndexKey>::Owned>) {
        if values.is_empty() {
            return;
        }

        self.values_count += values.len();
        if self.point_to_values.len() <= idx as usize {
            self.point_to_values.resize_with(idx as usize + 1, Vec::new)
        }

        self.point_to_values[idx as usize] = Vec::with_capacity(values.len());

        for value in values {
            let entry = self.map.entry(value.clone());
            let inserted = entry.or_default().insert(idx);

            // only insert into forward index if it is not a duplicate
            if inserted {
                self.point_to_values[idx as usize].push(value);
            }
        }

        self.indexed_points += 1;
    }

    /// Remove a point from the in-memory state, updating the value map and
    /// counters accordingly.
    ///
    /// Returns `true` if the point was within the indexed range; callers backed
    /// by a store should delete the on-disk entry only in that case.
    pub fn remove_point(&mut self, idx: PointOffsetType) -> bool {
        if self.point_to_values.len() <= idx as usize {
            return false;
        }

        let removed_values = std::mem::take(&mut self.point_to_values[idx as usize]);

        if !removed_values.is_empty() {
            self.indexed_points -= 1;
        }
        self.values_count -= removed_values.len();

        for value in &removed_values {
            if let Some(vals) = self.map.get_mut(value.borrow()) {
                vals.remove(idx);
            }
        }

        true
    }

    pub(in crate::index::field_index::map_index) fn for_points_values(
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

impl<'a, N: MapIndexKey + ?Sized + 'a> MapIndexRead<'a, N> for InMemoryMapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&N) -> bool,
    ) -> OperationResult<bool> {
        Ok(self
            .point_to_values
            .get(idx as usize)
            .map(|values| values.iter().any(|v| check_fn(v.borrow())))
            .unwrap_or(false))
    }

    fn get_values(
        &'a self,
        idx: PointOffsetType,
        _hw_counter: &HardwareCounterCell,
    ) -> Option<impl Iterator<Item = Cow<'a, N>> + 'a> {
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

    fn for_each_value_map(
        &self,
        _hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.map
            .iter()
            .try_for_each(|(k, v)| f(k.borrow(), &mut v.iter()))
    }

    /// Placeholder — neither wrapper exposes the inner directly; both override
    /// `storage_type()` in their own `MapIndexRead` impls to report the
    /// concrete storage. The inner is never consumed as a bare
    /// `&dyn MapIndexRead`, so this value is unobservable in practice.
    fn storage_type(&self) -> StorageType {
        StorageType::Gridstore
    }

    /// Placeholder telemetry tag — like [`Self::storage_type`], the inner is
    /// never queried directly; wrappers override this on their own
    /// [`MapIndexRead`] impls.
    fn telemetry_index_type(&self) -> &'static str {
        "mutable_map"
    }

    /// Approximate RAM usage in bytes for in-memory index structures.
    fn ram_usage_bytes(&self) -> usize {
        let Self {
            map,
            point_to_values,
            indexed_points: _,
            values_count: _,
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
