use std::borrow::{Borrow as _, Cow};
use std::collections::HashMap;
use std::iter;
use std::ops::Range;
use std::path::PathBuf;

use common::bitvec::{BitSliceExt, BitVec};
use common::persisted_hashmap::Key;
use common::types::PointOffsetType;
use gridstore::Blob;

use super::mmap_map_index::MmapMapIndex;
use super::{IdIter, MapIndexKey};
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;
use crate::index::payload_config::StorageType;

pub struct ImmutableMapIndex<N: MapIndexKey + Key + ?Sized> {
    value_to_points: HashMap<<N as MapIndexKey>::Owned, ContainerSegment>,
    /// Container holding a slice of point IDs per value. `value_to_point` holds the range per value.
    /// Each slice MUST be sorted so that we can binary search over it.
    value_to_points_container: Vec<PointOffsetType>,
    deleted_value_to_points_container: BitVec,
    point_to_values: ImmutablePointToValues<<N as MapIndexKey>::Owned>,
    /// Amount of point which have at least one indexed payload value
    indexed_points: usize,
    values_count: usize,
    // Backing storage, source of state, persists deletions
    storage: Storage<N>,
    /// Snapshot of approximate RAM usage at construction time.
    /// Not refreshed on `remove_point`.
    cached_ram_usage_bytes: usize,
}

enum Storage<N: MapIndexKey + Key + ?Sized> {
    Mmap(Box<MmapMapIndex<N>>),
}

pub(super) struct ContainerSegment {
    /// Range in the container which holds point IDs for the value.
    range: Range<u32>,
    /// Number of available point IDs in the range, excludes number of deleted points.
    count: u32,
}

impl<N: MapIndexKey + ?Sized> ImmutableMapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    /// Open and load immutable map index from mmap storage
    pub(super) fn open_mmap(index: MmapMapIndex<N>) -> Self {
        // Construct intermediate values to points map from backing storage
        let mapping = || {
            index.storage.value_to_points.iter().map(|(value, ids)| {
                (
                    value,
                    ids.iter().copied().filter(|idx| {
                        let is_deleted = index
                            .storage
                            .deleted
                            .get_bit(*idx as usize)
                            .unwrap_or(false);
                        !is_deleted
                    }),
                )
            })
        };

        let mut indexed_points = 0;
        let mut values_count = 0;
        let mut value_to_points = HashMap::new();

        // Create points to values mapping
        let mut point_to_values: Vec<Vec<<N as MapIndexKey>::Owned>> = vec![];
        for (value, ids) in mapping() {
            for idx in ids {
                if point_to_values.len() <= idx as usize {
                    point_to_values.resize_with(idx as usize + 1, Vec::new)
                }
                let point_values = &mut point_to_values[idx as usize];

                if point_values.is_empty() {
                    indexed_points += 1;
                }
                values_count += 1;

                point_values.push(MapIndexKey::to_owned(value));
            }
        }
        let point_to_values = ImmutablePointToValues::new(point_to_values);

        // Create flattened values-to-points mapping. Skip values whose live
        // points are all deleted in the backing mmap (e.g., points the
        // id-tracker has deleted at runtime, applied at open time by
        // `MmapMapIndex::open`). This mirrors the runtime invariant in
        // `remove_idx_from_value_list`: `value_to_points` only ever contains
        // entries with `count > 0`.
        let mut value_to_points_container = Vec::with_capacity(values_count);
        for (value, points) in mapping() {
            let points = points.into_iter().collect::<Vec<_>>();
            if points.is_empty() {
                continue;
            }
            let container_len = value_to_points_container.len() as u32;
            let range = container_len..container_len + points.len() as u32;
            value_to_points.insert(
                MapIndexKey::to_owned(value),
                ContainerSegment {
                    count: range.len() as u32,
                    range,
                },
            );
            value_to_points_container.extend(points);
        }
        value_to_points.shrink_to_fit();

        // Sort IDs in each slice of points
        // This is very important because we binary search
        for value in value_to_points.keys() {
            let value: &N = value.borrow();
            if let Some((slice, _offset)) = Self::get_mut_point_ids_slice(
                &value_to_points,
                &mut value_to_points_container,
                value,
            ) {
                slice.sort_unstable();
            } else {
                debug_assert!(false, "value {value} not found in value_to_points");
            }
        }

        debug_assert_eq!(indexed_points, index.get_indexed_points());

        // Index is now loaded into memory, clear cache of backing mmap storage
        if let Err(err) = index.clear_cache() {
            log::warn!("Failed to clear mmap cache of ram mmap map index: {err}");
        }

        let mut result = Self {
            value_to_points,
            value_to_points_container,
            deleted_value_to_points_container: BitVec::new(),
            point_to_values,
            indexed_points,
            values_count,
            storage: Storage::Mmap(Box::new(index)),
            cached_ram_usage_bytes: 0,
        };
        result.cached_ram_usage_bytes = result.compute_ram_usage_bytes();
        result
    }

    /// Return mutable slice of a container which holds point_ids for given value.
    ///
    /// The returned slice is sorted. Positions may correspond to point IDs
    /// also marked in `deleted_value_to_points_container`; callers must filter.
    /// The returned offset is the start of the range in the container.
    fn get_mut_point_ids_slice<'a>(
        value_to_points: &HashMap<<N as MapIndexKey>::Owned, ContainerSegment>,
        value_to_points_container: &'a mut [PointOffsetType],
        value: &N,
    ) -> Option<(&'a mut [PointOffsetType], usize)> {
        match value_to_points.get(value) {
            Some(entry) if entry.count > 0 => {
                let range = entry.range.start as usize..entry.range.end as usize;
                let vals = &mut value_to_points_container[range];
                Some((vals, entry.range.start as usize))
            }
            _ => None,
        }
    }

    /// Shrinks the range of values-to-points by one.
    ///
    /// Returns true if the last element was removed.
    fn shrink_value_range(
        value_to_points: &mut HashMap<<N as MapIndexKey>::Owned, ContainerSegment>,
        value: &N,
    ) -> bool {
        if let Some(entry) = value_to_points.get_mut(value) {
            entry.count = entry.count.saturating_sub(1);
            return entry.count == 0;
        }
        false
    }

    /// Removes `idx` from values-to-points-container.
    /// It is implemented by shrinking the range of values-to-points by one and moving the removed element
    /// out of the range.
    /// Previously last element is swapped with the removed one and then the range is shrank by one.
    ///
    ///
    /// Example:
    ///     Before:
    ///
    /// value_to_points -> {
    ///     "a": 0..5,
    ///     "b": 5..10
    /// }
    /// value_to_points_container -> [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    ///
    /// Args:
    ///   value: "a"
    ///   idx: 3
    ///
    /// After:
    ///
    /// value_to_points -> {
    ///    "a": 0..4,
    ///    "b": 5..10
    /// }
    ///
    /// value_to_points_container -> [0, 1, 2, 4, (3), 5, 6, 7, 8, 9]
    fn remove_idx_from_value_list(
        value_to_points: &mut HashMap<<N as MapIndexKey>::Owned, ContainerSegment>,
        value_to_points_container: &mut [PointOffsetType],
        deleted_value_to_points_container: &mut BitVec,
        value: &N,
        idx: PointOffsetType,
    ) {
        let Some((values, offset)) =
            Self::get_mut_point_ids_slice(value_to_points, value_to_points_container, value)
        else {
            debug_assert!(false, "value {value} not found in value_to_points");
            return;
        };

        // Finds the index of `idx` in values-to-points map which we want to remove
        // We mark it as removed in deleted flags
        if let Ok(local_pos) = values.binary_search(&idx) {
            let pos = offset + local_pos;

            if deleted_value_to_points_container.len() < pos + 1 {
                deleted_value_to_points_container.resize(pos + 1, false);
            }

            #[allow(unused_variables)]
            let did_exist = !deleted_value_to_points_container.replace(pos, true);
            debug_assert!(did_exist, "value {value} was already deleted");
        }

        if Self::shrink_value_range(value_to_points, value) {
            value_to_points.remove(value);
        }
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        if let Some(removed_values) = self.point_to_values.get_values(idx) {
            let mut removed_values_count = 0;
            for value in removed_values {
                Self::remove_idx_from_value_list(
                    &mut self.value_to_points,
                    &mut self.value_to_points_container,
                    &mut self.deleted_value_to_points_container,
                    value.borrow(),
                    idx,
                );

                // Update persisted storage
                match self.storage {
                    Storage::Mmap(ref mut index) => {
                        index.remove_point(idx);
                    }
                }
                removed_values_count += 1;
            }

            if removed_values_count > 0 {
                self.indexed_points = self.indexed_points.saturating_sub(1);
            }
            self.values_count = self.values_count.saturating_sub(removed_values_count);
        }
        self.point_to_values.remove_point(idx);
        Ok(())
    }

    #[inline]
    pub(super) fn wipe(self) -> OperationResult<()> {
        match self.storage {
            Storage::Mmap(index) => index.wipe(),
        }
    }

    /// Clear cache
    ///
    /// Only clears cache of mmap storage if used. Does not clear in-memory representation of
    /// index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self.storage {
            Storage::Mmap(ref index) => index.clear_cache(),
        }
    }

    #[inline]
    pub(super) fn files(&self) -> Vec<PathBuf> {
        match self.storage {
            Storage::Mmap(ref index) => index.files(),
        }
    }

    #[inline]
    pub(super) fn immutable_files(&self) -> Vec<PathBuf> {
        match &self.storage {
            Storage::Mmap(index) => index.immutable_files(),
        }
    }

    #[inline]
    pub(super) fn flusher(&self) -> Flusher {
        match self.storage {
            Storage::Mmap(ref index) => index.flusher(),
        }
    }

    pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&N) -> bool) -> bool {
        self.point_to_values
            .check_values_any(idx, |v| check_fn(v.borrow()))
    }

    pub fn get_values(
        &self,
        idx: PointOffsetType,
    ) -> Option<impl Iterator<Item = Cow<'_, N>> + '_> {
        Some(
            self.point_to_values
                .get_values(idx)?
                .map(|v| Cow::Borrowed(v.borrow())),
        )
    }

    pub fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        Some(self.point_to_values.get_values(idx)?.count())
    }

    pub fn get_indexed_points(&self) -> usize {
        self.indexed_points
    }

    pub fn get_values_count(&self) -> usize {
        self.values_count
    }

    pub fn get_unique_values_count(&self) -> usize {
        self.value_to_points.len()
    }

    pub fn get_count_for_value(&self, value: &N) -> Option<usize> {
        self.value_to_points
            .get(value)
            .map(|entry| entry.count as usize)
    }

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

    pub fn for_each_count_per_value(
        &self,
        mut f: impl FnMut(&N, usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.value_to_points
            .iter()
            .try_for_each(|(k, entry)| f(k.borrow(), entry.count as usize))
    }

    pub fn for_each_value_map(
        &self,
        mut f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.value_to_points
            .iter()
            .try_for_each(|(k, entry)| f(k.borrow(), &mut self.get_entry_iterator(entry)))
    }

    pub fn get_iterator(&self, value: &N) -> IdIter<'_> {
        if let Some(entry) = self.value_to_points.get(value) {
            Box::new(self.get_entry_iterator(entry))
        } else {
            Box::new(iter::empty::<PointOffsetType>())
        }
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

    pub fn for_each_value(
        &self,
        mut f: impl FnMut(&N) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.value_to_points.keys().try_for_each(|v| f(v.borrow()))
    }

    pub fn storage_type(&self) -> StorageType {
        match &self.storage {
            Storage::Mmap(index) => StorageType::Mmap {
                is_on_disk: index.is_on_disk(),
            },
        }
    }

    /// Approximate RAM usage in bytes (cached at construction).
    pub fn ram_usage_bytes(&self) -> usize {
        self.cached_ram_usage_bytes
    }

    fn compute_ram_usage_bytes(&self) -> usize {
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
