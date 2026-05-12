use std::borrow::Borrow as _;
use std::collections::HashMap;
use std::path::PathBuf;

use bitvec::vec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;

use super::super::MapIndexKey;
use super::super::mmap_map_index::MmapMapIndex;
use super::super::read_ops::MapIndexRead;
use super::{ContainerSegment, ImmutableMapIndex, Storage};
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;

impl<N: MapIndexKey + ?Sized> ImmutableMapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    /// Open and load immutable map index from mmap storage
    pub(in super::super) fn open_mmap(index: MmapMapIndex<N>) -> OperationResult<Self> {
        let hw_counter = HardwareCounterCell::disposable(); // Internal operation

        let mut indexed_points = 0;
        let mut values_count = 0;
        let mut value_to_points = HashMap::new();

        // Create points to values mapping
        let mut point_to_values: Vec<Vec<<N as MapIndexKey>::Owned>> = vec![];
        // Create flattened values-to-points mapping. Skip values whose live
        // points are all deleted in the backing mmap (e.g., points the
        // id-tracker has deleted at runtime, applied at open time by
        // `MmapMapIndex::open`). This mirrors the runtime invariant in
        // `remove_idx_from_value_list`: `value_to_points` only ever contains
        // entries with `count > 0`.
        let mut value_to_points_container = Vec::with_capacity(index.get_values_count());
        index.for_each_value_map(&hw_counter, |value, ids| {
            let range_start = value_to_points_container.len() as u32;
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
                value_to_points_container.push(idx);
            }
            let range = range_start..value_to_points_container.len() as u32;
            if !range.is_empty() {
                value_to_points.insert(
                    MapIndexKey::to_owned(value),
                    ContainerSegment {
                        count: range.len() as u32,
                        range,
                    },
                );
            }
            Ok(())
        })?;
        let point_to_values = ImmutablePointToValues::new(point_to_values);
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
        Ok(result)
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
    pub(in super::super) fn wipe(self) -> OperationResult<()> {
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
    pub(in super::super) fn files(&self) -> Vec<PathBuf> {
        match self.storage {
            Storage::Mmap(ref index) => index.files(),
        }
    }

    #[inline]
    pub(in super::super) fn immutable_files(&self) -> Vec<PathBuf> {
        match &self.storage {
            Storage::Mmap(index) => index.immutable_files(),
        }
    }

    #[inline]
    pub(in super::super) fn flusher(&self) -> Flusher {
        match self.storage {
            Storage::Mmap(ref index) => index.flusher(),
        }
    }
}
