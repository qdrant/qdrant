use std::borrow::Borrow as _;
use std::collections::HashMap;
use std::iter;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use bitvec::vec::BitVec;
use common::mmap_hashmap::Key;
use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use super::mmap_map_index::MmapMapIndex;
use super::mutable_map_index::MutableMapIndex;
use super::{IdIter, IdRefIter, MapIndex, MapIndexKey};
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;
use crate::index::field_index::mmap_point_to_values::MmapValue;

pub struct ImmutableMapIndex<N: MapIndexKey + Key + ?Sized> {
    value_to_points: HashMap<N::Owned, ContainerSegment>,
    /// Container holding a slice of point IDs per value. `value_to_point` holds the range per value.
    /// Each slice MUST be sorted so that we can binary search over it.
    value_to_points_container: Vec<PointOffsetType>,
    deleted_value_to_points_container: BitVec,
    point_to_values: ImmutablePointToValues<N::Owned>,
    /// Amount of point which have at least one indexed payload value
    indexed_points: usize,
    values_count: usize,
    // Backing storage, source of state, persists deletions
    storage: Storage<N>,
}

enum Storage<N: MapIndexKey + Key + ?Sized> {
    RocksDb(DatabaseColumnScheduledDeleteWrapper),
    Mmap(Box<MmapMapIndex<N>>),
}

pub(super) struct ContainerSegment {
    /// Range in the container which holds point IDs for the value.
    pub(super) range: Range<u32>,
    /// Number of available point IDs in the range, excludes number of deleted points.
    pub(super) count: u32,
}

impl<N: MapIndexKey + ?Sized> ImmutableMapIndex<N> {
    /// Immutable numeric index from RocksDB storage
    pub fn new_rocksdb(db: Arc<RwLock<DB>>, field_name: &str) -> Self {
        let store_cf_name = MapIndex::<N>::storage_cf_name(field_name);
        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            db,
            &store_cf_name,
        ));
        Self {
            value_to_points: Default::default(),
            value_to_points_container: Default::default(),
            deleted_value_to_points_container: Default::default(),
            point_to_values: Default::default(),
            indexed_points: 0,
            values_count: 0,
            storage: Storage::RocksDb(db_wrapper),
        }
    }

    /// Immutable numeric index from mmap storage
    pub(super) fn new_mmap(index: MmapMapIndex<N>) -> Self {
        // Construct intermediate values to points map from backing storage
        let map = index
            .value_to_points
            .iter()
            .map(|(value, ids)| {
                (
                    value,
                    ids.iter()
                        .copied()
                        .filter(|idx| {
                            let is_deleted = index.deleted.get(*idx as usize).unwrap_or(false);
                            !is_deleted
                        })
                        .collect(),
                )
            })
            .collect::<HashMap<_, Vec<PointOffsetType>>>();

        // Create points to values mapping
        let mut indexed_points = 0;
        let mut values_count = 0;
        let mut point_to_values: Vec<Vec<N::Owned>> = vec![];
        for (&value, ids) in &map {
            for &idx in ids {
                if point_to_values.len() <= idx as usize {
                    point_to_values.resize_with(idx as usize + 1, Vec::new)
                }
                let point_values = &mut point_to_values[idx as usize];

                if point_values.is_empty() {
                    indexed_points += 1;
                }
                values_count += 1;

                point_values.push(value.to_owned());
            }
        }

        let mut value_to_points: HashMap<N::Owned, ContainerSegment> = HashMap::new();
        let mut value_to_points_container: Vec<PointOffsetType> = Vec::with_capacity(values_count);
        let deleted_value_to_points_container = BitVec::new();

        // Create flattened values-to-points mapping
        for (value, points) in map {
            let points = points.into_iter().collect::<Vec<_>>();
            let container_len = value_to_points_container.len() as u32;
            let range = container_len..container_len + points.len() as u32;
            value_to_points.insert(
                value.to_owned(),
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
            if let Some((slice, _offset)) = Self::get_mut_point_ids_slice(
                &value_to_points,
                &mut value_to_points_container,
                value.borrow(),
            ) {
                slice.sort_unstable();
            } else {
                debug_assert!(
                    false,
                    "value {} not found in value_to_points",
                    value.borrow(),
                );
            }
        }

        debug_assert_eq!(indexed_points, index.get_indexed_points());
        // debug_assert_eq!(
        //     values_count,
        //     storage
        //         .get_values_count()
        //         .saturating_sub(storage.deleted_count)
        // );

        // Index is now loaded into memory, clear cache of backing mmap storage
        if let Err(err) = index.clear_cache() {
            log::warn!("Failed to clear mmap cache of ram mmap map index: {err}");
        }

        Self {
            value_to_points,
            value_to_points_container,
            deleted_value_to_points_container,
            point_to_values: ImmutablePointToValues::new(point_to_values),
            indexed_points,
            values_count,
            storage: Storage::Mmap(Box::new(index)),
        }
    }

    /// Load storage
    ///
    /// Loads RocksDB storage. Does nothing when using mmap based storage.
    pub fn load_from_db(&mut self) -> OperationResult<bool> {
        let db_wrapper = match &self.storage {
            Storage::RocksDb(db_wrapper) => Some(db_wrapper.clone()),
            Storage::Mmap(_) => None,
        };
        let Some(db_wrapper) = db_wrapper else {
            return Ok(true);
        };

        // To avoid code duplication, use `MutableMapIndex` to load data from db
        // and convert to immutable state

        let mut mutable = MutableMapIndex::<N> {
            map: Default::default(),
            point_to_values: Vec::new(),
            indexed_points: 0,
            values_count: 0,
            db_wrapper: db_wrapper.clone(),
        };
        let result = mutable.load_from_db()?;
        let MutableMapIndex::<N> {
            map,
            point_to_values,
            indexed_points,
            values_count,
            ..
        } = mutable;

        self.indexed_points = indexed_points;
        self.values_count = values_count;
        self.value_to_points.clear();
        self.value_to_points_container.clear();
        self.value_to_points_container.reserve_exact(values_count);
        self.deleted_value_to_points_container.clear();

        // flatten values-to-points map
        for (value, points) in map {
            let points = points.into_iter().collect::<Vec<_>>();
            let container_len = self.value_to_points_container.len() as u32;
            let range = container_len..container_len + points.len() as u32;
            self.value_to_points.insert(
                value,
                ContainerSegment {
                    count: range.len() as u32,
                    range,
                },
            );
            self.value_to_points_container.extend(points);
        }

        self.value_to_points.shrink_to_fit();

        // Sort IDs in each slice of points
        // This is very important because we binary search
        for value in self.value_to_points.keys() {
            if let Some((slice, _offset)) = Self::get_mut_point_ids_slice(
                &self.value_to_points,
                &mut self.value_to_points_container,
                value.borrow(),
            ) {
                slice.sort_unstable();
            } else {
                debug_assert!(
                    false,
                    "value {} not found in value_to_points",
                    value.borrow(),
                );
            }
        }

        self.point_to_values = ImmutablePointToValues::new(point_to_values);

        Ok(result)
    }

    /// Return mutable slice of a container which holds point_ids for given value.
    ///
    /// The returned slice is sorted and does contain deleted values.
    /// The returned offset is the start of the range in the container.
    fn get_mut_point_ids_slice<'a>(
        value_to_points: &HashMap<N::Owned, ContainerSegment>,
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
        value_to_points: &mut HashMap<N::Owned, ContainerSegment>,
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
        value_to_points: &mut HashMap<N::Owned, ContainerSegment>,
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
                    Storage::RocksDb(ref db_wrapper) => {
                        let key = MapIndex::encode_db_record(value.borrow(), idx);
                        db_wrapper.remove(key)?;
                    }
                    Storage::Mmap(ref mut index) => {
                        index.remove_point(idx);
                    }
                }
                removed_values_count += 1;
            }

            if removed_values_count > 0 {
                self.indexed_points -= 1;
            }
            self.values_count = self
                .values_count
                .checked_sub(removed_values_count)
                .unwrap_or_default();
        }
        self.point_to_values.remove_point(idx);
        Ok(())
    }

    #[cfg(test)]
    pub fn get_db_wrapper(&self) -> Option<&DatabaseColumnScheduledDeleteWrapper> {
        match self.storage {
            Storage::RocksDb(ref db_wrapper) => Some(db_wrapper),
            Storage::Mmap(_) => None,
        }
    }

    #[inline]
    pub(super) fn clear(&mut self) -> OperationResult<()> {
        match self.storage {
            Storage::RocksDb(ref db_wrapper) => db_wrapper.recreate_column_family(),
            Storage::Mmap(ref mut index) => index.clear(),
        }
    }

    /// Clear cache
    ///
    /// Only clears cache of mmap storage if used. Does not clear in-memory representation of
    /// index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self.storage {
            Storage::RocksDb(_) => Ok(()),
            Storage::Mmap(ref index) => index.clear_cache(),
        }
    }

    #[inline]
    pub(super) fn files(&self) -> Vec<PathBuf> {
        match self.storage {
            Storage::RocksDb(_) => vec![],
            Storage::Mmap(ref index) => index.files(),
        }
    }

    #[inline]
    pub(super) fn flusher(&self) -> Flusher {
        match self.storage {
            Storage::RocksDb(ref db_wrapper) => db_wrapper.flusher(),
            Storage::Mmap(ref index) => index.flusher(),
        }
    }

    pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&N) -> bool) -> bool {
        let mut hw_count_val = 0;

        let res = self.point_to_values.check_values_any(idx, |v| {
            let v = v.borrow();
            hw_count_val += <N as MmapValue>::mmapped_size(v.as_referenced());
            check_fn(v)
        });

        res
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<impl Iterator<Item = &N> + '_> {
        Some(self.point_to_values.get_values(idx)?.map(|v| v.borrow()))
    }

    pub fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.point_to_values.get_values_count(idx)
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

    pub fn iter_counts_per_value(&self) -> impl Iterator<Item = (&N, usize)> + '_ {
        self.value_to_points
            .iter()
            .map(|(k, entry)| (k.borrow(), entry.count as usize))
    }

    pub fn iter_values_map(&self) -> impl Iterator<Item = (&N, IdIter)> {
        self.value_to_points.keys().map(move |k| {
            (
                k.borrow(),
                Box::new(self.get_iterator(k.borrow()).copied()) as IdIter,
            )
        })
    }

    pub fn get_iterator(&self, value: &N) -> IdRefIter<'_> {
        if let Some(entry) = self.value_to_points.get(value) {
            let range = entry.range.start as usize..entry.range.end as usize;

            let deleted_flags = self
                .deleted_value_to_points_container
                .iter()
                .by_vals()
                .skip(range.start)
                .chain(std::iter::repeat(false));

            let values = self.value_to_points_container[range]
                .iter()
                .zip(deleted_flags)
                .filter(|(_, is_deleted)| !is_deleted)
                .map(|(idx, _)| idx);

            Box::new(values)
        } else {
            Box::new(iter::empty::<&PointOffsetType>())
        }
    }

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = &N> + '_> {
        Box::new(self.value_to_points.keys().map(|v| v.borrow()))
    }
}
