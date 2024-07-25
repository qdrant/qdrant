use std::borrow::Borrow as _;
use std::collections::HashMap;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use super::mutable_map_index::MutableMapIndex;
use super::{IdRefIter, MapIndex, MapIndexKey};
use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;

pub struct ImmutableMapIndex<N: MapIndexKey + ?Sized> {
    value_to_points: HashMap<N::Owned, Range<u32>>,
    value_to_points_container: Vec<PointOffsetType>,
    point_to_values: ImmutablePointToValues<N::Owned>,
    /// Amount of point which have at least one indexed payload value
    indexed_points: usize,
    values_count: usize,
    db_wrapper: DatabaseColumnScheduledDeleteWrapper,
}

impl<N: MapIndexKey + ?Sized> ImmutableMapIndex<N> {
    pub fn new(db: Arc<RwLock<DB>>, field_name: &str) -> Self {
        let store_cf_name = MapIndex::<N>::storage_cf_name(field_name);
        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            db,
            &store_cf_name,
        ));
        Self {
            value_to_points: Default::default(),
            value_to_points_container: Default::default(),
            point_to_values: Default::default(),
            indexed_points: 0,
            values_count: 0,
            db_wrapper,
        }
    }

    /// Return mutable slice of a container which holds point_ids for given value.
    fn get_mut_point_ids_slice<'a>(
        value_to_points: &mut HashMap<N::Owned, Range<u32>>,
        value_to_points_container: &'a mut [PointOffsetType],
        value: &N,
    ) -> Option<&'a mut [PointOffsetType]> {
        match value_to_points.get(value) {
            Some(vals_range) if vals_range.start < vals_range.end => {
                let range = vals_range.start as usize..vals_range.end as usize;
                let vals = &mut value_to_points_container[range];
                Some(vals)
            }
            _ => None,
        }
    }

    /// Shrinks the range of values-to-points by one.
    /// Returns true if the last element was removed.
    fn shrink_value_range(value_to_points: &mut HashMap<N::Owned, Range<u32>>, value: &N) -> bool {
        if let Some(range) = value_to_points.get_mut(value) {
            range.end -= 1;
            return range.start == range.end; // true if the last element was removed
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
        value_to_points: &mut HashMap<N::Owned, Range<u32>>,
        value_to_points_container: &mut [PointOffsetType],
        value: &N,
        idx: PointOffsetType,
    ) {
        let Some(values) =
            Self::get_mut_point_ids_slice(value_to_points, value_to_points_container, value)
        else {
            debug_assert!(false, "value {value} not found in value_to_points");
            return;
        };

        // Finds the index of `idx` in values-to-points map and swaps it with the last element.
        // So that removed element is out of the shrank range.
        if let Some(pos) = values.iter().position(|&x| x == idx) {
            // remove `idx` from values-to-points map by swapping it with the last element
            values.swap(pos, values.len() - 1);
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
                    value.borrow(),
                    idx,
                );
                // update db
                let key = MapIndex::encode_db_record(value.borrow(), idx);
                self.db_wrapper.remove(key)?;
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

    pub fn get_db_wrapper(&self) -> &DatabaseColumnScheduledDeleteWrapper {
        &self.db_wrapper
    }

    pub fn load_from_db(&mut self) -> OperationResult<bool> {
        // To avoid code duplication, use `MutableMapIndex` to load data from db
        // and convert to immutable state

        let mut mutable = MutableMapIndex::<N> {
            map: Default::default(),
            point_to_values: Vec::new(),
            indexed_points: 0,
            values_count: 0,
            db_wrapper: self.db_wrapper.clone(),
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

        // flatten values-to-points map
        for (value, points) in map {
            let points = points.into_iter().collect::<Vec<_>>();
            let container_len = self.value_to_points_container.len() as u32;
            let range = container_len..container_len + points.len() as u32;
            self.value_to_points.insert(value, range.clone());
            self.value_to_points_container.extend(points);
        }

        self.point_to_values = ImmutablePointToValues::new(point_to_values);

        Ok(result)
    }

    pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&N) -> bool) -> bool {
        self.point_to_values
            .check_values_any(idx, |v| check_fn(v.borrow()))
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
        self.value_to_points.get(value).map(|p| p.len())
    }

    pub fn iter_counts_per_value(&self) -> impl Iterator<Item = (&N, usize)> + '_ {
        self.value_to_points
            .iter()
            .map(|(k, v)| (k.borrow(), v.len()))
    }

    pub fn iter_values_map(&self) -> impl Iterator<Item = (&N, IdRefIter<'_>)> + '_ {
        self.value_to_points.keys().map(|k| {
            (
                k.borrow(),
                Box::new(self.get_iterator(k.borrow()))
                    as Box<dyn Iterator<Item = &PointOffsetType>>,
            )
        })
    }

    pub fn get_iterator(&self, value: &N) -> IdRefIter<'_> {
        if let Some(range) = self.value_to_points.get(value) {
            let range = range.start as usize..range.end as usize;
            Box::new(self.value_to_points_container[range].iter())
        } else {
            Box::new(iter::empty::<&PointOffsetType>())
        }
    }

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = &N> + '_> {
        Box::new(self.value_to_points.keys().map(|v| v.borrow()))
    }
}
