use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::iter;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use super::mutable_map_index::MutableMapIndex;
use super::MapIndex;
use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;

pub struct ImmutableMapIndex<N: Hash + Eq + Clone + Display + FromStr + Default> {
    value_to_points: HashMap<N, Range<u32>>,
    value_to_points_container: Vec<PointOffsetType>,
    point_to_values: ImmutablePointToValues<N>,
    /// Amount of point which have at least one indexed payload value
    indexed_points: usize,
    values_count: usize,
    db_wrapper: DatabaseColumnWrapper,
}

impl<N: Hash + Eq + Clone + Display + FromStr + Default> ImmutableMapIndex<N> {
    pub fn new(db: Arc<RwLock<DB>>, field_name: &str) -> Self {
        let store_cf_name = MapIndex::<N>::storage_cf_name(field_name);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
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
        value_to_points: &mut HashMap<N, Range<u32>>,
        value_to_points_container: &'a mut Vec<PointOffsetType>,
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
    fn shrink_value_range(value_to_points: &mut HashMap<N, Range<u32>>, value: &N) -> bool {
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
        value_to_points: &mut HashMap<N, Range<u32>>,
        value_to_points_container: &mut Vec<PointOffsetType>,
        value: &N,
        idx: PointOffsetType,
    ) {
        let values = if let Some(values) =
            Self::get_mut_point_ids_slice(value_to_points, value_to_points_container, value)
        {
            values
        } else {
            debug_assert!(false, "value {} not found in value_to_points", value);
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
            for value in removed_values {
                Self::remove_idx_from_value_list(
                    &mut self.value_to_points,
                    &mut self.value_to_points_container,
                    value,
                    idx,
                );
                // update db
                let key = MapIndex::encode_db_record(value, idx);
                self.db_wrapper.remove(key)?;
            }
        }
        self.point_to_values.remove_point(idx);
        Ok(())
    }

    pub fn get_db_wrapper(&self) -> &DatabaseColumnWrapper {
        &self.db_wrapper
    }

    pub fn load_from_db(&mut self) -> OperationResult<bool> {
        // To avoid code duplication, use `MutableMapIndex` to load data from db
        // and convert to immutable state

        let mut mutable = MutableMapIndex {
            map: Default::default(),
            point_to_values: Vec::new(),
            indexed_points: 0,
            values_count: 0,
            db_wrapper: self.db_wrapper.clone(),
        };
        let result = mutable.load_from_db()?;
        let MutableMapIndex {
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

    pub fn get_values(&self, idx: PointOffsetType) -> Option<&[N]> {
        self.point_to_values.get_values(idx)
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

    pub fn get_points_with_value_count<Q>(&self, value: &Q) -> Option<usize>
    where
        Q: ?Sized,
        N: std::borrow::Borrow<Q>,
        Q: Hash + Eq,
    {
        self.value_to_points.get(value).map(|p| p.len())
    }

    pub fn get_iterator<Q>(&self, value: &Q) -> Box<dyn Iterator<Item = PointOffsetType> + '_>
    where
        Q: ?Sized,
        N: std::borrow::Borrow<Q>,
        Q: Hash + Eq,
    {
        if let Some(range) = self.value_to_points.get(value) {
            let range = range.start as usize..range.end as usize;
            Box::new(self.value_to_points_container[range].iter().cloned())
        } else {
            Box::new(iter::empty::<PointOffsetType>())
        }
    }

    pub fn get_values_iterator(&self) -> Box<dyn Iterator<Item = &N> + '_> {
        Box::new(self.value_to_points.keys())
    }
}
