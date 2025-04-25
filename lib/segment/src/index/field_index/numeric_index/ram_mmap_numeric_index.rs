use std::ops::Bound;
use std::path::PathBuf;

use common::types::PointOffsetType;

use super::Encodable;
use super::immutable_numeric_index::NumericKeySortedVec;
use super::mmap_numeric_index::MmapNumericIndex;
use super::mutable_numeric_index::InMemoryNumericIndex;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::histogram::{Histogram, Numericable, Point};
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;
use crate::index::field_index::mmap_point_to_values::MmapValue;

pub struct RamMmapNumericIndex<T: Encodable + Numericable + Default + MmapValue> {
    map: NumericKeySortedVec<T>,
    histogram: Histogram<T>,
    points_count: usize,
    max_values_per_point: usize,
    point_to_values: ImmutablePointToValues<T>,
    // Backing storage on top of mmap, used to persist deletions
    storage: MmapNumericIndex<T>,
}

impl<T: Encodable + Numericable + Default + MmapValue> RamMmapNumericIndex<T> {
    /// Construct in-memroy index from given mmap index
    ///
    /// # Warning
    ///
    /// Expensive because this reads the full mmap index.
    pub(super) fn from_mmap(storage: MmapNumericIndex<T>) -> Self {
        let InMemoryNumericIndex {
            map,
            histogram,
            points_count,
            max_values_per_point,
            point_to_values,
        } = InMemoryNumericIndex::from_mmap(&storage);

        // Index is now loaded into memory, clear cache of backing mmap storage
        // TODO: keep this?
        if let Err(err) = storage.clear_cache() {
            log::warn!("Failed to clear mmap cache of ram mmap numeric index: {err}");
        }

        Self {
            map: NumericKeySortedVec::from_btree_set(map),
            histogram,
            points_count,
            max_values_per_point,
            point_to_values: ImmutablePointToValues::new(point_to_values),
            storage,
        }
    }

    pub(super) fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&T) -> bool,
    ) -> bool {
        self.point_to_values.check_values_any(idx, |v| check_fn(v))
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        Some(Box::new(
            self.point_to_values
                .get_values(idx)
                .map(|iter| iter.copied())?,
        ))
    }

    pub fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.point_to_values.get_values_count(idx)
    }

    pub(super) fn total_unique_values_count(&self) -> usize {
        self.map.len()
    }

    pub(super) fn values_range_size(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> usize {
        let iterator = self.map.values_range(start_bound, end_bound);
        iterator.end_index - iterator.start_index
    }

    pub(super) fn values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> impl Iterator<Item = PointOffsetType> {
        self.map
            .values_range(start_bound, end_bound)
            .map(|Point { idx, .. }| idx)
    }

    pub(super) fn orderable_values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_ {
        self.map
            .values_range(start_bound, end_bound)
            .map(|Point { val, idx, .. }| (val, idx))
    }

    #[inline]
    pub fn clear(self) -> OperationResult<()> {
        let Self {
            storage,
            // In-memory structures don't need to be cleared
            map: _,
            histogram: _,
            points_count: _,
            max_values_per_point: _,
            point_to_values: _,
        } = self;
        storage.clear()
    }

    #[inline]
    pub fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    #[inline]
    pub fn flusher(&self) -> Flusher {
        self.storage.flusher()
    }

    pub(super) fn remove_point(&mut self, idx: PointOffsetType) {
        if let Some(removed_values) = self.point_to_values.get_values(idx) {
            let mut removed_count = 0;
            for value in removed_values {
                let key = Point::new(*value, idx);
                Self::remove_from_map(&mut self.map, &mut self.histogram, &key);

                // update storage
                self.storage.remove_point(idx);

                removed_count += 1;
            }
            if removed_count > 0 {
                self.points_count -= 1;
            }
        }
        self.point_to_values.remove_point(idx);
    }

    pub(super) fn get_histogram(&self) -> &Histogram<T> {
        &self.histogram
    }

    pub(super) fn get_points_count(&self) -> usize {
        self.points_count
    }

    pub(super) fn get_max_values_per_point(&self) -> usize {
        self.max_values_per_point
    }

    fn remove_from_map(
        map: &mut NumericKeySortedVec<T>,
        histogram: &mut Histogram<T>,
        key: &Point<T>,
    ) {
        if map.remove(key) {
            histogram.remove(
                key,
                |x| Self::get_histogram_left_neighbor(map, x),
                |x| Self::get_histogram_right_neighbor(map, x),
            );
        }
    }

    fn get_histogram_left_neighbor(
        map: &NumericKeySortedVec<T>,
        point: &Point<T>,
    ) -> Option<Point<T>> {
        map.values_range(Bound::Unbounded, Bound::Excluded(point.clone()))
            .next_back()
    }

    fn get_histogram_right_neighbor(
        map: &NumericKeySortedVec<T>,
        point: &Point<T>,
    ) -> Option<Point<T>> {
        map.values_range(Bound::Excluded(point.clone()), Bound::Unbounded)
            .next()
    }

    /// Drop disk cache of backing mmap storage
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache()
    }
}
