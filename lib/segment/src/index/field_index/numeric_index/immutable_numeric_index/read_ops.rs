use std::ops::Bound;

use common::types::PointOffsetType;
use gridstore::Blob;

use super::super::Encodable;
use super::ImmutableNumericIndex;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::stored_point_to_values::StoredValue;
use crate::index::payload_config::StorageType;

impl<T: Encodable + Numericable + StoredValue + Default> ImmutableNumericIndex<T> {
    /// Approximate RAM usage in bytes for in-memory structures (cached at construction).
    pub fn ram_usage_bytes(&self) -> usize {
        self.cached_ram_usage_bytes
    }

    pub(super) fn compute_ram_usage_bytes(&self) -> usize {
        let Self {
            map,
            histogram,
            points_count: _,
            max_values_per_point: _,
            point_to_values,
            storage: _,
            cached_ram_usage_bytes: _,
        } = self;

        map.ram_usage_bytes() + histogram.ram_usage_bytes() + point_to_values.ram_usage_bytes()
    }
}

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default> ImmutableNumericIndex<T>
where
    Vec<T>: Blob,
{
    pub(in super::super) fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&T) -> bool,
    ) -> bool {
        self.point_to_values.check_values_any(idx, |v| check_fn(v))
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        let iter = self.point_to_values.get_values(idx)?;
        Some(Box::new(iter.copied()))
    }

    pub fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.point_to_values.get_values_count(idx)
    }

    pub(in super::super) fn total_unique_values_count(&self) -> usize {
        self.map.len()
    }

    pub(in super::super) fn values_range_size(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> usize {
        let iterator = self.map.values_range(start_bound, end_bound);
        iterator.end_index - iterator.start_index
    }

    pub(in super::super) fn values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> impl Iterator<Item = PointOffsetType> {
        self.map
            .values_range(start_bound, end_bound)
            .map(|Point { idx, .. }| idx)
    }

    pub(in super::super) fn orderable_values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_ {
        self.map
            .values_range(start_bound, end_bound)
            .map(|Point { val, idx, .. }| (val, idx))
    }

    pub(in super::super) fn get_histogram(&self) -> &Histogram<T> {
        &self.histogram
    }

    pub(in super::super) fn get_points_count(&self) -> usize {
        self.points_count
    }

    pub(in super::super) fn get_max_values_per_point(&self) -> usize {
        self.max_values_per_point
    }

    pub fn storage_type(&self) -> StorageType {
        StorageType::Mmap {
            is_on_disk: self.storage.is_on_disk(),
        }
    }
}
