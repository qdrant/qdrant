use std::ops::Bound;
use std::path::PathBuf;

use common::types::PointOffsetType;
use gridstore::Blob;

use super::super::Encodable;
use super::super::mutable_numeric_index::InMemoryNumericIndex;
use super::super::universal_numeric_index::UniversalNumericIndex;
use super::{ImmutableNumericIndex, NumericKeySortedVec};
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::stored_point_to_values::StoredValue;

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default> ImmutableNumericIndex<T>
where
    Vec<T>: Blob,
{
    /// Open and load immutable numeric index from mmap storage.
    ///
    /// NOTE: returns `Self` (infallible) while sibling
    /// `ImmutableMapIndex::open_mmap` / `ImmutableGeoMapIndex::open_mmap` /
    /// `ImmutableFullTextIndex::open_mmap` return `OperationResult<Self>`.
    /// Numeric's body has no fallible reads to propagate (`from_mmap` is
    /// infallible; `clear_cache` errors are warn-and-continue, matching the
    /// other variants).
    pub(in super::super) fn open_mmap(index: UniversalNumericIndex<T>) -> Self {
        // Load in-memory index from mmap storage
        let InMemoryNumericIndex {
            map,
            histogram,
            points_count,
            max_values_per_point,
            point_to_values,
        } = InMemoryNumericIndex::from_mmap(&index);

        // Index is now loaded into memory, clear cache of backing mmap storage
        if let Err(err) = index.clear_cache() {
            log::warn!("Failed to clear mmap cache of ram mmap numeric index: {err}");
        }

        let mut result = Self {
            map: NumericKeySortedVec::from_btree_set(map),
            histogram,
            points_count,
            max_values_per_point,
            point_to_values: ImmutablePointToValues::new(point_to_values),
            storage: Box::new(index),
            cached_ram_usage_bytes: 0,
        };
        result.cached_ram_usage_bytes = result.compute_ram_usage_bytes();
        result
    }

    #[inline]
    pub(in super::super) fn wipe(self) -> OperationResult<()> {
        self.storage.wipe()
    }

    /// Clear cache
    ///
    /// Only clears cache of mmap storage if used. Does not clear in-memory representation of
    /// index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache()
    }

    #[inline]
    pub(in super::super) fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    #[inline]
    pub(in super::super) fn immutable_files(&self) -> Vec<PathBuf> {
        self.storage.immutable_files()
    }

    #[inline]
    pub(in super::super) fn flusher(&self) -> Flusher {
        self.storage.flusher()
    }

    pub(in super::super) fn remove_point(&mut self, idx: PointOffsetType) {
        if let Some(removed_values) = self.point_to_values.get_values(idx) {
            let mut removed_count = 0;
            for value in removed_values {
                let key = Point::new(*value, idx);
                Self::remove_from_map(&mut self.map, &mut self.histogram, &key);

                // Update persisted storage
                self.storage.remove_point(idx);

                removed_count += 1;
            }
            if removed_count > 0 {
                self.points_count = self.points_count.saturating_sub(1);
            }
        }
        self.point_to_values.remove_point(idx);
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
        map.values_range(Bound::Unbounded, Bound::Excluded(*point))
            .next_back()
    }

    fn get_histogram_right_neighbor(
        map: &NumericKeySortedVec<T>,
        point: &Point<T>,
    ) -> Option<Point<T>> {
        map.values_range(Bound::Excluded(*point), Bound::Unbounded)
            .next()
    }
}
