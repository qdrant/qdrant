use std::borrow::{Borrow, Cow};
use std::hash::{BuildHasher, Hash};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;
use indexmap::IndexSet;

use super::key::MapIndexKey;
use super::{IdIter, MapIndex};
use crate::common::operation_error::OperationResult;
use crate::index::field_index::CardinalityEstimation;
use crate::index::field_index::stat_tools::number_of_selected_points;
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::telemetry::PayloadIndexTelemetry;

/// Read-only operations supported by every map-index storage variant
/// ([`super::mutable_map_index::MutableMapIndex`],
/// [`super::immutable_map_index::ImmutableMapIndex`],
/// [`super::mmap_map_index::MmapMapIndex`]).
///
/// Signatures are unified across variants so the enum-level dispatcher in
/// [`MapIndex`] can call them generically. Variants that don't need
/// `hw_counter` (`Mutable` / `Immutable`) accept and ignore it; the mmap
/// variant uses it to track payload-index IO.
pub(super) trait MapIndexRead<N: MapIndexKey + ?Sized> {
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&N) -> bool,
    ) -> bool;

    fn get_values<'a>(
        &'a self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<impl Iterator<Item = Cow<'a, N>> + 'a>
    where
        N: 'a;

    fn values_count(&self, idx: PointOffsetType) -> Option<usize>;

    fn get_indexed_points(&self) -> usize;

    fn get_values_count(&self) -> usize;

    fn get_unique_values_count(&self) -> usize;

    fn get_count_for_value(&self, value: &N, hw_counter: &HardwareCounterCell) -> Option<usize>;

    fn get_iterator(&self, value: &N, hw_counter: &HardwareCounterCell) -> IdIter<'_>;

    fn for_each_value(&self, f: impl FnMut(&N) -> OperationResult<()>) -> OperationResult<()>;

    /// Iterate `(value, count)` pairs.
    ///
    /// `deferred_internal_id` (mutable / mmap only) restricts the count to
    /// point IDs strictly less than the given value. The immutable variant
    /// does not support deferred filtering and asserts the argument is `None`.
    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(&N, usize) -> OperationResult<()>,
    ) -> OperationResult<()>;

    fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()>;

    fn storage_type(&self) -> StorageType;

    fn ram_usage_bytes(&self) -> usize;
}

impl<N: MapIndexKey + ?Sized> MapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&N) -> bool,
    ) -> bool {
        match self {
            MapIndex::Mutable(index) => index.check_values_any(idx, hw_counter, check_fn),
            MapIndex::Immutable(index) => index.check_values_any(idx, hw_counter, check_fn),
            MapIndex::Mmap(index) => index.check_values_any(idx, hw_counter, check_fn),
        }
    }

    pub fn get_values(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = Cow<'_, N>> + '_>> {
        match self {
            MapIndex::Mutable(index) => Some(Box::new(index.get_values(idx, hw_counter)?)),
            MapIndex::Immutable(index) => Some(Box::new(index.get_values(idx, hw_counter)?)),
            MapIndex::Mmap(index) => Some(Box::new(index.get_values(idx, hw_counter)?)),
        }
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        match self {
            MapIndex::Mutable(index) => index.values_count(idx).unwrap_or_default(),
            MapIndex::Immutable(index) => index.values_count(idx).unwrap_or_default(),
            MapIndex::Mmap(index) => index.values_count(idx).unwrap_or_default(),
        }
    }

    pub(crate) fn get_indexed_points(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_indexed_points(),
            MapIndex::Immutable(index) => index.get_indexed_points(),
            MapIndex::Mmap(index) => index.get_indexed_points(),
        }
    }

    fn get_values_count(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_values_count(),
            MapIndex::Immutable(index) => index.get_values_count(),
            MapIndex::Mmap(index) => index.get_values_count(),
        }
    }

    pub fn get_unique_values_count(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_unique_values_count(),
            MapIndex::Immutable(index) => index.get_unique_values_count(),
            MapIndex::Mmap(index) => index.get_unique_values_count(),
        }
    }

    pub(crate) fn get_count_for_value(
        &self,
        value: &N,
        hw_counter: &HardwareCounterCell,
    ) -> Option<usize> {
        match self {
            MapIndex::Mutable(index) => index.get_count_for_value(value, hw_counter),
            MapIndex::Immutable(index) => index.get_count_for_value(value, hw_counter),
            MapIndex::Mmap(index) => index.get_count_for_value(value, hw_counter),
        }
    }

    pub(crate) fn get_iterator(&self, value: &N, hw_counter: &HardwareCounterCell) -> IdIter<'_> {
        match self {
            MapIndex::Mutable(index) => index.get_iterator(value, hw_counter),
            MapIndex::Immutable(index) => index.get_iterator(value, hw_counter),
            MapIndex::Mmap(index) => index.get_iterator(value, hw_counter),
        }
    }

    pub fn for_each_value(&self, f: impl FnMut(&N) -> OperationResult<()>) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.for_each_value(f),
            MapIndex::Immutable(index) => index.for_each_value(f),
            MapIndex::Mmap(index) => index.for_each_value(f),
        }
    }

    pub fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(&N, usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        // The immutable variant does not support deferred filtering — it
        // asserts the argument is `None`. Two reasons we don't implement it:
        //  - We don't have both deferred points and an immutable index.
        //  - It is not trivial (nor performant) to implement correct filtering
        //    for this index variant as it doesn't work well in combination
        //    with the way it handles deletions.
        match self {
            MapIndex::Mutable(index) => index.for_each_count_per_value(deferred_internal_id, f),
            MapIndex::Immutable(index) => index.for_each_count_per_value(deferred_internal_id, f),
            MapIndex::Mmap(index) => index.for_each_count_per_value(deferred_internal_id, f),
        }
    }

    pub fn for_each_value_map(
        &self,
        hw_cell: &HardwareCounterCell,
        f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.for_each_value_map(hw_cell, f),
            MapIndex::Immutable(index) => index.for_each_value_map(hw_cell, f),
            MapIndex::Mmap(index) => index.for_each_value_map(hw_cell, f),
        }
    }

    pub(crate) fn match_cardinality(
        &self,
        value: &N,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        let values_count = self.get_count_for_value(value, hw_counter).unwrap_or(0);

        CardinalityEstimation::exact(values_count)
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.get_indexed_points(),
            points_values_count: self.get_values_count(),
            histogram_bucket_size: None,
            index_type: match self {
                MapIndex::Mutable(_) => "mutable_map",
                MapIndex::Immutable(_) => "immutable_map",
                MapIndex::Mmap(_) => "mmap_map",
            },
        }
    }

    pub fn values_is_empty(&self, idx: PointOffsetType) -> bool {
        self.values_count(idx) == 0
    }

    /// Estimates cardinality for `except` clause
    ///
    /// # Arguments
    ///
    /// * 'excluded' - values, which are not considered as matching
    ///
    /// # Returns
    ///
    /// * `CardinalityEstimation` - estimation of cardinality
    pub(crate) fn except_cardinality<'a>(
        &'a self,
        excluded: impl Iterator<Item = &'a N>,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        // Minimal case: we exclude as many points as possible.
        // In this case, excluded points do not have any other values except excluded ones.
        // So the first step - we estimate how many other points is needed to fit unused values.

        // Example:
        // Values: 20, 20
        // Unique values: 5
        // Total points: 100
        // Total values: 110
        // total_excluded_value_count = 40
        // non_excluded_values_count = 110 - 40 = 70
        // max_values_per_point = 5 - 2 = 3
        // min_not_excluded_by_values = 70 / 3 = 24
        // min = max(24, 100 - 40) = 60
        // exp = ...
        // max = min(20, 70) = 20

        // Values: 60, 60
        // Unique values: 5
        // Total points: 100
        // Total values: 200
        // total_excluded_value_count = 120
        // non_excluded_values_count = 200 - 120 = 80
        // max_values_per_point = 5 - 2 = 3
        // min_not_excluded_by_values = 80 / 3 = 27
        // min = max(27, 100 - 120) = 27
        // exp = ...
        // max = min(60, 80) = 60

        // Values: 60, 60, 60
        // Unique values: 5
        // Total points: 100
        // Total values: 200
        // total_excluded_value_count = 180
        // non_excluded_values_count = 200 - 180 = 20
        // max_values_per_point = 5 - 3 = 2
        // min_not_excluded_by_values = 20 / 2 = 10
        // min = max(10, 100 - 180) = 10
        // exp = ...
        // max = min(60, 20) = 20

        let excluded_value_counts: Vec<_> = excluded
            .map(|val| {
                self.get_count_for_value(val.borrow(), hw_counter)
                    .unwrap_or(0)
            })
            .collect();
        let total_excluded_value_count: usize = excluded_value_counts.iter().sum();

        debug_assert!(total_excluded_value_count <= self.get_values_count());

        let non_excluded_values_count = self
            .get_values_count()
            .saturating_sub(total_excluded_value_count);
        let max_values_per_point = self
            .get_unique_values_count()
            .saturating_sub(excluded_value_counts.len());

        if max_values_per_point == 0 {
            debug_assert_eq!(non_excluded_values_count, 0);
            return CardinalityEstimation::exact(0);
        }

        let min_not_excluded_by_values = non_excluded_values_count.div_ceil(max_values_per_point);

        let min = min_not_excluded_by_values.max(
            self.get_indexed_points()
                .saturating_sub(total_excluded_value_count),
        );

        let max_excluded_value_count = excluded_value_counts.iter().max().copied().unwrap_or(0);

        let max = self
            .get_indexed_points()
            .saturating_sub(max_excluded_value_count)
            .min(non_excluded_values_count);

        let exp = number_of_selected_points(self.get_indexed_points(), non_excluded_values_count)
            .max(min)
            .min(max);

        CardinalityEstimation {
            primary_clauses: vec![],
            min,
            exp,
            max,
        }
    }

    pub(crate) fn except_set<'a, K, A>(
        &'a self,
        excluded: &'a IndexSet<K, A>,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>>
    where
        A: BuildHasher,
        K: Borrow<N> + Hash + Eq,
    {
        let mut points = IndexSet::new();
        self.for_each_value(|key| {
            if !excluded.contains(key.borrow()) {
                self.get_iterator(key.borrow(), hw_counter).for_each(|p| {
                    points.insert(p);
                });
            }
            Ok(())
        })?;
        Ok(Box::new(points.into_iter()))
    }

    /// Approximate RAM usage in bytes for in-memory structures.
    pub fn ram_usage_bytes(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.ram_usage_bytes(),
            MapIndex::Immutable(index) => index.ram_usage_bytes(),
            MapIndex::Mmap(index) => index.ram_usage_bytes(),
        }
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            MapIndex::Mutable(_) => false,
            MapIndex::Immutable(_) => false,
            MapIndex::Mmap(index) => index.is_on_disk(),
        }
    }

    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            Self::Mutable(_) => IndexMutability::Mutable,
            Self::Immutable(_) => IndexMutability::Immutable,
            Self::Mmap(_) => IndexMutability::Immutable,
        }
    }

    pub fn get_storage_type(&self) -> StorageType {
        match self {
            Self::Mutable(index) => index.storage_type(),
            Self::Immutable(index) => index.storage_type(),
            Self::Mmap(index) => index.storage_type(),
        }
    }
}
