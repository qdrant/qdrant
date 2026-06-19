use std::borrow::{Borrow, Cow};
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;

use common::condition_checker::ConditionChecker;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use gridstore::Blob;
use indexmap::IndexSet;

use super::key::MapIndexKey;
use super::{IdIter, MapIndex};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::facets::FacetValue;
use crate::index::field_index::CardinalityEstimation;
use crate::index::field_index::stat_tools::number_of_selected_points;
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::index::query_optimization::optimized_filter::DynConditionChecker;
use crate::payload_storage::condition_checker::INDEXSET_ITER_THRESHOLD;
use crate::telemetry::PayloadIndexTelemetry;

/// Read-only operations supported by every map-index storage variant
/// ([`super::mutable_map_index::MutableMapIndex`],
/// [`super::immutable_map_index::ImmutableMapIndex`],
/// [`super::universal_map_index::UniversalMapIndex`]).
///
/// Signatures are unified across variants so the enum-level dispatcher in
/// [`MapIndex`] can call them generically. Variants that don't need
/// `hw_counter` (`Mutable` / `Immutable`) accept and ignore it; the
/// storage-backed `Universal` variant uses it to track payload-index IO.
pub trait MapIndexRead<'a, N: MapIndexKey + ?Sized + 'a>: Sized {
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&N) -> bool,
    ) -> OperationResult<bool>;

    fn get_values(
        &'a self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<impl Iterator<Item = Cow<'a, N>> + 'a>;

    fn values_count(&self, idx: PointOffsetType) -> Option<usize>;

    fn get_indexed_points(&self) -> usize;

    fn get_values_count(&self) -> usize;

    fn get_unique_values_count(&self) -> usize;

    fn get_count_for_value(&self, value: &N, hw_counter: &HardwareCounterCell) -> Option<usize>;

    fn get_iterator(&self, value: &N, hw_counter: &HardwareCounterCell) -> IdIter<'_>;

    fn for_each_value(&self, f: impl FnMut(&N) -> OperationResult<()>) -> OperationResult<()>;

    fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()>;

    fn storage_type(&self) -> StorageType;

    fn ram_usage_bytes(&self) -> usize;

    /// Per-variant telemetry tag (e.g. `"mutable_map"`, `"mmap_map"`,
    /// `"read_only_map"`). Used by the default [`Self::get_telemetry_data`]
    /// to fill the `index_type` field. Mirrors
    /// [`NullIndexRead::telemetry_index_type`][1].
    ///
    /// [1]: crate::index::field_index::null_index::NullIndexRead::telemetry_index_type
    fn telemetry_index_type(&self) -> &'static str;

    // Default-method helpers derived from the required methods above.
    //
    // Kept here (rather than as inherent methods on a single concrete type) so
    // that every `MapIndexRead<N>` impl — `MapIndex<N>`, `ReadOnlyMapIndex<N, S>`,
    // and the leaf variants — exposes them via the same call syntax.

    fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.get_indexed_points(),
            points_values_count: self.get_values_count(),
            histogram_bucket_size: None,
            index_type: self.telemetry_index_type(),
        }
    }

    fn values_is_empty(&self, idx: PointOffsetType) -> bool {
        self.values_count(idx).unwrap_or(0) == 0
    }

    /// Backs [`FacetIndex::for_values_map`]: yield each value's posting,
    /// skipping values whose variant doesn't match this index's key type.
    ///
    /// [`FacetIndex::for_values_map`]: crate::index::field_index::FacetIndex::for_values_map
    fn for_values_map(
        &self,
        values: impl Iterator<Item = FacetValue>,
        hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(FacetValue, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        for value in values {
            let Some(key) = N::from_facet_value(&value) else {
                continue;
            };
            let mut ids = self.get_iterator(key, hw_counter);
            f(value, &mut ids)?;
        }
        Ok(())
    }

    fn match_cardinality(
        &self,
        value: &N,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        let values_count = self.get_count_for_value(value, hw_counter).unwrap_or(0);
        CardinalityEstimation::exact(values_count)
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
    fn except_cardinality<'b>(
        &self,
        excluded: impl Iterator<Item = &'b N>,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation
    where
        N: 'b,
    {
        // Minimal case: we exclude as many points as possible.
        let excluded_value_counts: Vec<_> = excluded
            .map(|val| self.get_count_for_value(val, hw_counter).unwrap_or(0))
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

    fn except_set<K, A>(
        &'a self,
        excluded: &'a IndexSet<K, A>,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>>
    where
        A: BuildHasher,
        K: Borrow<N> + Hash + Eq,
    {
        let mut points = IndexSet::<PointOffsetType>::new();
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

    /// Condition checker for [`crate::types::Match::Value`].
    fn match_value_checker(
        &'a self,
        hw_counter: HardwareCounterCell,
        value: impl Borrow<N> + 'a,
    ) -> DynConditionChecker<'a> {
        Box::new(MapConditionChecker {
            index: self,
            hw_counter,
            predicate: move |v: &N| v == value.borrow(),
            _key: PhantomData,
        })
    }

    /// Condition checker for
    /// - [`crate::types::Match::Any`] (when `negate` is `false`),
    /// - [`crate::types::Match::Except`] (when `negate` is `true`).
    fn match_any_checker<K, A>(
        &'a self,
        hw_counter: HardwareCounterCell,
        list: IndexSet<K, A>,
        negate: bool,
    ) -> DynConditionChecker<'a>
    where
        A: BuildHasher + 'a,
        K: Borrow<N> + Hash + Eq + 'a,
    {
        if list.len() < INDEXSET_ITER_THRESHOLD {
            Box::new(MapConditionChecker {
                index: self,
                hw_counter,
                predicate: move |value: &N| list.iter().any(|e| e.borrow() == value) != negate,
                _key: PhantomData,
            })
        } else {
            Box::new(MapConditionChecker {
                index: self,
                hw_counter,
                predicate: move |value: &N| list.contains(value) != negate,
                _key: PhantomData,
            })
        }
    }
}

impl<'a, N: MapIndexKey + ?Sized + 'a> MapIndexRead<'a, N> for MapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&N) -> bool,
    ) -> OperationResult<bool> {
        match self {
            MapIndex::Mutable(index) => index.check_values_any(idx, hw_counter, check_fn),
            MapIndex::Immutable(index) => index.check_values_any(idx, hw_counter, check_fn),
            MapIndex::OnDisk(index) => index.check_values_any(idx, hw_counter, check_fn),
        }
    }

    fn get_values(
        &'a self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<impl Iterator<Item = Cow<'a, N>> + 'a> {
        let boxed: Box<dyn Iterator<Item = Cow<'a, N>> + 'a> = match self {
            MapIndex::Mutable(index) => Box::new(index.get_values(idx, hw_counter)?),
            MapIndex::Immutable(index) => Box::new(index.get_values(idx, hw_counter)?),
            MapIndex::OnDisk(index) => Box::new(index.get_values(idx, hw_counter)?),
        };
        Some(boxed)
    }

    fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        match self {
            MapIndex::Mutable(index) => index.values_count(idx),
            MapIndex::Immutable(index) => index.values_count(idx),
            MapIndex::OnDisk(index) => index.values_count(idx),
        }
    }

    fn get_indexed_points(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_indexed_points(),
            MapIndex::Immutable(index) => index.get_indexed_points(),
            MapIndex::OnDisk(index) => index.get_indexed_points(),
        }
    }

    fn get_values_count(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_values_count(),
            MapIndex::Immutable(index) => index.get_values_count(),
            MapIndex::OnDisk(index) => index.get_values_count(),
        }
    }

    fn get_unique_values_count(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_unique_values_count(),
            MapIndex::Immutable(index) => index.get_unique_values_count(),
            MapIndex::OnDisk(index) => index.get_unique_values_count(),
        }
    }

    fn get_count_for_value(&self, value: &N, hw_counter: &HardwareCounterCell) -> Option<usize> {
        match self {
            MapIndex::Mutable(index) => index.get_count_for_value(value, hw_counter),
            MapIndex::Immutable(index) => index.get_count_for_value(value, hw_counter),
            MapIndex::OnDisk(index) => index.get_count_for_value(value, hw_counter),
        }
    }

    fn get_iterator(&self, value: &N, hw_counter: &HardwareCounterCell) -> IdIter<'_> {
        match self {
            MapIndex::Mutable(index) => index.get_iterator(value, hw_counter),
            MapIndex::Immutable(index) => index.get_iterator(value, hw_counter),
            MapIndex::OnDisk(index) => index.get_iterator(value, hw_counter),
        }
    }

    fn for_each_value(&self, f: impl FnMut(&N) -> OperationResult<()>) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.for_each_value(f),
            MapIndex::Immutable(index) => index.for_each_value(f),
            MapIndex::OnDisk(index) => index.for_each_value(f),
        }
    }

    fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.for_each_value_map(hw_counter, f),
            MapIndex::Immutable(index) => index.for_each_value_map(hw_counter, f),
            MapIndex::OnDisk(index) => index.for_each_value_map(hw_counter, f),
        }
    }

    fn storage_type(&self) -> StorageType {
        match self {
            MapIndex::Mutable(index) => index.storage_type(),
            MapIndex::Immutable(index) => index.storage_type(),
            MapIndex::OnDisk(index) => index.storage_type(),
        }
    }

    fn ram_usage_bytes(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.ram_usage_bytes(),
            MapIndex::Immutable(index) => index.ram_usage_bytes(),
            MapIndex::OnDisk(index) => index.ram_usage_bytes(),
        }
    }

    fn telemetry_index_type(&self) -> &'static str {
        match self {
            MapIndex::Mutable(_) => "mutable_map",
            MapIndex::Immutable(_) => "immutable_map",
            MapIndex::OnDisk(_) => "mmap_map",
        }
    }
}

impl<N: MapIndexKey + ?Sized> MapIndex<N>
where
    Vec<<N as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: <Self as MapIndexRead<'_, N>>::get_indexed_points(self),
            points_values_count: <Self as MapIndexRead<'_, N>>::get_values_count(self),
            histogram_bucket_size: None,
            index_type: match self {
                MapIndex::Mutable(_) => "mutable_map",
                MapIndex::Immutable(_) => "immutable_map",
                MapIndex::OnDisk(_) => "mmap_map",
            },
        }
    }

    /// `usize`-returning convenience wrapper around
    /// [`MapIndexRead::values_count`] for callers outside this module who
    /// don't have the (`pub(super)`) trait in scope.
    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        <Self as MapIndexRead<'_, N>>::values_count(self, idx).unwrap_or(0)
    }

    /// `bool`-returning convenience wrapper around
    /// [`MapIndexRead::values_is_empty`]; see [`Self::values_count`] for why.
    pub fn values_is_empty(&self, idx: PointOffsetType) -> bool {
        <Self as MapIndexRead<'_, N>>::values_is_empty(self, idx)
    }

    /// Convenience wrapper around [`MapIndexRead::get_values`] that boxes the
    /// returned iterator into `dyn Iterator`. See [`Self::values_count`] for
    /// why the wrapper exists.
    pub fn get_values(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = Cow<'_, N>> + '_>> {
        let iter = <Self as MapIndexRead<'_, N>>::get_values(self, idx, hw_counter)?;
        Some(Box::new(iter))
    }

    /// Convenience wrapper around [`MapIndexRead::ram_usage_bytes`]; see
    /// [`Self::values_count`] for why.
    pub fn ram_usage_bytes(&self) -> usize {
        <Self as MapIndexRead<'_, N>>::ram_usage_bytes(self)
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            MapIndex::Mutable(_) => false,
            MapIndex::Immutable(_) => false,
            MapIndex::OnDisk(index) => index.is_on_disk(),
        }
    }

    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            Self::Mutable(_) => IndexMutability::Mutable,
            Self::Immutable(_) => IndexMutability::Immutable,
            Self::OnDisk(_) => IndexMutability::Immutable,
        }
    }

    pub fn get_storage_type(&self) -> StorageType {
        match self {
            Self::Mutable(index) => index.storage_type(),
            Self::Immutable(index) => index.storage_type(),
            Self::OnDisk(index) => index.storage_type(),
        }
    }
}

struct MapConditionChecker<'a, T, N: ?Sized, F> {
    index: &'a T,
    hw_counter: HardwareCounterCell,
    predicate: F,
    _key: PhantomData<fn(&N)>,
}

impl<'a, N, T, F> ConditionChecker for MapConditionChecker<'a, T, N, F>
where
    N: MapIndexKey + ?Sized + 'a,
    T: MapIndexRead<'a, N>,
    F: Fn(&N) -> bool,
{
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        self.index
            .check_values_any(point_id, &self.hw_counter, &self.predicate)
    }
}
