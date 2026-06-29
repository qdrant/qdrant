use std::borrow::{Borrow, Cow};
use std::hash::{BuildHasher, Hash};

use common::condition_checker::{CheckItem, ConditionChecker, Partitioner, Rest, Select};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UserData;
use fnv::FnvBuildHasher;
use gridstore::Blob;
use indexmap::IndexSet;
use itertools::Itertools;

use super::key::MapIndexKey;
use super::{IdIter, MapIndex};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::CardinalityEstimation;
use crate::index::field_index::stat_tools::number_of_selected_points;
use crate::index::payload_config::{IndexMutability, StorageType};
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

    /// Batched counterpart of [`Self::check_values_any`].
    fn for_each_matching_value<I, F, M, U>(
        &self,
        items: I,
        hw_counter: &HardwareCounterCell,
        check_fn: F,
        mut on_match: M,
    ) -> OperationResult<()>
    where
        U: UserData,
        I: Iterator<Item = (U, PointOffsetType)>,
        F: Fn(&N) -> bool,
        M: FnMut(U, bool),
    {
        for (tag, idx) in items {
            on_match(tag, self.check_values_any(idx, hw_counter, &check_fn)?);
        }
        Ok(())
    }

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

    /// Use each value's posting of point ids, invoking `f` once per value.
    ///
    /// Order of invocations is not guaranteed.
    fn for_values_map<V: Borrow<N>>(
        &self,
        values: impl Iterator<Item = V>,
        hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        for value in values {
            let value = value.borrow();
            let mut ids = self.get_iterator(value, hw_counter);
            f(value, &mut ids)?;
        }
        Ok(())
    }

    /// Iterator over deduplicated points matching **any** of the given `values`.
    fn iter_for_values<V: Borrow<N> + 'a>(
        &'a self,
        values: impl Iterator<Item = V> + 'a,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<IdIter<'a>> {
        Ok(Box::new(
            values
                .flat_map(move |value| self.get_iterator(value.borrow(), hw_counter))
                .unique(),
        ))
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
        value: impl Borrow<N>,
    ) -> MapConditionChecker<'a, N, Self> {
        MapConditionChecker {
            index: self,
            hw_counter,
            predicate: MapPredicate::Value(<N as MapIndexKey>::to_owned(value.borrow())),
        }
    }

    /// Condition checker for [`crate::types::Match::Prefix`].
    ///
    /// Checks a point's values through the forward index, so it works in
    /// every variant regardless of whether prefix structures were built.
    fn match_prefix_checker(
        &'a self,
        hw_counter: HardwareCounterCell,
        prefix: impl Borrow<N>,
    ) -> MapConditionChecker<'a, N, Self> {
        MapConditionChecker {
            index: self,
            hw_counter,
            predicate: MapPredicate::Prefix(<N as MapIndexKey>::to_owned(prefix.borrow())),
        }
    }

    /// Condition checker for
    /// - [`crate::types::Match::Any`] (when `negate` is `false`),
    /// - [`crate::types::Match::Except`] (when `negate` is `true`).
    fn match_any_checker<K, A>(
        &'a self,
        hw_counter: HardwareCounterCell,
        list: IndexSet<K, A>,
        negate: bool,
    ) -> MapConditionChecker<'a, N, Self>
    where
        A: BuildHasher,
        K: Borrow<N> + Hash + Eq,
    {
        let scan = list.len() < INDEXSET_ITER_THRESHOLD;
        let list = list
            .iter()
            .map(|key| <N as MapIndexKey>::to_owned(key.borrow()))
            .collect();
        MapConditionChecker {
            index: self,
            hw_counter,
            predicate: if scan {
                MapPredicate::AnyScan { list, negate }
            } else {
                MapPredicate::AnyProbe { list, negate }
            },
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

    fn for_each_count_per_value(
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
            MapIndex::OnDisk(index) => index.for_each_count_per_value(deferred_internal_id, f),
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

    // Dispatch instead of using default impl, for on-disk impl to use batched reads
    fn for_values_map<V: Borrow<N>>(
        &self,
        values: impl Iterator<Item = V>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.for_values_map(values, hw_counter, f),
            MapIndex::Immutable(index) => index.for_values_map(values, hw_counter, f),
            MapIndex::OnDisk(index) => index.for_values_map(values, hw_counter, f),
        }
    }

    // Dispatch instead of using default impl, for on-disk impl to use batched reads
    fn iter_for_values<V: Borrow<N> + 'a>(
        &'a self,
        values: impl Iterator<Item = V> + 'a,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<IdIter<'a>> {
        match self {
            MapIndex::Mutable(index) => index.iter_for_values(values, hw_counter),
            MapIndex::Immutable(index) => index.iter_for_values(values, hw_counter),
            MapIndex::OnDisk(index) => index.iter_for_values(values, hw_counter),
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

pub struct MapConditionChecker<'a, N: MapIndexKey + ?Sized, T> {
    index: &'a T,
    hw_counter: HardwareCounterCell,
    predicate: MapPredicate<N>,
}

enum MapPredicate<N: MapIndexKey + ?Sized> {
    /// For [`crate::types::Match::Value`].
    Value(<N as MapIndexKey>::Owned),
    /// For [`crate::types::Match::Prefix`]; meaningful for string keys only
    /// ([`MapIndexKey::starts_with`] is constant `false` elsewhere).
    Prefix(<N as MapIndexKey>::Owned),
    /// For [`crate::types::Match::Any`] and [`crate::types::Match::Except`],
    /// Linear scan version.
    AnyScan {
        list: IndexSet<<N as MapIndexKey>::Owned, FnvBuildHasher>,
        negate: bool,
    },
    /// For [`crate::types::Match::Any`] and [`crate::types::Match::Except`],
    /// hash-probe version.
    AnyProbe {
        list: IndexSet<<N as MapIndexKey>::Owned, FnvBuildHasher>,
        negate: bool,
    },
}

impl<'a, N, T> ConditionChecker for MapConditionChecker<'a, N, T>
where
    N: MapIndexKey + ?Sized + 'a,
    T: MapIndexRead<'a, N>,
{
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        self.index
            .check_values_any(point_id, &self.hw_counter, |value| self.matches(value))
    }

    fn check_batched<K: CheckItem>(
        &mut self,
        ids: &mut [K],
        select: Select,
        _rest: Rest,
    ) -> OperationResult<usize> {
        let p = Partitioner::new(ids);
        self.index.for_each_matching_value(
            p.iter().map(|item| (item, item.point_id())),
            &self.hw_counter,
            |value| self.matches(value),
            |item, matched| p.write(item, matched == select.is_match()),
        )?;
        Ok(p.finish())
    }
}

impl<'a, N: MapIndexKey + ?Sized + 'a, T> MapConditionChecker<'a, N, T> {
    /// Whether a single indexed `value` satisfies this checker's predicate.
    fn matches(&self, value: &N) -> bool {
        match &self.predicate {
            MapPredicate::Value(expected) => value == expected.borrow(),
            MapPredicate::Prefix(prefix) => N::starts_with(value, prefix.borrow()),
            MapPredicate::AnyScan { list, negate } => {
                list.iter().any(|key| key.borrow() == value) != *negate
            }
            MapPredicate::AnyProbe { list, negate } => list.contains(value) != *negate,
        }
    }
}
