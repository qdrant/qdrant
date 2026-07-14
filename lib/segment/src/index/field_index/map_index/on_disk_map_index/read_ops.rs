use std::borrow::{Borrow, Cow};
use std::iter;

use common::counter::conditioned_counter::ConditionedCounter;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::iterator_hw_measurement::HwMeasurementIteratorExt;
use common::persisted_hashmap::{Key, READ_ENTRY_OVERHEAD};
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use itertools::Itertools;
use roaring::RoaringBitmap;

use super::super::read_ops::MapIndexRead;
use super::super::{IdIter, MapIndexKey};
use super::OnDiskMapIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::on_disk_point_to_values::ValuesIter;
use crate::index::field_index::stat_tools::number_of_selected_points;
use crate::index::field_index::CardinalityEstimation;
use crate::index::payload_config::StorageType;

impl<'a, N: MapIndexKey + Key + ?Sized + 'a, S: UniversalRead> MapIndexRead<'a, N>
    for OnDiskMapIndex<N, S>
{
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&N) -> bool,
    ) -> OperationResult<bool> {
        let hw_counter = ConditionedCounter::always(hw_counter);

        // Measure self.deleted access.
        hw_counter
            .payload_index_io_read_counter()
            .incr_delta(size_of::<bool>());

        if !self.storage.deleted.is_active(idx) {
            return Ok(false);
        }

        self.storage
            .point_to_values
            .check_values_any(idx, |v| check_fn(v), &hw_counter)
    }

    fn get_values(
        &'a self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<impl Iterator<Item = Cow<'a, N>> + 'a> {
        let hw_counter = ConditionedCounter::always(hw_counter);

        // We can account cost of reading `bool`, but it will likely be more expensive, than
        // actually reading bool itself.

        if self.storage.deleted.is_active(idx) {
            self.storage
                .point_to_values
                .values_iter(idx, hw_counter)
                .ok()?
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Cow<'_, N>>>)
        } else {
            None
        }
    }

    fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        if self.storage.deleted.is_active(idx) {
            self.storage.point_to_values.get_values_count(idx).ok()?
        } else {
            None
        }
    }

    fn get_indexed_points(&self) -> usize {
        self.storage
            .point_to_values
            .len()
            .saturating_sub(self.storage.deleted.deleted_count())
    }

    /// Returns the number of key-value pairs in the index.
    /// Note that is doesn't count deleted pairs.
    fn get_values_count(&self) -> usize {
        self.total_key_value_pairs
    }

    fn get_unique_values_count(&self) -> usize {
        self.storage.value_to_points.keys_count()
    }

    fn get_count_for_value(&self, value: &N, hw_counter: &HardwareCounterCell) -> Option<usize> {
        let hw_counter = ConditionedCounter::always(hw_counter);

        // Since `value_to_points.get` doesn't actually force read from disk for all values
        // we need to only account for the overhead of hashmap lookup
        hw_counter
            .payload_index_io_read_counter()
            .incr_delta(READ_ENTRY_OVERHEAD);

        match self
            .storage
            .value_to_points
            .unbatched_get_values_count(value)
        {
            Ok(Some(count)) => Some(count),
            Ok(None) => None,
            Err(err) => {
                debug_assert!(
                    false,
                    "Error while getting count for value {value:?}: {err:?}",
                );
                log::error!("Error while getting count for value {value:?}: {err:?}");
                None
            }
        }
    }

    fn get_iterator(&self, value: &N, hw_counter: &HardwareCounterCell) -> IdIter<'_> {
        let hw_counter = ConditionedCounter::always(hw_counter);

        match self.storage.value_to_points.unbatched_get(value) {
            Ok(Some(values)) => {
                // We're iterating over the whole (mmapped) slice
                hw_counter
                    .payload_index_io_read_counter()
                    .incr_delta(size_of_val(values.as_slice()) + READ_ENTRY_OVERHEAD);

                let deleted = &self.storage.deleted;
                Box::new(
                    values
                        .into_iter()
                        .filter(move |idx| deleted.is_active(*idx)),
                )
            }
            Ok(None) => {
                hw_counter
                    .payload_index_io_read_counter()
                    .incr_delta(READ_ENTRY_OVERHEAD);

                Box::new(iter::empty())
            }
            Err(err) => {
                debug_assert!(
                    false,
                    "Error while getting iterator for value {value:?}: {err:?}",
                );
                log::error!("Error while getting iterator for value {value:?}: {err:?}");
                Box::new(iter::empty())
            }
        }
    }

    fn match_cardinality(
        &self,
        value: &N,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        let Some(stored_count) = self.get_count_for_value(value, hw_counter) else {
            return CardinalityEstimation::exact(0);
        };

        if self.storage.deleted.deleted_count() == 0 {
            return CardinalityEstimation::exact(stored_count);
        }

        let live_points = self.get_indexed_points();
        let total_points = live_points + self.storage.deleted.deleted_count();
        let max = stored_count.min(live_points);
        let exp = if total_points == 0 {
            0
        } else {
            stored_count
                .saturating_mul(live_points)
                .div_ceil(total_points)
                .min(max)
        };

        CardinalityEstimation {
            primary_clauses: vec![],
            min: 0,
            exp,
            max,
        }
    }

    fn except_cardinality<'b>(
        &self,
        excluded: impl Iterator<Item = &'b N>,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation
    where
        N: 'b,
    {
        let excluded_value_counts: Vec<_> = excluded
            .map(|val| self.get_count_for_value(val, hw_counter).unwrap_or(0))
            .collect();
        let total_excluded_value_count: usize = excluded_value_counts.iter().sum();

        if self.storage.deleted.deleted_count() == 0 {
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

            let min_not_excluded_by_values =
                non_excluded_values_count.div_ceil(max_values_per_point);

            let min = min_not_excluded_by_values.max(
                self.get_indexed_points()
                    .saturating_sub(total_excluded_value_count),
            );

            let max_excluded_value_count = excluded_value_counts.iter().max().copied().unwrap_or(0);

            let max = self
                .get_indexed_points()
                .saturating_sub(max_excluded_value_count)
                .min(non_excluded_values_count);

            let exp =
                number_of_selected_points(self.get_indexed_points(), non_excluded_values_count)
                    .max(min)
                    .min(max);

            return CardinalityEstimation {
                primary_clauses: vec![],
                min,
                exp,
                max,
            };
        }

        let live_points = self.get_indexed_points();
        let stored_non_excluded_values = self
            .get_values_count()
            .saturating_sub(total_excluded_value_count);

        if live_points == 0 || stored_non_excluded_values == 0 {
            return CardinalityEstimation::exact(0);
        }

        let total_points = live_points + self.storage.deleted.deleted_count();
        let estimated_live_non_excluded_values = stored_non_excluded_values
            .saturating_mul(live_points)
            .div_ceil(total_points);
        let exp = number_of_selected_points(live_points, estimated_live_non_excluded_values)
            .min(live_points);

        CardinalityEstimation {
            primary_clauses: vec![],
            min: 0,
            exp,
            max: live_points,
        }
    }

    /// Batched override of [`MapIndexRead::for_values_map`].
    ///
    /// `f` may be called in any order, since batched reads can complete out of
    /// order.
    fn for_values_map<V: Borrow<N>>(
        &self,
        values: impl Iterator<Item = V>,
        hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        let hw_counter = ConditionedCounter::always(hw_counter);

        let values: Vec<V> = values.collect();
        let requests = values.iter().map(|value| {
            let key: &N = value.borrow();
            (key, key)
        });

        self.storage
            .value_to_points
            .for_each_entry_in_iter(requests, |key, point_ids| {
                // Mirror `get_iterator`'s IO accounting.
                let io_read = match point_ids {
                    Some(ids) => size_of_val(ids) + READ_ENTRY_OVERHEAD,
                    None => READ_ENTRY_OVERHEAD,
                };
                hw_counter
                    .payload_index_io_read_counter()
                    .incr_delta(io_read);

                let deleted = &self.storage.deleted;
                let mut ids = point_ids
                    .unwrap_or(&[])
                    .iter()
                    .copied()
                    .filter(move |point| deleted.is_active(*point));

                f(key, &mut ids)
            })
    }

    /// Batched override of [`MapIndexRead::iter_for_values`].
    ///
    /// Resolves every value's posting in a single batched read and collects the
    /// union into a [`RoaringBitmap`]
    fn iter_for_values<V: Borrow<N> + 'a>(
        &'a self,
        values: impl Iterator<Item = V> + 'a,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<IdIter<'a>> {
        let mut ids = RoaringBitmap::new();
        self.for_values_map(values, hw_counter, |_value, posting| {
            ids.extend(&mut *posting);
            Ok(())
        })?;
        Ok(Box::new(ids.into_iter()))
    }

    fn for_each_value(&self, f: impl FnMut(&N) -> OperationResult<()>) -> OperationResult<()> {
        self.storage.value_to_points.for_each_key(f)
    }

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        mut f: impl FnMut(&N, usize) -> OperationResult<()>,
    ) -> OperationResult<()> {
        let deleted = &self.storage.deleted;
        self.storage.value_to_points.for_each_entry(|k, v| {
            let count = v
                .iter()
                .filter(|&&idx| {
                    deleted.is_active(idx)

                    // TODO(deferred): Maybe we can improve this filter and use take_while instead. For this we
                    // need to make sure that `v` is always sorted which we _can_ enforce when finalizing the index.
                    && deferred_internal_id.is_none_or(|deferred| idx < deferred)
                })
                .unique()
                .count();
            f(k, count)
        })
    }

    fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        let hw_counter = ConditionedCounter::always(hw_counter);

        let deleted = &self.storage.deleted;

        self.storage.value_to_points.for_each_entry(|k, v| {
            hw_counter
                .payload_index_io_read_counter()
                .incr_delta(k.write_bytes());

            let mut iter = v
                .iter()
                .copied()
                .filter(|idx| deleted.is_active(*idx))
                .measure_hw_with_acc(
                    hw_counter.new_accumulator(),
                    size_of::<PointOffsetType>(),
                    |i| i.payload_index_io_read_counter(),
                );

            f(k, &mut iter)
        })
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Mmap { is_on_disk: true }
    }

    fn ram_usage_bytes(&self) -> usize {
        self.storage.ram_usage_bytes()
    }

    fn telemetry_index_type(&self) -> &'static str {
        "mmap_map"
    }
}

impl<N: MapIndexKey + Key + ?Sized, S: UniversalRead> OnDiskMapIndex<N, S> {
    pub fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(PointOffsetType, ValuesIter<'_, N>),
    ) -> OperationResult<()> {
        let hw_counter = ConditionedCounter::always(hw_counter);

        // Skip deleted points
        let points = points.filter(|&idx| self.storage.deleted.is_active(idx));

        self.storage
            .point_to_values
            .values_iter_batch(points, hw_counter, f)
    }

    pub fn is_on_disk(&self) -> bool {
        true
    }
}
