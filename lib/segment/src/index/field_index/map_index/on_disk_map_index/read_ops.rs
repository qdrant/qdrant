use std::borrow::{Borrow, Cow};
use std::iter;

use common::counter::HwMeasurementIteratorExt;
use common::counter::hw::HwMetric;
use common::persisted_hashmap::{Key, READ_ENTRY_OVERHEAD};
use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UserData};
use itertools::Itertools;
use roaring::RoaringBitmap;

use super::super::read_ops::MapIndexRead;
use super::super::{IdIter, MapIndexKey};
use super::OnDiskMapIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::on_disk_point_to_values::ValuesIter;
use crate::index::payload_config::StorageType;

impl<'a, N: MapIndexKey + Key + ?Sized + 'a, S: UniversalRead> MapIndexRead<'a, N>
    for OnDiskMapIndex<N, S>
{
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&N) -> bool,
    ) -> OperationResult<bool> {
        // Measure self.deleted access.
        HwMetric::PayloadIndexIoRead.bump(size_of::<bool>());

        if !self.storage.deleted.is_active(idx) {
            return Ok(false);
        }

        self.storage
            .point_to_values
            .check_values_any(idx, |v| check_fn(v))
    }

    fn for_each_matching_value<I, F, M, U>(
        &self,
        items: I,
        check_fn: F,
        mut on_match: M,
    ) -> OperationResult<()>
    where
        U: UserData,
        I: Iterator<Item = (U, PointOffsetType)>,
        F: Fn(&N) -> bool,
        M: FnMut(U, bool),
    {
        self.storage.point_to_values.values_iter_batch(
            items,
            &self.storage.deleted,
            |tag, mut values| on_match(tag, values.any(|value| check_fn(&value))),
        )
    }

    fn get_values(&'a self, idx: PointOffsetType) -> Option<impl Iterator<Item = Cow<'a, N>> + 'a> {
        // We can account cost of reading `bool`, but it will likely be more expensive, than
        // actually reading bool itself.

        if self.storage.deleted.is_active(idx) {
            self.storage
                .point_to_values
                .values_iter(idx)
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

    fn get_count_for_value(&self, value: &N) -> Option<usize> {
        // Since `value_to_points.get` doesn't actually force read from disk for all values
        // we need to only account for the overhead of hashmap lookup
        HwMetric::PayloadIndexIoRead.bump(READ_ENTRY_OVERHEAD);

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

    fn get_iterator(&self, value: &N) -> IdIter<'_> {
        match self.storage.value_to_points.unbatched_get(value) {
            Ok(Some(values)) => {
                // We're iterating over the whole (mmapped) slice
                HwMetric::PayloadIndexIoRead
                    .bump(size_of_val(values.as_slice()) + READ_ENTRY_OVERHEAD);

                let deleted = &self.storage.deleted;
                Box::new(
                    values
                        .into_iter()
                        .filter(move |idx| deleted.is_active(*idx)),
                )
            }
            Ok(None) => {
                HwMetric::PayloadIndexIoRead.bump(READ_ENTRY_OVERHEAD);

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

    /// Batched override of [`MapIndexRead::for_values_map`].
    ///
    /// `f` may be called in any order, since batched reads can complete out of
    /// order.
    fn for_values_map<V: Borrow<N>>(
        &self,
        values: impl Iterator<Item = V>,
        mut f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
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
                HwMetric::PayloadIndexIoRead.bump(io_read);

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
    ) -> OperationResult<IdIter<'a>> {
        let mut ids = RoaringBitmap::new();
        self.for_values_map(values, |_value, posting| {
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
        mut f: impl FnMut(&N, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        let deleted = &self.storage.deleted;

        self.storage.value_to_points.for_each_entry(|k, v| {
            HwMetric::PayloadIndexIoRead.bump(k.write_bytes());

            let mut iter = v
                .iter()
                .copied()
                .filter(|idx| deleted.is_active(*idx))
                .measure_hw(HwMetric::PayloadIndexIoRead, size_of::<PointOffsetType>());

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
        f: impl FnMut(PointOffsetType, ValuesIter<'_, N>),
    ) -> OperationResult<()> {
        self.storage.point_to_values.values_iter_batch(
            points.map(|point_id| (point_id, point_id)),
            &self.storage.deleted,
            f,
        )
    }

    pub fn is_cold(&self) -> bool {
        true
    }
}
