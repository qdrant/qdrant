use std::borrow::Cow;
use std::ops::Bound;

use common::bitvec::BitSliceExt as _;
use common::counter::conditioned_counter::ConditionedCounter;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::iterator_hw_measurement::HwMeasurementIteratorExt;
use common::generic_consts::Random;
use common::types::PointOffsetType;
use common::universal_io::{ReadRange, UniversalRead};
use itertools::Either;

use super::super::Encodable;
use super::super::numeric_index_read::NumericIndexRead;
use super::OnDiskNumericIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::index::payload_config::StorageType;

impl<T: Encodable + Numericable + Default + StoredValue + 'static, S: UniversalRead>
    NumericIndexRead<T> for OnDiskNumericIndex<T, S>
{
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&T) -> bool,
        hw_counter: &HardwareCounterCell,
    ) -> bool {
        // FIXME: don't silently ignore error — propagate it once
        // [`NumericIndexRead::check_values_any`] returns `OperationResult`.
        let hw_counter = ConditionedCounter::always(hw_counter);

        if self.storage.deleted.get_bit(idx as usize) == Some(false) {
            self.storage
                .point_to_values
                .check_values_any(idx, |v| check_fn(v), &hw_counter)
                .unwrap_or(false)
        } else {
            false
        }
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        if self.storage.deleted.get_bit(idx as usize) == Some(false) {
            Some(Box::new(
                self.storage
                    .point_to_values
                    // TODO: Propagate counter upwards
                    .values_iter(idx, ConditionedCounter::never())
                    .ok()??
                    .map(|v| *v),
            ))
        } else {
            None
        }
    }

    fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        if self.storage.deleted.get_bit(idx as usize) == Some(false) {
            self.storage.point_to_values.get_values_count(idx).ok()?
        } else {
            None
        }
    }

    /// Returns the number of key-value pairs in the index.
    /// Note that is doesn't count deleted pairs.
    fn total_unique_values_count(&self) -> OperationResult<usize> {
        Ok(self.storage.pairs.len()? as usize)
    }

    fn values_range<'a>(
        &'a self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<impl Iterator<Item = PointOffsetType> + 'a> {
        let hw_counter = ConditionedCounter::always(hw_counter);

        Ok(self
            .values_range_iterator(start_bound, end_bound)?
            .map(|point| point.idx)
            .measure_hw_with_condition_cell(hw_counter, size_of::<Point<T>>(), |i| {
                i.payload_index_io_read_counter()
            }))
    }

    fn orderable_values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_> {
        Ok(self
            .values_range_iterator(start_bound, end_bound)?
            .map(|Point { val, idx, .. }| (val, idx)))
    }

    /// Cheap `O(log n)` boundary search over the on-disk sorted pairs.
    ///
    /// `hw_counter` is unused: the two binary searches are accounted
    /// upfront by the caller ([`query::estimate_points`]).
    ///
    /// [`query::estimate_points`]: super::super::query::estimate_points
    fn values_range_size(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        let (start, end) = self.values_range_bounds(start_bound, end_bound)?;
        Ok(end - start)
    }

    fn get_histogram(&self) -> &Histogram<T> {
        &self.histogram
    }

    fn get_points_count(&self) -> usize {
        self.storage.point_to_values.len() - self.deleted_count
    }

    fn get_max_values_per_point(&self) -> usize {
        self.max_values_per_point
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Mmap { is_on_disk: true }
    }

    fn ram_usage_bytes(&self) -> usize {
        self.histogram.ram_usage_bytes() + self.storage.ram_usage_bytes()
    }

    fn telemetry_index_type(&self) -> &'static str {
        "mmap_numeric"
    }
}

impl<T: Encodable + Numericable + Default + StoredValue + 'static, S: UniversalRead>
    OnDiskNumericIndex<T, S>
{
    /// Binary search within `[lo, hi)` range of `pairs` storage.
    ///
    /// Returns `Ok(index)` if the element is found, `Err(index)` if not
    /// (where `index` is where the element would be inserted).
    fn binary_search_pairs(
        &self,
        bound: &Point<T>,
        lo: usize,
        hi: usize,
    ) -> OperationResult<Result<usize, usize>> {
        let mut left = lo;
        let mut right = hi;
        while left < right {
            let mid = left + (right - left) / 2;
            // TODO(luis): use read_one
            let elem = self.storage.pairs.read::<Random>(ReadRange {
                byte_offset: (mid * size_of::<Point<T>>()) as u64,
                length: 1,
            })?;
            match elem[0].cmp(bound) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Equal => return Ok(Ok(mid)),
                std::cmp::Ordering::Greater => right = mid,
            }
        }
        Ok(Err(left))
    }

    /// Find the `[start_index, end_index)` range for the given bounds.
    fn values_range_bounds(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> OperationResult<(usize, usize)> {
        let len = self.storage.pairs.len()? as usize;

        let start_index = match start_bound {
            Bound::Included(bound) => self
                .binary_search_pairs(&bound, 0, len)?
                .unwrap_or_else(|idx| idx),
            Bound::Excluded(bound) => match self.binary_search_pairs(&bound, 0, len)? {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            },
            Bound::Unbounded => 0,
        };

        if start_index >= len {
            return Ok((len, len));
        }

        let end_index = match end_bound {
            Bound::Included(bound) => match self.binary_search_pairs(&bound, start_index, len)? {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            },
            Bound::Excluded(bound) => self
                .binary_search_pairs(&bound, start_index, len)?
                .unwrap_or_else(|idx| idx),
            Bound::Unbounded => len,
        };

        Ok((start_index, end_index))
    }

    /// Returns an iterator over non-deleted pairs.
    ///
    /// This will read the entire range upfront if it was not already cached.
    fn values_range_iterator(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> OperationResult<impl DoubleEndedIterator<Item = Point<T>> + '_> {
        let (start_pos, end_pos) = self.values_range_bounds(start_bound, end_bound)?;
        let count = end_pos - start_pos;

        let iter = if count > 0 {
            match self.storage.pairs.read::<Random>(ReadRange {
                byte_offset: (start_pos * size_of::<Point<T>>()) as u64,
                length: count as u64,
            })? {
                Cow::Borrowed(slice) => Either::Left(slice.iter().copied()),
                Cow::Owned(vec) => Either::Right(vec.into_iter()),
            }
        } else {
            Either::Right(Vec::new().into_iter())
        };

        let deleted = &self.storage.deleted;

        Ok(iter.filter(move |point| !deleted.get_bit(point.idx as usize).unwrap_or(true)))
    }

    pub fn is_on_disk(&self) -> bool {
        true
    }
}
