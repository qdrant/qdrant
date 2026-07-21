use std::ops::Bound;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UserData;

use super::Encodable;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::on_disk_point_to_values::StoredValue;
use crate::index::payload_config::StorageType;
use crate::telemetry::PayloadIndexTelemetry;

/// Read-only operations supported by every numeric-index storage variant
/// ([`super::mutable_numeric_index::MutableNumericIndex`],
/// [`super::immutable_numeric_index::ImmutableNumericIndex`],
/// [`super::mmap_numeric_index::UniversalNumericIndex`]).
///
/// Signatures are unified across variants so the enum-level dispatcher in
/// [`NumericIndexInner`] can call them generically. Variants that don't
/// need `hw_counter` (`Mutable` / `Immutable`) accept and ignore it; the
/// storage-backed `Mmap` variant uses it to track payload-index IO.
///
/// [`NumericIndexInner`]: super::NumericIndexInner
pub trait NumericIndexRead<T: Encodable + Numericable + Default + StoredValue> {
    /// Hardware counter is used only by the mmap-backed variant; in-memory
    /// variants ignore it. Returns `false` if the mmap read fails (matches
    /// the legacy enum dispatcher behavior — see the FIXME on
    /// [`super::NumericIndexInner::check_values_any`]).
    fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&T) -> bool,
        hw_counter: &HardwareCounterCell,
    ) -> bool;

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
        F: Fn(&T) -> bool,
        M: FnMut(U, bool),
    {
        for (tag, idx) in items {
            on_match(tag, self.check_values_any(idx, &check_fn, hw_counter));
        }
        Ok(())
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>>;

    fn values_count(&self, idx: PointOffsetType) -> Option<usize>;

    fn total_unique_values_count(&self) -> OperationResult<usize>;

    fn values_range<'a>(
        &'a self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<impl Iterator<Item = PointOffsetType> + 'a>;

    fn orderable_values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_>;

    /// Number of `(value, point)` pairs in the given range.
    ///
    /// The default counts [`Self::values_range`]; variants with a
    /// precomputed sorted container (`Immutable` / `Mmap`) override it with
    /// an `O(log n)` boundary search.
    fn values_range_size(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        Ok(self
            .values_range(start_bound, end_bound, hw_counter)?
            .count())
    }

    fn get_histogram(&self) -> &Histogram<T>;

    fn get_points_count(&self) -> usize;

    fn get_max_values_per_point(&self) -> usize;

    fn storage_type(&self) -> StorageType;

    fn ram_usage_bytes(&self) -> usize;

    /// Per-variant telemetry tag (e.g. `"mutable_numeric"`, `"mmap_numeric"`).
    /// Used by the default [`Self::get_telemetry_data`] to fill the
    /// `index_type` field. Mirrors [`MapIndexRead::telemetry_index_type`][1].
    ///
    /// [1]: crate::index::field_index::map_index::read_ops::MapIndexRead::telemetry_index_type
    fn telemetry_index_type(&self) -> &'static str;

    // Default-method helpers derived from the required methods above.

    fn values_is_empty(&self, idx: PointOffsetType) -> bool {
        self.values_count(idx).unwrap_or(0) == 0
    }

    fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.get_points_count(),
            points_values_count: self.get_histogram().get_total_count(),
            histogram_bucket_size: Some(self.get_histogram().current_bucket_size()),
            index_type: self.telemetry_index_type(),
        }
    }
}
