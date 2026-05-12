use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::payload_field_index::PayloadFieldIndexRead;
use crate::index::field_index::facet_index::FacetIndex;
use crate::index::field_index::numeric_index::NumericFieldIndexRead;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::telemetry::PayloadIndexTelemetry;

/// Read-only access surface of [`FieldIndex`](super::FieldIndex).
///
/// Extends [`PayloadFieldIndexRead`] with enum-level operations that
/// don't belong to any single typed-index variant: telemetry,
/// per-point value access, an index-aware condition check, value
/// retrieval, and downcasts to specific index views (`as_numeric`,
/// `as_facet_index`). The five overlapping read methods
/// (`count_indexed_points`, `filter`, `estimate_cardinality`,
/// `for_each_payload_block`, `condition_checker`) come from the
/// [`PayloadFieldIndexRead`] supertrait directly — no `&dyn`
/// indirection.
///
/// Lifecycle methods (`wipe`, `flusher`, `files`, …) and per-point
/// write methods (`add_point`, `remove_point`) stay inherent on
/// `FieldIndex`.
///
/// Helpers that destructure `FieldIndex` to reach a variant-specific
/// predicate on the underlying typed index keep taking `&FieldIndex`
/// directly — this trait only captures the uniform surface.
///
/// [`StructPayloadIndexReadView`]: crate::index::struct_payload_index::StructPayloadIndexReadView
pub trait FieldIndexRead: PayloadFieldIndexRead {
    /// Per-index telemetry snapshot.
    fn get_telemetry_data(&self) -> PayloadIndexTelemetry;

    /// Number of values for `point_id`.
    fn values_count(&self, point_id: PointOffsetType) -> usize;

    /// True if `point_id` has zero values in this index.
    fn values_is_empty(&self, point_id: PointOffsetType) -> bool;

    /// Build a closure that extracts this index's values for a given
    /// point as a [`MultiValue<Value>`](crate::common::utils::MultiValue).
    ///
    /// Returns `None` when this index can't serve point-level value
    /// retrieval — `FullTextIndex` (callers fall back to payload) and
    /// `NullIndex` (no underlying values to return).
    ///
    /// Used by rescore-formula value lookup; mirrors the shape of
    /// [`PayloadFieldIndexRead::condition_checker`] (build a closure
    /// once, invoke per point).
    fn value_retriever<'a, 'q>(
        &'a self,
        hw_counter: &'q HardwareCounterCell,
    ) -> Option<VariableRetrieverFn<'q>>
    where
        'a: 'q;

    /// Borrowed numeric view, if this index is numeric.
    ///
    /// The concrete numeric-index type is opaque per implementation —
    /// this matches the shape of [`PayloadIndexRead::numeric_index_for`].
    ///
    /// [`PayloadIndexRead::numeric_index_for`]: crate::index::PayloadIndexRead::numeric_index_for
    fn as_numeric(&self) -> Option<impl NumericFieldIndexRead + '_>;

    /// Borrowed facet view, if this index supports faceting.
    ///
    /// The concrete facet-index type is opaque per implementation —
    /// this matches the shape of [`PayloadIndexRead::facet_index_for`].
    ///
    /// [`PayloadIndexRead::facet_index_for`]: crate::index::PayloadIndexRead::facet_index_for
    fn as_facet_index(&self) -> Option<impl FacetIndex + '_>;
}
