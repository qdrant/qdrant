use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::operation_error::OperationResult;
use crate::index::field_index::facet_index::FacetIndex;
use crate::index::field_index::numeric_index::NumericFieldIndexRead;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, PayloadKeyType};

/// Read-only access surface of [`FieldIndex`](super::FieldIndex).
///
/// Mirrors the per-variant read methods that
/// [`StructPayloadIndexReadView`] and its helpers reach through
/// [`IndexesMap`]. Lifecycle methods (`wipe`, `flusher`, `files`,
/// …) and per-point write methods (`add_point`, `remove_point`)
/// stay inherent on `FieldIndex`.
///
/// Helpers that destructure `FieldIndex` to reach a variant-specific
/// predicate on the underlying typed index keep taking `&FieldIndex`
/// directly — this trait only captures the uniform surface.
///
/// [`StructPayloadIndexReadView`]: crate::index::struct_payload_index::StructPayloadIndexReadView
/// [`IndexesMap`]: crate::common::utils::IndexesMap
pub trait FieldIndexRead {
    /// Number of points with at least one value in this index.
    fn count_indexed_points(&self) -> usize;

    /// Iterator of point offsets matching `condition`. `None` if the
    /// condition can't be evaluated by this index type.
    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>>;

    /// Cardinality estimation for `condition`. `Ok(None)` if the
    /// condition can't be evaluated by this index type.
    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>>;

    /// Iterate payload blocks of at least `threshold` points.
    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()>;

    /// Per-index telemetry snapshot.
    fn get_telemetry_data(&self) -> PayloadIndexTelemetry;

    /// Number of values for `point_id`.
    fn values_count(&self, point_id: PointOffsetType) -> usize;

    /// True if `point_id` has zero values in this index.
    fn values_is_empty(&self, point_id: PointOffsetType) -> bool;

    /// Index-aware check for conditions that need parameters held by
    /// the index (today: full-text tokenizers).
    ///
    /// Returns `Ok(None)` for index types that don't have such
    /// conditions, `Ok(Some(true))` if the condition is satisfied,
    /// `Ok(Some(false))` if it is not.
    fn special_check_condition(
        &self,
        condition: &FieldCondition,
        payload_value: &Value,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<bool>>;

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
