use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};
use serde_json::Value;

use super::field_index::numeric_index::NumericFieldIndexRead;
use super::field_index::{FacetIndex, FieldIndex};
use super::query_optimization::rescore_formula::FormulaScorer;
use super::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::{IdTrackerRead, PointMappingsRefEnum};
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition};
use crate::json_path::JsonPath;
use crate::payload_storage::FilterContext;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{Filter, Payload, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef};

pub enum BuildIndexResult {
    /// Index was built
    Built(Vec<FieldIndex>),
    /// Index was already built
    AlreadyBuilt,
    /// Field Index already exists, but incompatible schema
    /// Requires extra actions to remove the old index.
    IncompatibleSchema,
}

/// Read-only trait for payload index.
///
/// Defines all read operations on the payload index. Search and retrieval logic
/// only requires this trait, which makes it possible to implement read-only
/// segments without duplicating index code.
pub trait PayloadIndexRead {
    /// Get indexed fields
    fn indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema>;

    /// Estimate amount of points (min, max) which satisfies filtering condition.
    ///
    /// A best estimation of the number of available points should be given.
    fn estimate_cardinality(
        &self,
        query: &Filter,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation>;

    /// Estimate amount of points (min, max) which satisfies filtering of a nested condition.
    fn estimate_nested_cardinality(
        &self,
        query: &Filter,
        nested_path: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation>;

    /// Return list of all point ids, which satisfy filtering criteria
    ///
    /// A best estimation of the number of available points should be given.
    ///
    /// If `is_stopped` is set to true during execution, the function should return early with no results.
    fn query_points(
        &self,
        filter: &Filter,
        hw_counter: &HardwareCounterCell,
        is_stopped: &AtomicBool,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OperationResult<Vec<PointOffsetType>>;

    /// Return number of points, indexed by this field
    fn indexed_points(&self, field: PayloadKeyTypeRef) -> usize;

    fn filter_context<'a>(
        &'a self,
        filter: &'a Filter,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Box<dyn FilterContext + 'a>>;

    /// Look up a numeric index for the given payload key, if one exists.
    ///
    /// Used by ordered reads to stream values from the index in sort order.
    /// The concrete numeric-index type is opaque so each implementation can
    /// expose its own internal representation.
    fn numeric_index_for(&self, key: &PayloadKeyType) -> Option<impl NumericFieldIndexRead + '_>;

    /// Look up a facet index for the given payload key, if one exists.
    ///
    /// Used by faceting to enumerate values and per-value point sets. The
    /// concrete facet-index type is opaque per implementation.
    fn facet_index_for(&self, key: &JsonPath) -> Option<impl FacetIndex + '_>;

    /// Per-field-index telemetry data.
    fn get_telemetry_data(&self) -> Vec<PayloadIndexTelemetry>;

    /// Build a per-query formula scorer that evaluates the given parsed
    /// formula against this index's payload, using the prefetch scores as
    /// extra inputs.
    fn formula_scorer<'q>(
        &'q self,
        parsed_formula: &'q ParsedFormula,
        prefetches_scores: &'q [AHashMap<PointOffsetType, ScoreType>],
        hw_counter: &'q HardwareCounterCell,
    ) -> OperationResult<FormulaScorer<'q>>;

    /// Iterate point offsets that match the filter.
    ///
    /// Generic over `I: IdTrackerRead` so callers pass their concrete tracker
    /// without dynamic dispatch; the iterator return uses RPITIT so each impl
    /// keeps its own zero-cost concrete chain.
    #[allow(clippy::too_many_arguments)]
    fn iter_filtered_points<'a, I: IdTrackerRead>(
        &'a self,
        filter: &'a Filter,
        id_tracker: &'a I,
        point_mappings: &'a PointMappingsRefEnum<'a>,
        query_cardinality: &'a CardinalityEstimation,
        hw_counter: &'a HardwareCounterCell,
        is_stopped: &'a AtomicBool,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OperationResult<impl Iterator<Item = PointOffsetType> + 'a>;

    /// Iterate conditions for payload blocks with minimum size of `threshold`
    /// Required for building HNSW index
    fn for_each_payload_block(
        &self,
        field: PayloadKeyTypeRef,
        threshold: usize,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()>;

    /// Get payload for point
    fn get_payload(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload>;

    /// Get payload for point with potential optimization for sequential access.
    fn get_payload_sequential(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload>;
}

/// Trait for payload index with mutating operations.
pub trait PayloadIndex: PayloadIndexRead {
    /// Build the index, if not built before, taking the caller by reference only
    fn build_index(
        &self,
        field: PayloadKeyTypeRef,
        payload_schema: &PayloadFieldSchema,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BuildIndexResult>;

    /// Apply already built indexes
    fn apply_index(
        &mut self,
        field: PayloadKeyType,
        payload_schema: PayloadFieldSchema,
        field_index: Vec<FieldIndex>,
    ) -> OperationResult<()>;

    /// Mark field as one which should be indexed
    fn set_indexed(
        &mut self,
        field: PayloadKeyTypeRef,
        payload_schema: impl Into<PayloadFieldSchema>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    /// Remove index
    fn drop_index(&mut self, field: PayloadKeyTypeRef) -> OperationResult<bool>;

    /// Remove index if incompatible with new payload schema
    fn drop_index_if_incompatible(
        &mut self,
        field: PayloadKeyTypeRef,
        new_payload_schema: &PayloadFieldSchema,
    ) -> OperationResult<bool>;

    /// Overwrite payload for point_id. If payload already exists, replace it.
    fn overwrite_payload(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    /// Assign payload to a concrete point with a concrete payload value
    fn set_payload(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &Option<JsonPath>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    /// Delete payload by key
    fn delete_payload(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<Value>>;

    /// Drop all payload of the point
    fn clear_payload(
        &mut self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>>;

    /// Return function that forces persistence of current storage state.
    fn flusher(&self) -> Flusher;

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<(PayloadKeyType, PathBuf)> {
        Vec::new()
    }
}
