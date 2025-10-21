use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::TelemetryDetail;

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult, SegmentFailedState};
use crate::data_types::build_index_result::BuildFieldIndexResult;
use crate::data_types::facets::{FacetParams, FacetValue};
use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::order_by::{OrderBy, OrderValue};
use crate::data_types::query_context::{FormulaContext, QueryContext, SegmentQueryContext};
use crate::data_types::vectors::{QueryVector, VectorInternal};
use crate::entry::snapshot_entry::SnapshotEntry;
use crate::index::field_index::{CardinalityEstimation, FieldIndex};
use crate::json_path::JsonPath;
use crate::telemetry::SegmentTelemetry;
use crate::types::{
    Filter, Payload, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef, PointIdType,
    ScoredPoint, SearchParams, SegmentConfig, SegmentInfo, SegmentType, SeqNumberType, VectorName,
    VectorNameBuf, WithPayload, WithVector,
};

/// Define all operations which can be performed with Segment or Segment-like entity.
///
/// Assume all operations are idempotent - which means that no matter how many times an operation
/// is executed - the storage state will be the same.
pub trait SegmentEntry: SnapshotEntry {
    /// Get current update version of the segment
    fn version(&self) -> SeqNumberType;

    /// Get current persistent version of the segment
    fn persistent_version(&self) -> SeqNumberType;

    fn is_proxy(&self) -> bool;

    /// Get version of specified point
    ///
    /// Returns `None` if point does not exist or is soft-deleted.
    fn point_version(&self, point_id: PointIdType) -> Option<SeqNumberType>;

    #[allow(clippy::too_many_arguments)]
    fn search_batch(
        &self,
        vector_name: &VectorName,
        query_vectors: &[&QueryVector],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &SegmentQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPoint>>>;

    /// Rescore results with a formula that can reference payload values.
    ///
    /// A deleted bitslice is passed to exclude points from a wrapped segment.
    fn rescore_with_formula(
        &self,
        formula_ctx: Arc<FormulaContext>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<ScoredPoint>>;

    fn upsert_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vectors: NamedVectors,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    fn delete_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    fn update_vectors(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vectors: NamedVectors,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    fn delete_vector(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vector_name: &VectorName,
    ) -> OperationResult<bool>;

    fn set_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        payload: &Payload,
        key: &Option<JsonPath>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    fn set_full_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        full_payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    fn delete_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        key: PayloadKeyTypeRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    fn clear_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool>;

    fn vector(
        &self,
        vector_name: &VectorName,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<VectorInternal>>;

    fn all_vectors(
        &self,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<NamedVectors<'_>>;

    /// Retrieve payload for the point
    /// If not found, return empty payload
    fn payload(
        &self,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload>;

    /// Iterator over all points in segment in ascending order.
    fn iter_points(&self) -> Box<dyn Iterator<Item = PointIdType> + '_>;

    /// Paginate over points which satisfies filtering condition starting with `offset` id including.
    ///
    /// Cancelled by `is_stopped` flag.
    fn read_filtered<'a>(
        &'a self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        filter: Option<&'a Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<PointIdType>;

    /// Return points which satisfies filtering condition ordered by the `order_by.key` field,
    /// starting with `order_by.start_from` value including.
    ///
    /// Will fail if there is no index for the order_by key.
    /// Cancelled by `is_stopped` flag.
    fn read_ordered_filtered<'a>(
        &'a self,
        limit: Option<usize>,
        filter: Option<&'a Filter>,
        order_by: &'a OrderBy,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<(OrderValue, PointIdType)>>;

    /// Return random points which satisfies filtering condition.
    ///
    /// Cancelled by `is_stopped` flag.
    fn read_random_filtered(
        &self,
        limit: usize,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<PointIdType>;

    /// Read points in [from; to) range
    fn read_range(&self, from: Option<PointIdType>, to: Option<PointIdType>) -> Vec<PointIdType>;

    /// Return all unique values for the given key.
    fn unique_values(
        &self,
        key: &JsonPath,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BTreeSet<FacetValue>>;

    /// Return the largest counts for the given facet request.
    fn facet(
        &self,
        request: &FacetParams,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>>;

    /// Check if there is point with `point_id` in this segment.
    ///
    /// Soft deleted points are excluded.
    fn has_point(&self, point_id: PointIdType) -> bool;

    /// Estimate available point count in this segment for given filter.
    fn estimate_point_count<'a>(
        &'a self,
        filter: Option<&'a Filter>,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation;

    fn vector_names(&self) -> HashSet<VectorNameBuf>;

    /// Whether this segment is completely empty in terms of points
    ///
    /// The segment is considered to not be empty if it contains any points, even if deleted.
    /// Deleted points still have a version which may be important at time of recovery. Deciding
    /// this by just the reported point count is not reliable in case a proxy segment is used.
    ///
    /// Payload indices or type of storage are not considered here.
    fn is_empty(&self) -> bool;

    /// Number of available points
    ///
    /// - excludes soft deleted points
    fn available_point_count(&self) -> usize;

    /// Number of deleted points
    fn deleted_point_count(&self) -> usize;

    /// Size of all available vectors in storage
    fn available_vectors_size_in_bytes(&self, vector_name: &VectorName) -> OperationResult<usize>;

    /// Max value from all `available_vectors_size_in_bytes`
    fn max_available_vectors_size_in_bytes(&self) -> OperationResult<usize> {
        self.vector_names()
            .into_iter()
            .map(|vector_name| self.available_vectors_size_in_bytes(&vector_name))
            .collect::<OperationResult<Vec<_>>>()
            .map(|sizes| sizes.into_iter().max().unwrap_or_default())
    }

    /// Get segment type
    fn segment_type(&self) -> SegmentType;

    /// Get current stats of the segment
    fn info(&self) -> SegmentInfo;

    /// Get size related stats of the segment.
    /// This returns `SegmentInfo` with some non size-related data (like `schema`) unset to improve performance.
    fn size_info(&self) -> SegmentInfo;

    /// Get segment configuration
    fn config(&self) -> &SegmentConfig;

    /// Whether this segment is appendable
    ///
    /// Returns appendable state of outer most segment. If this is a proxy segment, this shadows
    /// the appendable state of the wrapped segment.
    fn is_appendable(&self) -> bool;

    /// Get flush ordering affinity
    /// When multiple segments are flushed together, it must follow this ordering to guarantee data
    /// consistency.
    fn flush_ordering(&self) -> SegmentFlushOrdering;

    /// Returns a function, which when called, will flush all pending changes to disk.
    /// If there are currently no changes to flush, returns None.
    /// If `force` is true, will return a flusher even if there are no changes to flush.
    fn flusher(&self, force: bool) -> Option<Flusher>;

    /// Immediately flush all changes to disk and return persisted version.
    /// Blocks the current thread.
    fn flush(&self, force: bool) -> OperationResult<SeqNumberType> {
        if let Some(flusher) = self.flusher(force) {
            flusher()?;
        }
        Ok(self.persistent_version())
    }

    /// Removes all persisted data and forces to destroy segment
    fn drop_data(self) -> OperationResult<()>;

    /// Path to data, owned by segment
    fn data_path(&self) -> PathBuf;

    /// Delete field index, if exists
    fn delete_field_index(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<bool>;

    /// Delete field index, if exists and doesn't match the schema
    fn delete_field_index_if_incompatible(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
        field_schema: &PayloadFieldSchema,
    ) -> OperationResult<bool>;

    /// Build the field index for the key and schema, if not built before.
    fn build_field_index(
        &self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
        field_type: &PayloadFieldSchema,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BuildFieldIndexResult>;

    /// Apply a built index. Returns whether it was actually applied or not.
    fn apply_field_index(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyType,
        field_schema: PayloadFieldSchema,
        field_index: Vec<FieldIndex>,
    ) -> OperationResult<bool>;

    /// Create index for a payload field, if not exists
    fn create_field_index(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
        field_schema: Option<&PayloadFieldSchema>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        let Some(field_schema) = field_schema else {
            // Legacy case, where we tried to automatically detect the schema for the field.
            // We don't do this anymore, as it is not reliable.
            return Err(OperationError::TypeInferenceError {
                field_name: key.clone(),
            });
        };

        self.delete_field_index_if_incompatible(op_num, key, field_schema)?;

        let (schema, indexes) =
            match self.build_field_index(op_num, key, field_schema, hw_counter)? {
                BuildFieldIndexResult::SkippedByVersion => {
                    return Ok(false);
                }
                BuildFieldIndexResult::AlreadyExists => {
                    return Ok(false);
                }
                BuildFieldIndexResult::IncompatibleSchema => {
                    // This is a service error, as we should have just removed the old index
                    // So it should not be possible to get this error
                    return Err(OperationError::service_error(format!(
                        "Incompatible schema for field index on field {key}",
                    )));
                }
                BuildFieldIndexResult::Built { schema, indexes } => (schema, indexes),
            };

        self.apply_field_index(op_num, key.to_owned(), schema, indexes)
    }

    /// Get indexed fields
    fn get_indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema>;

    /// Checks if segment errored during last operations
    fn check_error(&self) -> Option<SegmentFailedState>;

    // Get collected telemetry data of segment
    fn get_telemetry_data(&self, detail: TelemetryDetail) -> SegmentTelemetry;

    fn fill_query_context(&self, query_context: &mut QueryContext);
}

/// Defines in what order multiple segments must be flushed.
///
/// To achieve data consistency with our point copy on write mechanism, we must flush segments in a
/// strict order. Appendable segments must be flushed first, non-appendable segments last. Proxy
/// segments fall in between.
///
/// When flush the segment holder, we effectively flush in four stages defined by the enum variants
/// below.
#[derive(PartialEq, Eq, Debug, Clone, Copy, Ord, PartialOrd)]
pub enum SegmentFlushOrdering {
    // Must always be flushed first
    // - Point-CoW moves points into this segment
    Appendable,
    // - Point-CoW may have moved points into this segment before proxying, might be pending flush
    // - Point-CoW may have moved out and deleted points from this segment, these are not persisted
    ProxyWithAppendable,
    // - Point-CoW may have moved out and deleted points from here, these are not persisted
    ProxyWithNonAppendable,
    // Must always be flushed last
    // - Point-CoW moves out and deletes points from this segment
    NonAppendable,
}

impl SegmentFlushOrdering {
    pub fn proxy(self) -> Self {
        match self {
            SegmentFlushOrdering::Appendable => SegmentFlushOrdering::ProxyWithAppendable,
            SegmentFlushOrdering::NonAppendable => SegmentFlushOrdering::ProxyWithNonAppendable,
            proxy @ SegmentFlushOrdering::ProxyWithAppendable => proxy,
            proxy @ SegmentFlushOrdering::ProxyWithNonAppendable => proxy,
        }
    }
}
