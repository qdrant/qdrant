use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{DeferredBehavior, TelemetryDetail};
use uuid::Uuid;

use super::ReadOnlySegment;
use crate::common::operation_error::OperationResult;
use crate::data_types::facets::{FacetParams, FacetValue};
use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::order_by::{OrderBy, OrderValue};
use crate::data_types::query_context::{FormulaContext, QueryContext, SegmentQueryContext};
use crate::data_types::segment_record::{SegmentRecord, SegmentRecordRaw};
use crate::data_types::vectors::{QueryVector, VectorInternal};
use crate::entry::entry_point::ReadSegmentEntry;
use crate::id_tracker::IdTrackerRead;
use crate::index::field_index::CardinalityEstimation;
use crate::index::{PayloadIndexRead, UniversalReadExt};
use crate::json_path::JsonPath;
use crate::telemetry::SegmentTelemetry;
use crate::types::{
    ExtendedPointId, Filter, Payload, PayloadFieldSchema, PayloadKeyType, PointIdType, ScoredPoint,
    SearchParams, SegmentConfig, SegmentInfo, SegmentType, SeqNumberType, VectorName,
    VectorNameBuf, WithPayload, WithVector,
};

/// Read-only counterpart of `impl ReadSegmentEntry for Segment`.
///
/// A `ReadOnlySegment` never accepts appends, so `is_appendable()` is always
/// `false` and the `appendable_flag` passed into the view builders is hard-coded
/// accordingly. All other operations delegate to the shared `SegmentReadView`,
/// exactly like the mutable `Segment`.
impl<S: UniversalReadExt + 'static> ReadSegmentEntry for ReadOnlySegment<S> {
    fn is_proxy(&self) -> bool {
        false
    }

    fn point_version(&self, point_id: PointIdType) -> Option<SeqNumberType> {
        self.with_view(|view| view.point_version(point_id))
    }

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
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        self.with_view(|view| {
            view.search_batch(
                vector_name,
                query_vectors,
                with_payload,
                with_vector,
                filter,
                top,
                params,
                query_context,
            )
        })
    }

    fn rescore_with_formula(
        &self,
        ctx: Arc<FormulaContext>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<ScoredPoint>> {
        self.with_view(|view| view.rescore_with_formula(ctx, hw_counter))
    }

    fn vector(
        &self,
        vector_name: &VectorName,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<VectorInternal>> {
        self.with_view(|view| view.vector(vector_name, point_id, hw_counter))
    }

    fn vector_with_behavior(
        &self,
        vector_name: &VectorName,
        point_id: PointIdType,
        deferred_behavior: DeferredBehavior,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<VectorInternal>> {
        self.with_view(|view| {
            view.vector_with_behavior(vector_name, point_id, deferred_behavior, hw_counter)
        })
    }

    fn all_vectors(
        &self,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<NamedVectors<'_>> {
        self.with_view(|view| {
            let mut result = NamedVectors::default();
            for vector_name in view.vector_data.keys() {
                if let Some(vec) = view.vector_with_behavior(
                    vector_name,
                    point_id,
                    DeferredBehavior::VisibleOnly,
                    hw_counter,
                )? {
                    result.insert(vector_name.clone(), vec);
                }
            }
            Ok(result)
        })
    }

    fn payload(
        &self,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        self.with_view(|view| view.payload(point_id, hw_counter))
    }

    fn retrieve(
        &self,
        point_ids: &[PointIdType],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        hw_counter: &HardwareCounterCell,
        is_stopped: &AtomicBool,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<AHashMap<ExtendedPointId, SegmentRecord>> {
        self.with_view(|view| {
            view.retrieve(
                point_ids,
                with_payload,
                with_vector,
                hw_counter,
                is_stopped,
                deferred_behavior,
            )
        })
    }

    fn retrieve_raw(
        &self,
        point_ids: &[PointIdType],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        hw_counter: &HardwareCounterCell,
        is_stopped: &AtomicBool,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<AHashMap<ExtendedPointId, SegmentRecordRaw>> {
        self.with_view(|view| {
            view.retrieve_raw(
                point_ids,
                with_payload,
                with_vector,
                hw_counter,
                is_stopped,
                deferred_behavior,
            )
        })
    }

    fn read_filtered<'a>(
        &'a self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        filter: Option<&'a Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<Vec<PointIdType>> {
        self.with_view(|view| {
            view.read_filtered(
                offset,
                limit,
                filter,
                is_stopped,
                hw_counter,
                deferred_behavior,
            )
        })
    }

    fn read_ordered_filtered<'a>(
        &'a self,
        limit: Option<usize>,
        filter: Option<&'a Filter>,
        order_by: &'a OrderBy,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
        deferred_behavior: DeferredBehavior,
    ) -> OperationResult<Vec<(OrderValue, PointIdType)>> {
        self.with_view(|view| {
            view.read_ordered_filtered(
                limit,
                filter,
                order_by,
                is_stopped,
                hw_counter,
                deferred_behavior,
            )
        })
    }

    fn read_random_filtered(
        &self,
        limit: usize,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<PointIdType>> {
        self.with_view(|view| view.read_random_filtered(limit, filter, is_stopped, hw_counter))
    }

    fn read_range(&self, from: Option<PointIdType>, to: Option<PointIdType>) -> Vec<PointIdType> {
        self.with_view(|view| view.read_range(from, to))
    }

    fn unique_values(
        &self,
        key: &JsonPath,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BTreeSet<FacetValue>> {
        self.with_view(|view| view.facet_values(key, filter, is_stopped, hw_counter))
    }

    fn facet(
        &self,
        request: &FacetParams,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        self.with_view(|view| view.approximate_facet(request, is_stopped, hw_counter))
    }

    fn has_point(&self, point_id: PointIdType, deferred_behavior: DeferredBehavior) -> bool {
        self.id_tracker
            .borrow()
            .internal_id_with_behavior(point_id, deferred_behavior)
            .is_some()
    }

    fn estimate_point_count<'a>(
        &'a self,
        filter: Option<&'a Filter>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        self.with_view(|view| view.estimate_point_count(filter, hw_counter))
    }

    fn vector_names(&self) -> HashSet<VectorNameBuf> {
        self.vector_data.keys().cloned().collect()
    }

    fn is_empty(&self) -> bool {
        self.id_tracker.borrow().total_point_count() == 0
    }

    fn available_point_count(&self) -> usize {
        self.id_tracker.borrow().available_point_count()
    }

    fn deleted_point_count(&self) -> usize {
        self.id_tracker.borrow().deleted_point_count()
    }

    fn available_point_count_without_deferred(&self) -> usize {
        self.with_view(|view| view.available_point_count_without_deferred())
    }

    fn available_vectors_size_in_bytes(&self, vector_name: &VectorName) -> OperationResult<usize> {
        self.with_view(|view| view.available_vectors_size_in_bytes(vector_name))
    }

    fn segment_uuid(&self) -> Uuid {
        self.uuid
    }

    fn segment_type(&self) -> SegmentType {
        self.segment_type
    }

    fn info(&self) -> SegmentInfo {
        // A read-only segment is never appendable.
        self.with_view(|view| view.build_info(self.uuid, self.segment_type, false))
    }

    fn size_info(&self) -> SegmentInfo {
        // A read-only segment is never appendable.
        self.with_view(|view| view.build_size_info(self.uuid, self.segment_type, false))
    }

    fn config(&self) -> &SegmentConfig {
        &self.segment_config
    }

    fn is_appendable(&self) -> bool {
        false
    }

    fn get_indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema> {
        self.payload_index
            .borrow()
            .with_view(|v| v.indexed_fields())
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> SegmentTelemetry {
        self.with_view(|view| {
            // A read-only segment is never appendable.
            view.build_telemetry(
                self.uuid,
                self.segment_type,
                false,
                &self.segment_config,
                detail,
            )
        })
    }

    fn fill_query_context(&self, query_context: &mut QueryContext) -> OperationResult<()> {
        self.with_view(|view| view.fill_query_context(query_context))
    }

    fn point_is_deferred(&self, point_id: PointIdType) -> bool {
        self.with_view(|view| view.point_is_deferred(point_id))
    }

    fn deferred_point_ids(&self) -> Vec<PointIdType> {
        self.with_view(|view| view.deferred_point_ids())
    }

    fn deferred_point_count(&self) -> usize {
        self.with_view(|view| view.deferred_point_count())
    }

    fn has_deferred_points(&self) -> bool {
        self.with_view(|view| view.has_deferred_points())
    }
}
