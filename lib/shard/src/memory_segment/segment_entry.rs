use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::TelemetryDetail;
use segment::common::operation_error::{OperationResult, SegmentFailedState};
use segment::data_types::build_index_result::BuildFieldIndexResult;
use segment::data_types::facets::{FacetParams, FacetValue};
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::order_by::OrderValue;
use segment::data_types::query_context::{FormulaContext, QueryContext, SegmentQueryContext};
use segment::data_types::vectors::{QueryVector, VectorInternal};
use segment::entry::entry_point::SegmentEntry;
use segment::index::field_index::{CardinalityEstimation, FieldIndex};
use segment::json_path::JsonPath;
use segment::telemetry::SegmentTelemetry;
use segment::types::*;

use super::MemorySegment;

impl SegmentEntry for MemorySegment {
    fn version(&self) -> SeqNumberType {
        self.wrapped_segment.get().read().version()
    }

    fn point_version(&self, point_id: PointIdType) -> Option<SeqNumberType> {
        self.wrapped_segment.get().read().point_version(point_id)
    }

    fn search_batch(
        &self,
        vector_name: &VectorName,
        vectors: &[&QueryVector],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &SegmentQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        self.wrapped_segment.get().read().search_batch(
            vector_name,
            vectors,
            with_payload,
            with_vector,
            filter,
            top,
            params,
            query_context,
        )
    }

    fn rescore_with_formula(
        &self,
        formula_ctx: Arc<FormulaContext>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<ScoredPoint>> {
        self.wrapped_segment
            .get()
            .read()
            .rescore_with_formula(formula_ctx, hw_counter)
    }

    fn upsert_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vectors: NamedVectors,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        self.wrapped_segment.get().write().upsert_point(
            op_num,
            point_id,
            vectors.clone(),
            hw_counter,
        )
    }

    fn delete_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        // TODO: also adjust list of local deletes
        self.wrapped_segment
            .get()
            .write()
            .delete_point(op_num, point_id, hw_counter)
    }

    fn update_vectors(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vectors: NamedVectors,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        self.wrapped_segment.get().write().update_vectors(
            op_num,
            point_id,
            vectors.clone(),
            hw_counter,
        )
    }

    fn delete_vector(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vector_name: &VectorName,
    ) -> OperationResult<bool> {
        self.wrapped_segment
            .get()
            .write()
            .delete_vector(op_num, point_id, vector_name)
    }

    fn set_full_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        full_payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        self.wrapped_segment.get().write().set_full_payload(
            op_num,
            point_id,
            full_payload,
            hw_counter,
        )
    }

    fn set_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        payload: &Payload,
        key: &Option<JsonPath>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        self.wrapped_segment
            .get()
            .write()
            .set_payload(op_num, point_id, payload, key, hw_counter)
    }

    fn delete_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        key: PayloadKeyTypeRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        self.wrapped_segment
            .get()
            .write()
            .delete_payload(op_num, point_id, key, hw_counter)
    }

    fn clear_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        self.wrapped_segment
            .get()
            .write()
            .clear_payload(op_num, point_id, hw_counter)
    }

    fn vector(
        &self,
        vector_name: &VectorName,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<VectorInternal>> {
        self.wrapped_segment
            .get()
            .read()
            .vector(vector_name, point_id, hw_counter)
    }

    fn all_vectors(
        &self,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<NamedVectors<'_>> {
        self.wrapped_segment
            .get()
            .read()
            .all_vectors(point_id, hw_counter)
            .map(|vectors| NamedVectors::from_map(vectors.into_owned_map()))
    }

    fn payload(
        &self,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        self.wrapped_segment
            .get()
            .read()
            .payload(point_id, hw_counter)
    }

    /// Not implemented for memory
    fn iter_points(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        // iter_points is not available for memory implementation
        // Due to internal locks it is almost impossible to return iterator with proper owning, lifetimes, e.t.c.
        unimplemented!("call to iter_points is not implemented for memory segment")
    }

    fn read_filtered<'a>(
        &'a self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        filter: Option<&'a Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<PointIdType> {
        self.wrapped_segment
            .get()
            .read()
            .read_filtered(offset, limit, filter, is_stopped, hw_counter)
    }

    fn read_ordered_filtered<'a>(
        &'a self,
        limit: Option<usize>,
        filter: Option<&'a Filter>,
        order_by: &'a segment::data_types::order_by::OrderBy,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<(OrderValue, PointIdType)>> {
        self.wrapped_segment
            .get()
            .read()
            .read_ordered_filtered(limit, filter, order_by, is_stopped, hw_counter)
    }

    fn read_random_filtered<'a>(
        &'a self,
        limit: usize,
        filter: Option<&'a Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<PointIdType> {
        self.wrapped_segment
            .get()
            .read()
            .read_random_filtered(limit, filter, is_stopped, hw_counter)
    }

    /// Read points in [from; to) range
    fn read_range(&self, from: Option<PointIdType>, to: Option<PointIdType>) -> Vec<PointIdType> {
        self.wrapped_segment.get().read().read_range(from, to)
    }

    fn unique_values(
        &self,
        key: &JsonPath,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BTreeSet<FacetValue>> {
        self.wrapped_segment
            .get()
            .read()
            .unique_values(key, filter, is_stopped, hw_counter)
    }

    fn facet(
        &self,
        request: &FacetParams,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        self.wrapped_segment
            .get()
            .read()
            .facet(request, is_stopped, hw_counter)
    }

    fn has_point(&self, point_id: PointIdType) -> bool {
        self.wrapped_segment.get().read().has_point(point_id)
    }

    fn is_empty(&self) -> bool {
        self.wrapped_segment.get().read().is_empty()
    }

    fn available_point_count(&self) -> usize {
        self.wrapped_segment.get().read().available_point_count()
    }

    fn deleted_point_count(&self) -> usize {
        self.wrapped_segment.get().read().deleted_point_count()
    }

    fn available_vectors_size_in_bytes(&self, vector_name: &VectorName) -> OperationResult<usize> {
        self.wrapped_segment
            .get()
            .read()
            .available_vectors_size_in_bytes(vector_name)
    }

    fn estimate_point_count<'a>(
        &'a self,
        filter: Option<&'a Filter>,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        self.wrapped_segment
            .get()
            .read()
            .estimate_point_count(filter, hw_counter)
    }

    fn segment_type(&self) -> SegmentType {
        SegmentType::Special
    }

    fn size_info(&self) -> SegmentInfo {
        // To reduce code complexity for estimations, we use `.info()` directly here.
        self.info()
    }

    fn info(&self) -> SegmentInfo {
        self.wrapped_segment.get().read().info()
    }

    fn config(&self) -> &SegmentConfig {
        &self.config
    }

    fn is_appendable(&self) -> bool {
        true
    }

    fn flush(&self, _sync: bool, _force: bool) -> OperationResult<SeqNumberType> {
        Ok(self.persisted_version)
    }

    fn drop_data(self) -> OperationResult<()> {
        // TODO: explicitly drop data here?
        Ok(())
    }

    fn data_path(&self) -> PathBuf {
        self.wrapped_segment.get().read().data_path()
    }

    fn delete_field_index(&mut self, op_num: u64, key: PayloadKeyTypeRef) -> OperationResult<bool> {
        self.wrapped_segment
            .get()
            .write()
            .delete_field_index(op_num, key)
    }

    fn delete_field_index_if_incompatible(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
        field_schema: &PayloadFieldSchema,
    ) -> OperationResult<bool> {
        self.wrapped_segment
            .get()
            .write()
            .delete_field_index_if_incompatible(op_num, key, field_schema)
    }

    fn build_field_index(
        &self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
        field_type: &PayloadFieldSchema,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BuildFieldIndexResult> {
        self.wrapped_segment
            .get()
            .read()
            .build_field_index(op_num, key, field_type, hw_counter)
    }

    fn apply_field_index(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyType,
        field_schema: PayloadFieldSchema,
        field_index: Vec<FieldIndex>,
    ) -> OperationResult<bool> {
        self.wrapped_segment
            .get()
            .write()
            .apply_field_index(op_num, key, field_schema, field_index)
    }

    fn get_indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema> {
        self.wrapped_segment.get().read().get_indexed_fields()
    }

    fn check_error(&self) -> Option<SegmentFailedState> {
        self.wrapped_segment.get().read().check_error()
    }

    fn vector_names(&self) -> HashSet<VectorNameBuf> {
        self.wrapped_segment.get().read().vector_names()
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> SegmentTelemetry {
        self.wrapped_segment.get().read().get_telemetry_data(detail)
    }

    fn fill_query_context(&self, query_context: &mut QueryContext) {
        self.wrapped_segment
            .get()
            .read()
            .fill_query_context(query_context)
    }
}
