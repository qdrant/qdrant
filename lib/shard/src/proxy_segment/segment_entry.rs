use std::cmp;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::TelemetryDetail;
use segment::common::Flusher;
use segment::common::operation_error::{OperationError, OperationResult, SegmentFailedState};
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

use super::{ProxyDeletedPoint, ProxyIndexChange, ProxySegment};
use crate::locked_segment::LockedSegment;
impl SegmentEntry for ProxySegment {
    fn version(&self) -> SeqNumberType {
        cmp::max(self.wrapped_segment.get().read().version(), self.version)
    }

    fn persistent_version(&self) -> SeqNumberType {
        self.wrapped_segment.get().read().persistent_version()
    }

    fn is_proxy(&self) -> bool {
        true
    }

    fn point_version(&self, point_id: PointIdType) -> Option<SeqNumberType> {
        // Use wrapped segment version, if absent we have no version at all
        let wrapped_version = self.wrapped_segment.get().read().point_version(point_id)?;

        // Ignore point from wrapped segment if already marked for deletion with newer version
        // By `point_version` semantics we don't expect to get a version if the point
        // is deleted. This also prevents `move_if_exists` from moving an old point
        // into the write segment again.
        if self
            .deleted_points
            .get(&point_id)
            .is_some_and(|delete| wrapped_version <= delete.local_version)
        {
            return None;
        }

        Some(wrapped_version)
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
        // Some point might be deleted after temporary segment creation
        // We need to prevent them from being found by search request
        // That is why we need to pass additional filter for deleted points
        let do_update_filter = !self.deleted_points.is_empty();
        let wrapped_results = if do_update_filter {
            // If we are wrapping a segment with deleted points,
            // we can make this hack of replacing deleted_points of the wrapped_segment
            // with our proxied deleted_points, do avoid additional filter creation
            if let Some(deleted_points) = self.deleted_mask.as_ref() {
                let query_context_with_deleted =
                    query_context.fork().with_deleted_points(deleted_points);

                let res = self.wrapped_segment.get().read().search_batch(
                    vector_name,
                    vectors,
                    with_payload,
                    with_vector,
                    filter,
                    top,
                    params,
                    &query_context_with_deleted,
                );

                res?
            } else {
                let wrapped_filter = Self::add_deleted_points_condition_to_filter(
                    filter,
                    self.deleted_points.keys().copied(),
                );

                self.wrapped_segment.get().read().search_batch(
                    vector_name,
                    vectors,
                    with_payload,
                    with_vector,
                    Some(&wrapped_filter),
                    top,
                    params,
                    query_context,
                )?
            }
        } else {
            self.wrapped_segment.get().read().search_batch(
                vector_name,
                vectors,
                with_payload,
                with_vector,
                filter,
                top,
                params,
                query_context,
            )?
        };
        Ok(wrapped_results)
    }

    fn rescore_with_formula(
        &self,
        formula_ctx: Arc<FormulaContext>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<ScoredPoint>> {
        // Run rescore in wrapped segment
        let wrapped_results = self
            .wrapped_segment
            .get()
            .read()
            .rescore_with_formula(formula_ctx.clone(), hw_counter)?;

        let result = {
            if self.deleted_points.is_empty() {
                wrapped_results
            } else {
                wrapped_results
                    .into_iter()
                    .filter(|point| !self.deleted_points.contains_key(&point.id))
                    .collect()
            }
        };

        Ok(result)
    }

    fn upsert_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        _vectors: NamedVectors,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        Err(OperationError::service_error(format!(
            "Upsert is disabled for proxy segments: operation {op_num} on point {point_id}",
        )))
    }

    fn delete_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        let mut was_deleted = false;

        self.version = cmp::max(self.version, op_num);

        let point_offset = match &self.wrapped_segment {
            LockedSegment::Original(raw_segment) => {
                let point_offset = raw_segment.read().get_internal_id(point_id);
                if point_offset.is_some() {
                    let prev = self.deleted_points.insert(
                        point_id,
                        ProxyDeletedPoint {
                            local_version: op_num,
                            operation_version: op_num,
                        },
                    );
                    was_deleted = prev.is_none();
                    if let Some(prev) = prev {
                        debug_assert!(
                            prev.operation_version < op_num,
                            "Overriding deleted flag {prev:?} with older op_num:{op_num}",
                        )
                    }
                }
                point_offset
            }
            LockedSegment::Proxy(proxy) => {
                if proxy.read().has_point(point_id) {
                    let prev = self.deleted_points.insert(
                        point_id,
                        ProxyDeletedPoint {
                            local_version: op_num,
                            operation_version: op_num,
                        },
                    );
                    was_deleted = prev.is_none();
                    if let Some(prev) = prev {
                        debug_assert!(
                            prev.operation_version < op_num,
                            "Overriding deleted flag {prev:?} with older op_num:{op_num}",
                        )
                    }
                }
                None
            }
        };

        self.set_deleted_offset(point_offset);

        Ok(was_deleted)
    }

    fn update_vectors(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        _vectors: NamedVectors,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        Err(OperationError::service_error(format!(
            "Update vectors is disabled for proxy segments: operation {op_num} on point {point_id}",
        )))
    }

    fn delete_vector(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        _vector_name: &VectorName,
    ) -> OperationResult<bool> {
        // Print current stack trace for easier debugging of unexpected calls
        Err(OperationError::service_error(format!(
            "Delete vector is disabled for proxy segments: operation {op_num} on point {point_id}",
        )))
    }

    fn set_full_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        _full_payload: &Payload,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        Err(OperationError::service_error(format!(
            "Set full payload is disabled for proxy segments: operation {op_num} on point {point_id}",
        )))
    }

    fn set_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        _payload: &Payload,
        _key: &Option<JsonPath>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        Err(OperationError::service_error(format!(
            "Set payload is disabled for proxy segments: operation {op_num} on point {point_id}",
        )))
    }

    fn delete_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        _key: PayloadKeyTypeRef,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        Err(OperationError::service_error(format!(
            "Delete payload is disabled for proxy segments: operation {op_num} on point {point_id}",
        )))
    }

    fn clear_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        Err(OperationError::service_error(format!(
            "Clear payload is disabled for proxy segments: operation {op_num} on point {point_id}",
        )))
    }

    fn vector(
        &self,
        vector_name: &VectorName,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<VectorInternal>> {
        if self.deleted_points.contains_key(&point_id) {
            Ok(None)
        } else {
            self.wrapped_segment
                .get()
                .read()
                .vector(vector_name, point_id, hw_counter)
        }
    }

    fn all_vectors(
        &self,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<NamedVectors<'_>> {
        let mut result = NamedVectors::default();
        let wrapped = self.wrapped_segment.get();
        let wrapped_guard = wrapped.read();
        let config = wrapped_guard.config();
        let vector_names: Vec<_> = config
            .vector_data
            .keys()
            .chain(config.sparse_vector_data.keys())
            .cloned()
            .collect();

        // Must drop wrapped guard to prevent self-deadlock in `vector()` function below
        drop(wrapped_guard);

        for vector_name in vector_names {
            if let Some(vector) = self.vector(&vector_name, point_id, hw_counter)? {
                result.insert(vector_name, vector);
            }
        }
        Ok(result)
    }

    fn payload(
        &self,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        if self.deleted_points.contains_key(&point_id) {
            Ok(Payload::default())
        } else {
            self.wrapped_segment
                .get()
                .read()
                .payload(point_id, hw_counter)
        }
    }

    /// Not implemented for proxy
    fn iter_points(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        // iter_points is not available for Proxy implementation
        // Due to internal locks it is almost impossible to return iterator with proper owning, lifetimes, e.t.c.
        unimplemented!("call to iter_points is not implemented for Proxy segment")
    }

    fn read_filtered<'a>(
        &'a self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        filter: Option<&'a Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<PointIdType> {
        if self.deleted_points.is_empty() {
            self.wrapped_segment
                .get()
                .read()
                .read_filtered(offset, limit, filter, is_stopped, hw_counter)
        } else {
            let wrapped_filter = Self::add_deleted_points_condition_to_filter(
                filter,
                self.deleted_points.keys().copied(),
            );
            self.wrapped_segment.get().read().read_filtered(
                offset,
                limit,
                Some(&wrapped_filter),
                is_stopped,
                hw_counter,
            )
        }
    }

    fn read_ordered_filtered<'a>(
        &'a self,
        limit: Option<usize>,
        filter: Option<&'a Filter>,
        order_by: &'a segment::data_types::order_by::OrderBy,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<(OrderValue, PointIdType)>> {
        let read_points = if self.deleted_points.is_empty() {
            self.wrapped_segment
                .get()
                .read()
                .read_ordered_filtered(limit, filter, order_by, is_stopped, hw_counter)?
        } else {
            let wrapped_filter = Self::add_deleted_points_condition_to_filter(
                filter,
                self.deleted_points.keys().copied(),
            );
            self.wrapped_segment.get().read().read_ordered_filtered(
                limit,
                Some(&wrapped_filter),
                order_by,
                is_stopped,
                hw_counter,
            )?
        };
        Ok(read_points)
    }

    fn read_random_filtered<'a>(
        &'a self,
        limit: usize,
        filter: Option<&'a Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<PointIdType> {
        if self.deleted_points.is_empty() {
            self.wrapped_segment
                .get()
                .read()
                .read_random_filtered(limit, filter, is_stopped, hw_counter)
        } else {
            let wrapped_filter = Self::add_deleted_points_condition_to_filter(
                filter,
                self.deleted_points.keys().copied(),
            );
            self.wrapped_segment.get().read().read_random_filtered(
                limit,
                Some(&wrapped_filter),
                is_stopped,
                hw_counter,
            )
        }
    }

    /// Read points in [from; to) range
    fn read_range(&self, from: Option<PointIdType>, to: Option<PointIdType>) -> Vec<PointIdType> {
        let read_points = self.wrapped_segment.get().read().read_range(from, to);
        if self.deleted_points.is_empty() {
            read_points
        } else {
            read_points
                .into_iter()
                .filter(|idx| !self.deleted_points.contains_key(idx))
                .collect()
        }
    }

    fn unique_values(
        &self,
        key: &JsonPath,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BTreeSet<FacetValue>> {
        let values = self
            .wrapped_segment
            .get()
            .read()
            .unique_values(key, filter, is_stopped, hw_counter)?;
        Ok(values)
    }

    fn facet(
        &self,
        request: &FacetParams,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        let hits = if self.deleted_points.is_empty() {
            self.wrapped_segment
                .get()
                .read()
                .facet(request, is_stopped, hw_counter)?
        } else {
            let wrapped_filter = Self::add_deleted_points_condition_to_filter(
                request.filter.as_ref(),
                self.deleted_points.keys().copied(),
            );
            let new_request = FacetParams {
                filter: Some(wrapped_filter),
                ..request.clone()
            };
            self.wrapped_segment
                .get()
                .read()
                .facet(&new_request, is_stopped, hw_counter)?
        };

        Ok(hits)
    }

    fn has_point(&self, point_id: PointIdType) -> bool {
        !self.deleted_points.contains_key(&point_id)
            && self.wrapped_segment.get().read().has_point(point_id)
    }

    fn is_empty(&self) -> bool {
        self.wrapped_segment.get().read().is_empty()
    }

    fn available_point_count(&self) -> usize {
        let deleted_points_count = self.deleted_points.len();
        let wrapped_segment_count = self.wrapped_segment.get().read().available_point_count();
        wrapped_segment_count.saturating_sub(deleted_points_count)
    }

    fn deleted_point_count(&self) -> usize {
        self.wrapped_segment.get().read().deleted_point_count() + self.deleted_points.len()
    }

    fn available_vectors_size_in_bytes(&self, vector_name: &VectorName) -> OperationResult<usize> {
        let wrapped_segment = self.wrapped_segment.get();
        let wrapped_segment_guard = wrapped_segment.read();
        let wrapped_size = wrapped_segment_guard.available_vectors_size_in_bytes(vector_name)?;
        let wrapped_count = wrapped_segment_guard.available_point_count();
        drop(wrapped_segment_guard);

        let stored_points = wrapped_count;
        // because we don't know the exact size of deleted vectors, we assume that they are the same avg size as the wrapped ones
        if stored_points > 0 {
            let deleted_points_count = self.deleted_points.len();
            let available_points = stored_points.saturating_sub(deleted_points_count);
            Ok(
                ((wrapped_size as u128) * available_points as u128 / stored_points as u128)
                    as usize,
            )
        } else {
            Ok(0)
        }
    }

    fn estimate_point_count<'a>(
        &'a self,
        filter: Option<&'a Filter>,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        let deleted_point_count = self.deleted_points.len();

        let (wrapped_segment_est, total_wrapped_size) = {
            let wrapped_segment = self.wrapped_segment.get();
            let wrapped_segment_guard = wrapped_segment.read();
            (
                wrapped_segment_guard.estimate_point_count(filter, hw_counter),
                wrapped_segment_guard.available_point_count(),
            )
        };

        let expected_deleted_count = if total_wrapped_size > 0 {
            (wrapped_segment_est.exp as f64
                * (deleted_point_count as f64 / total_wrapped_size as f64)) as usize
        } else {
            0
        };

        let CardinalityEstimation {
            primary_clauses,
            min,
            exp,
            max,
        } = wrapped_segment_est;

        CardinalityEstimation {
            primary_clauses,
            min: min.saturating_sub(deleted_point_count),
            exp: exp.saturating_sub(expected_deleted_count),
            max,
        }
    }

    fn segment_type(&self) -> SegmentType {
        SegmentType::Special
    }

    fn size_info(&self) -> SegmentInfo {
        // To reduce code complexity for estimations, we use `.info()` directly here.
        self.info()
    }

    fn info(&self) -> SegmentInfo {
        let wrapped_info = self.wrapped_segment.get().read().info();

        let vector_name_count =
            self.config().vector_data.len() + self.config().sparse_vector_data.len();
        let deleted_points_count = self.deleted_points.len();

        // This is a best estimate
        let num_vectors = wrapped_info
            .num_vectors
            .saturating_sub(deleted_points_count * vector_name_count);

        let num_indexed_vectors = if wrapped_info.segment_type == SegmentType::Indexed {
            wrapped_info
                .num_vectors
                .saturating_sub(deleted_points_count * vector_name_count)
        } else {
            0
        };

        let vector_data = wrapped_info.vector_data;

        SegmentInfo {
            segment_type: SegmentType::Special,
            num_vectors,
            num_indexed_vectors,
            num_points: self.available_point_count(),
            num_deleted_vectors: wrapped_info.num_deleted_vectors
                + deleted_points_count * vector_name_count,
            vectors_size_bytes: wrapped_info.vectors_size_bytes, //  + write_info.vectors_size_bytes,
            payloads_size_bytes: wrapped_info.payloads_size_bytes,
            ram_usage_bytes: wrapped_info.ram_usage_bytes,
            disk_usage_bytes: wrapped_info.disk_usage_bytes,
            is_appendable: false,
            index_schema: wrapped_info.index_schema,
            vector_data,
        }
    }

    fn config(&self) -> &SegmentConfig {
        &self.wrapped_config
    }

    fn is_appendable(&self) -> bool {
        false
    }

    fn flusher(&self, force: bool) -> Option<Flusher> {
        let wrapped_segment = self.wrapped_segment.get();
        let wrapped_segment_guard = wrapped_segment.read();
        wrapped_segment_guard.flusher(force)
    }

    fn drop_data(self) -> OperationResult<()> {
        self.wrapped_segment.drop_data()
    }

    fn data_path(&self) -> PathBuf {
        self.wrapped_segment.get().read().data_path()
    }

    fn delete_field_index(&mut self, op_num: u64, key: PayloadKeyTypeRef) -> OperationResult<bool> {
        if self.version() > op_num {
            return Ok(false);
        }

        self.version = cmp::max(self.version, op_num);

        // Store index change to later propagate to optimized/wrapped segment
        self.changed_indexes
            .insert(key.clone(), ProxyIndexChange::Delete(op_num));

        Ok(true)
    }

    fn delete_field_index_if_incompatible(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
        field_schema: &PayloadFieldSchema,
    ) -> OperationResult<bool> {
        if self.version() > op_num {
            return Ok(false);
        }

        self.version = cmp::max(self.version, op_num);

        self.changed_indexes.insert(
            key.clone(),
            ProxyIndexChange::DeleteIfIncompatible(op_num, field_schema.clone()),
        );

        Ok(true)
    }

    fn build_field_index(
        &self,
        op_num: SeqNumberType,
        _key: PayloadKeyTypeRef,
        field_type: &PayloadFieldSchema,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BuildFieldIndexResult> {
        if self.version() > op_num {
            return Ok(BuildFieldIndexResult::SkippedByVersion);
        }

        Ok(BuildFieldIndexResult::Built {
            indexes: vec![], // No actual index is built in proxy segment, they will be created later
            schema: field_type.clone(),
        })
    }

    fn apply_field_index(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyType,
        field_schema: PayloadFieldSchema,
        _field_index: Vec<FieldIndex>,
    ) -> OperationResult<bool> {
        if self.version() > op_num {
            return Ok(false);
        }

        self.version = cmp::max(self.version, op_num);

        // Store index change to later propagate to optimized/wrapped segment
        self.changed_indexes
            .insert(key.clone(), ProxyIndexChange::Create(field_schema, op_num));

        Ok(true)
    }

    fn get_indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema> {
        let mut indexed_fields = self.wrapped_segment.get().read().get_indexed_fields();

        for (field_name, change) in self.changed_indexes.iter_unordered() {
            match change {
                ProxyIndexChange::Create(schema, _) => {
                    indexed_fields.insert(field_name.to_owned(), schema.to_owned());
                }
                ProxyIndexChange::Delete(_) => {
                    indexed_fields.remove(field_name);
                }
                ProxyIndexChange::DeleteIfIncompatible(_, schema) => {
                    if let Some(existing_schema) = indexed_fields.get(field_name)
                        && existing_schema != schema
                    {
                        indexed_fields.remove(field_name);
                    }
                }
            }
        }

        indexed_fields
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
        // Information from temporary segment is not too important for query context
        self.wrapped_segment
            .get()
            .read()
            .fill_query_context(query_context)
    }
}
