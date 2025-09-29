use std::cmp;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::TelemetryDetail;
use segment::common::operation_error::{OperationResult, SegmentFailedState};
use segment::data_types::facets::{FacetParams, FacetValue};
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::order_by::OrderValue;
use segment::data_types::query_context::{FormulaContext, QueryContext, SegmentQueryContext};
use segment::data_types::vectors::{QueryVector, VectorInternal};
use segment::entry::read_entry_point::SegmentEntryRead;
use segment::index::field_index::CardinalityEstimation;
use segment::json_path::JsonPath;
use segment::telemetry::SegmentTelemetry;
use segment::types::*;

use super::{ProxyIndexChange, ProxySegment};
use crate::proxy_segment::proxy_read_guard::ProxyReadSegmentGuard;

impl<'b> SegmentEntryRead for ProxyReadSegmentGuard<'b> {
    fn version(&self) -> SeqNumberType {
        cmp::max(self.wrapped_guard.version(), self.write_guard.version())
    }

    fn point_version(&self, point_id: PointIdType) -> Option<SeqNumberType> {
        // Use write segment version if present, we assume it's always higher
        if let Some(version) = self.write_guard.point_version(point_id) {
            return Some(version);
        }

        // Use wrapped segment version, if absent we have no version at all
        let wrapped_version = self.wrapped_guard.point_version(point_id)?;

        // Ignore point from wrapped segment if already marked for deletion with newer version
        // By `point_version` semantics we don't expect to get a version if the point
        // is deleted. This also prevents `move_if_exists` from moving an old point
        // into the write segment again.
        if self
            .proxy
            .deleted_points
            .read()
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
        let deleted_points = self.proxy.deleted_points.read();

        // Some point might be deleted after temporary segment creation
        // We need to prevent them from being found by search request
        // That is why we need to pass additional filter for deleted points
        let do_update_filter = !deleted_points.is_empty();
        let mut wrapped_results = if do_update_filter {
            // If we are wrapping a segment with deleted points,
            // we can make this hack of replacing deleted_points of the wrapped_segment
            // with our proxied deleted_points, do avoid additional filter creation
            if let Some(deleted_points) = self.proxy.deleted_mask.as_ref() {
                let query_context_with_deleted =
                    query_context.fork().with_deleted_points(deleted_points);

                let res = self.wrapped_guard.search_batch(
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
                let wrapped_filter = ProxySegment::add_deleted_points_condition_to_filter(
                    filter,
                    deleted_points.keys().copied(),
                );

                self.wrapped_guard.search_batch(
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
            self.wrapped_guard.search_batch(
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
        let mut write_results = self.write_guard.search_batch(
            vector_name,
            vectors,
            with_payload,
            with_vector,
            filter,
            top,
            params,
            query_context,
        )?;
        for (index, write_result) in write_results.iter_mut().enumerate() {
            wrapped_results[index].append(write_result)
        }
        Ok(wrapped_results)
    }

    fn rescore_with_formula(
        &self,
        formula_ctx: Arc<FormulaContext>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<ScoredPoint>> {
        // Run rescore in wrapped segment
        let mut wrapped_results = self
            .wrapped_guard
            .rescore_with_formula(formula_ctx.clone(), hw_counter)?;

        // Run rescore in write segment
        let mut write_results = self
            .write_guard
            .rescore_with_formula(formula_ctx, hw_counter)?;

        {
            let deleted_points = self.proxy.deleted_points.read();
            if deleted_points.is_empty() {
                // Just join both results, they will be deduplicated and top-k'd later
                write_results.append(&mut wrapped_results);
            } else {
                for wrapped_result in wrapped_results {
                    if !deleted_points.contains_key(&wrapped_result.id) {
                        write_results.push(wrapped_result);
                    }
                }
            }
        }

        Ok(write_results)
    }

    fn vector(
        &self,
        vector_name: &VectorName,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<VectorInternal>> {
        if self.proxy.deleted_points.read().contains_key(&point_id) {
            self.write_guard.vector(vector_name, point_id, hw_counter)
        } else {
            {
                if self.write_guard.has_point(point_id) {
                    return self.write_guard.vector(vector_name, point_id, hw_counter);
                }
            }
            self.write_guard.vector(vector_name, point_id, hw_counter)
        }
    }

    fn all_vectors(
        &self,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<NamedVectors<'_>> {
        let mut result = NamedVectors::default();
        let config = self.wrapped_guard.config();
        let vector_names: Vec<_> = config
            .vector_data
            .keys()
            .chain(config.sparse_vector_data.keys())
            .cloned()
            .collect();

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
        if self.proxy.deleted_points.read().contains_key(&point_id) {
            self.write_guard.payload(point_id, hw_counter)
        } else {
            {
                if self.write_guard.has_point(point_id) {
                    return self.write_guard.payload(point_id, hw_counter);
                }
            }
            self.wrapped_guard.payload(point_id, hw_counter)
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
        let deleted_points = self.proxy.deleted_points.read();
        let mut read_points = if deleted_points.is_empty() {
            self.wrapped_guard
                .read_filtered(offset, limit, filter, is_stopped, hw_counter)
        } else {
            let wrapped_filter = ProxySegment::add_deleted_points_condition_to_filter(
                filter,
                deleted_points.keys().copied(),
            );
            self.wrapped_guard.read_filtered(
                offset,
                limit,
                Some(&wrapped_filter),
                is_stopped,
                hw_counter,
            )
        };
        let mut write_segment_points = self
            .write_guard
            .read_filtered(offset, limit, filter, is_stopped, hw_counter);
        read_points.append(&mut write_segment_points);
        read_points.sort_unstable();
        read_points.dedup();
        read_points
    }

    fn read_ordered_filtered<'a>(
        &'a self,
        limit: Option<usize>,
        filter: Option<&'a Filter>,
        order_by: &'a segment::data_types::order_by::OrderBy,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<(OrderValue, PointIdType)>> {
        let deleted_points = self.proxy.deleted_points.read();
        let mut read_points = if deleted_points.is_empty() {
            self.wrapped_guard
                .read_ordered_filtered(limit, filter, order_by, is_stopped, hw_counter)?
        } else {
            let wrapped_filter = ProxySegment::add_deleted_points_condition_to_filter(
                filter,
                deleted_points.keys().copied(),
            );
            self.wrapped_guard.read_ordered_filtered(
                limit,
                Some(&wrapped_filter),
                order_by,
                is_stopped,
                hw_counter,
            )?
        };
        let mut write_segment_points = self
            .write_guard
            .read_ordered_filtered(limit, filter, order_by, is_stopped, hw_counter)?;
        read_points.append(&mut write_segment_points);
        read_points.sort_unstable();
        read_points.dedup();
        Ok(read_points)
    }

    fn read_random_filtered<'a>(
        &'a self,
        limit: usize,
        filter: Option<&'a Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<PointIdType> {
        let deleted_points = self.proxy.deleted_points.read();
        let mut read_points = if deleted_points.is_empty() {
            self.wrapped_guard
                .read_random_filtered(limit, filter, is_stopped, hw_counter)
        } else {
            let wrapped_filter = ProxySegment::add_deleted_points_condition_to_filter(
                filter,
                deleted_points.keys().copied(),
            );
            self.wrapped_guard.read_random_filtered(
                limit,
                Some(&wrapped_filter),
                is_stopped,
                hw_counter,
            )
        };
        let mut write_segment_points = self
            .write_guard
            .read_random_filtered(limit, filter, is_stopped, hw_counter);
        read_points.append(&mut write_segment_points);
        read_points.sort_unstable();
        read_points.dedup();
        read_points
    }

    /// Read points in [from; to) range
    fn read_range(&self, from: Option<PointIdType>, to: Option<PointIdType>) -> Vec<PointIdType> {
        let deleted_points = self.proxy.deleted_points.read();
        let mut read_points = self.wrapped_guard.read_range(from, to);
        if !deleted_points.is_empty() {
            read_points.retain(|idx| !deleted_points.contains_key(idx))
        }
        let mut write_segment_points = self.write_guard.read_range(from, to);
        read_points.append(&mut write_segment_points);
        read_points.sort_unstable();
        read_points.dedup();
        read_points
    }

    fn unique_values(
        &self,
        key: &JsonPath,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BTreeSet<FacetValue>> {
        let mut values = self
            .wrapped_guard
            .unique_values(key, filter, is_stopped, hw_counter)?;

        values.extend(
            self.write_guard
                .unique_values(key, filter, is_stopped, hw_counter)?,
        );

        Ok(values)
    }

    fn facet(
        &self,
        request: &FacetParams,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        let deleted_points = self.proxy.deleted_points.read();
        let mut hits = if deleted_points.is_empty() {
            self.wrapped_guard.facet(request, is_stopped, hw_counter)?
        } else {
            let wrapped_filter = ProxySegment::add_deleted_points_condition_to_filter(
                request.filter.as_ref(),
                deleted_points.keys().copied(),
            );
            let new_request = FacetParams {
                filter: Some(wrapped_filter),
                ..request.clone()
            };
            self.wrapped_guard
                .facet(&new_request, is_stopped, hw_counter)?
        };

        let write_segment_hits = self.write_guard.facet(request, is_stopped, hw_counter)?;

        write_segment_hits
            .into_iter()
            .for_each(|(facet_value, count)| {
                *hits.entry(facet_value).or_insert(0) += count;
            });

        Ok(hits)
    }

    fn has_point(&self, point_id: PointIdType) -> bool {
        if self.proxy.deleted_points.read().contains_key(&point_id) {
            self.write_guard.has_point(point_id)
        } else {
            self.write_guard.has_point(point_id) || self.wrapped_guard.has_point(point_id)
        }
    }

    fn estimate_point_count<'a>(
        &'a self,
        filter: Option<&'a Filter>,
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        let deleted_point_count = self.proxy.deleted_points.read().len();

        let (wrapped_segment_est, total_wrapped_size) = {
            (
                self.wrapped_guard.estimate_point_count(filter, hw_counter),
                self.wrapped_guard.available_point_count(),
            )
        };

        let write_segment_est = self.write_guard.estimate_point_count(filter, hw_counter);

        let expected_deleted_count = if total_wrapped_size > 0 {
            (wrapped_segment_est.exp as f64
                * (deleted_point_count as f64 / total_wrapped_size as f64)) as usize
        } else {
            0
        };

        let primary_clauses =
            if wrapped_segment_est.primary_clauses == write_segment_est.primary_clauses {
                wrapped_segment_est.primary_clauses
            } else {
                vec![]
            };

        CardinalityEstimation {
            primary_clauses,
            min: wrapped_segment_est.min.saturating_sub(deleted_point_count)
                + write_segment_est.min,
            exp: (wrapped_segment_est.exp + write_segment_est.exp)
                .saturating_sub(expected_deleted_count),
            max: wrapped_segment_est.max + write_segment_est.max,
        }
    }

    fn vector_names(&self) -> HashSet<VectorNameBuf> {
        self.write_guard.vector_names()
    }

    fn is_empty(&self) -> bool {
        self.wrapped_guard.is_empty() && self.write_guard.is_empty()
    }

    fn available_point_count(&self) -> usize {
        let deleted_points_count = self.proxy.deleted_points.read().len();
        let wrapped_segment_count = self.wrapped_guard.available_point_count();
        let write_segment_count = self.write_guard.available_point_count();
        (wrapped_segment_count + write_segment_count).saturating_sub(deleted_points_count)
    }

    fn deleted_point_count(&self) -> usize {
        self.write_guard.deleted_point_count()
    }

    fn available_vectors_size_in_bytes(&self, vector_name: &VectorName) -> OperationResult<usize> {
        let wrapped_size = self
            .wrapped_guard
            .available_vectors_size_in_bytes(vector_name)?;
        let wrapped_count = self.wrapped_guard.available_point_count();

        let write_size = self
            .write_guard
            .available_vectors_size_in_bytes(vector_name)?;
        let write_count = self.write_guard.available_point_count();

        let stored_points = wrapped_count + write_count;
        // because we don't know the exact size of deleted vectors, we assume that they are the same avg size as the wrapped ones
        if stored_points > 0 {
            let deleted_points_count = self.proxy.deleted_points.read().len();
            let available_points = stored_points.saturating_sub(deleted_points_count);
            Ok(
                ((wrapped_size as u128 + write_size as u128) * available_points as u128
                    / stored_points as u128) as usize,
            )
        } else {
            Ok(0)
        }
    }

    fn segment_type(&self) -> SegmentType {
        SegmentType::Special
    }

    fn info(&self) -> SegmentInfo {
        let wrapped_info = self.wrapped_guard.info();
        let write_info = self.write_guard.size_info(); // Only fields set by `size_info()` needed!

        let vector_name_count =
            self.config().vector_data.len() + self.config().sparse_vector_data.len();
        let deleted_points_count = self.proxy.deleted_points.read().len();

        // This is a best estimate
        let num_vectors = (wrapped_info.num_vectors + write_info.num_vectors)
            .saturating_sub(deleted_points_count * vector_name_count);

        let num_indexed_vectors = if wrapped_info.segment_type == SegmentType::Indexed {
            wrapped_info
                .num_vectors
                .saturating_sub(deleted_points_count * vector_name_count)
        } else {
            0
        };

        let mut vector_data = wrapped_info.vector_data;

        for (key, info) in write_info.vector_data {
            vector_data
                .entry(key)
                .and_modify(|wrapped_info| {
                    wrapped_info.num_vectors += info.num_vectors;
                })
                .or_insert(info);
        }

        SegmentInfo {
            segment_type: SegmentType::Special,
            num_vectors,
            num_indexed_vectors,
            num_points: self.available_point_count(),
            num_deleted_vectors: write_info.num_deleted_vectors,
            vectors_size_bytes: wrapped_info.vectors_size_bytes, //  + write_info.vectors_size_bytes,
            payloads_size_bytes: wrapped_info.payloads_size_bytes,
            ram_usage_bytes: wrapped_info.ram_usage_bytes + write_info.ram_usage_bytes,
            disk_usage_bytes: wrapped_info.disk_usage_bytes + write_info.disk_usage_bytes,
            is_appendable: false,
            index_schema: wrapped_info.index_schema,
            vector_data,
        }
    }

    fn size_info(&self) -> SegmentInfo {
        // To reduce code complexity for estimations, we use `.info()` directly here.
        self.info()
    }

    fn config(&self) -> &SegmentConfig {
        &self.proxy.wrapped_config
    }

    fn is_appendable(&self) -> bool {
        true
    }

    fn flush(&self, sync: bool, force: bool) -> OperationResult<SeqNumberType> {
        let changed_indexes_guard = self.proxy.changed_indexes.read();
        let deleted_points_guard = self.proxy.deleted_points.read();

        let (wrapped_version, wrapped_persisted_version) = {
            let persisted_version = self.wrapped_guard.flush(sync, force)?;
            let version = self.wrapped_guard.version();
            (version, persisted_version)
        };

        // In case there are no updates happened to the proxy, this value can be
        // - either higher than wrapped segment (in case new updates were written to proxy as an appendable segment)
        // - or lower than wrapped segment (in case no updates were written to proxy, so the version is 0)
        let write_persisted_version = self.write_guard.flush(sync, force)?;

        // As soon as anything is written to the proxy, the max version of the proxy if fixed to
        // minimal of both versions. So we should never ack operation, which does copy-on-write.
        let is_all_empty = changed_indexes_guard.is_empty() && deleted_points_guard.is_empty();

        let flushed_version = if is_all_empty {
            // It might happen, that wrapped segment still has some data which is not flushed
            // Because we are going async flush and call to `.flush` doesn't guarantee that all data is already persisted
            // If this happens, we can't ack WAL based on write segment and should wait for wrapped segment to be flushed

            let wrapped_full_persisted = wrapped_persisted_version >= wrapped_version;

            if wrapped_full_persisted {
                debug_assert!(wrapped_persisted_version == wrapped_version);
                debug_assert!(
                    write_persisted_version == 0
                        || write_persisted_version >= wrapped_persisted_version
                );
                // This is the case, if **wrapped segment is fully persisted and will not be changed anymore**
                // Examples:
                // write_persisted_version = 0, wrapped_persisted_version = 10 -> flushed_version = 10
                // write_persisted_version = 15, wrapped_persisted_version = 10 -> flushed_version = 15
                // write_persisted_version = 7, wrapped_persisted_version = 10 -> flushed_version = 10 // should never happen
                cmp::max(write_persisted_version, wrapped_persisted_version)
            } else {
                // This is the case, if wrapped segment is not fully persisted yet
                // We should wait for it to be fully persisted.
                // At the same time "write_segment_version" can be either higher than wrapped_version
                // Or it can be 0, if no updates were written to proxy
                // In both cases it should be fine to return minimal of both.
                // Even if for the short of first period we would think that accepted version goes back to 0,
                // it shouldn't be a problem for was, as it will just stop ack WAL changes.
                // Examples:
                // write_persisted_version = 0, wrapped_persisted_version = 10 -> flushed_version = 0
                // write_persisted_version = 15, wrapped_persisted_version = 10 -> flushed_version = 10
                cmp::min(write_persisted_version, wrapped_persisted_version)
            }
        } else {
            cmp::min(write_persisted_version, wrapped_persisted_version)
        };

        Ok(flushed_version)
    }

    fn data_path(&self) -> PathBuf {
        self.wrapped_guard.data_path()
    }

    fn get_indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema> {
        let mut indexed_fields = self.wrapped_guard.get_indexed_fields();

        for (field_name, change) in self.proxy.changed_indexes.read().iter_unordered() {
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
        self.write_guard.check_error()
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> SegmentTelemetry {
        self.wrapped_guard.get_telemetry_data(detail)
    }

    fn fill_query_context(&self, query_context: &mut QueryContext) {
        // Information from temporary segment is not too important for query context
        self.wrapped_guard.fill_query_context(query_context)
    }
}
