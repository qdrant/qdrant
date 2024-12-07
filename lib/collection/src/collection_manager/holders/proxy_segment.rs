use std::cmp;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bitvec::prelude::BitVec;
use common::tar_ext;
use common::types::{PointOffsetType, TelemetryDetail};
use itertools::Itertools;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use segment::common::operation_error::{OperationResult, SegmentFailedState};
use segment::data_types::facets::{FacetParams, FacetValue};
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::order_by::OrderValue;
use segment::data_types::query_context::{QueryContext, SegmentQueryContext};
use segment::data_types::vectors::{QueryVector, VectorInternal};
use segment::entry::entry_point::SegmentEntry;
use segment::index::field_index::{CardinalityEstimation, FieldIndex};
use segment::json_path::JsonPath;
use segment::telemetry::SegmentTelemetry;
use segment::types::{
    Condition, Filter, HasIdCondition, Payload, PayloadFieldSchema, PayloadKeyType,
    PayloadKeyTypeRef, PointIdType, ScoredPoint, SearchParams, SegmentConfig, SegmentInfo,
    SegmentType, SeqNumberType, SnapshotFormat, WithPayload, WithVector,
};

use crate::collection_manager::holders::segment_holder::LockedSegment;

pub type LockedRmSet = Arc<RwLock<HashMap<PointIdType, ProxyDeletedPoint>>>;
pub type LockedIndexChanges = Arc<RwLock<ProxyIndexChanges>>;

/// This object is a wrapper around read-only segment.
///
/// It could be used to provide all read and write operations while wrapped segment is being optimized (i.e. not available for writing)
/// It writes all changed records into a temporary `write_segment` and keeps track on changed points
#[derive(Debug)]
pub struct ProxySegment {
    pub write_segment: LockedSegment,
    pub wrapped_segment: LockedSegment,
    /// Internal mask of deleted points, specific to the wrapped segment
    /// Present if the wrapped segment is a plain segment
    /// Used for faster deletion checks
    deleted_mask: Option<BitVec>,
    changed_indexes: LockedIndexChanges,
    /// Points which should no longer used from wrapped_segment
    /// May contain points which are not in wrapped_segment,
    /// because the set is shared among all proxy segments
    deleted_points: LockedRmSet,
    wrapped_config: SegmentConfig,
}

impl ProxySegment {
    pub fn new(
        segment: LockedSegment,
        write_segment: LockedSegment,
        deleted_points: LockedRmSet,
        changed_indexes: LockedIndexChanges,
    ) -> Self {
        let deleted_mask = match &segment {
            LockedSegment::Original(raw_segment) => {
                let raw_segment_guard = raw_segment.read();
                let already_deleted = raw_segment_guard.get_deleted_points_bitvec();
                Some(already_deleted)
            }
            LockedSegment::Proxy(_) => None,
        };
        let wrapped_config = segment.get().read().config().clone();
        ProxySegment {
            write_segment,
            wrapped_segment: segment,
            deleted_mask,
            changed_indexes,
            deleted_points,
            wrapped_config,
        }
    }

    /// Ensure that write segment have same indexes as wrapped segment
    pub fn replicate_field_indexes(&mut self, op_num: SeqNumberType) -> OperationResult<()> {
        let existing_indexes = self.write_segment.get().read().get_indexed_fields();
        let expected_indexes = self.wrapped_segment.get().read().get_indexed_fields();
        // create missing indexes
        for (expected_field, expected_schema) in &expected_indexes {
            let existing_schema = existing_indexes.get(expected_field);

            if existing_schema != Some(expected_schema) {
                if existing_schema.is_some() {
                    self.write_segment
                        .get()
                        .write()
                        .delete_field_index(op_num, expected_field)?;
                }
                self.write_segment.get().write().create_field_index(
                    op_num,
                    expected_field,
                    Some(expected_schema),
                )?;
            }
        }
        // remove extra indexes
        for existing_field in existing_indexes.keys() {
            if !expected_indexes.contains_key(existing_field) {
                self.write_segment
                    .get()
                    .write()
                    .delete_field_index(op_num, existing_field)?;
            }
        }
        Ok(())
    }

    /// Updates the deleted mask with the given point offset
    /// Ensures that the mask is resized if necessary and returns false
    /// if either the mask or the point offset is missing (mask is not applicable)
    fn set_deleted_offset(&mut self, point_offset: Option<PointOffsetType>) -> bool {
        match (&mut self.deleted_mask, point_offset) {
            (Some(deleted_mask), Some(point_offset)) => {
                if deleted_mask.len() <= point_offset as usize {
                    deleted_mask.resize(point_offset as usize + 1, false);
                }
                deleted_mask.set(point_offset as usize, true);
                true
            }
            _ => false,
        }
    }

    fn move_if_exists(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        let deleted_points_guard = self.deleted_points.upgradable_read();

        let (point_offset, local_version) = {
            let (wrapped_segment, point_offset): (
                Arc<RwLock<dyn SegmentEntry>>,
                Option<PointOffsetType>,
            ) = match &self.wrapped_segment {
                LockedSegment::Original(raw_segment) => {
                    let point_offset = raw_segment.read().get_internal_id(point_id);
                    (raw_segment.clone(), point_offset)
                }
                LockedSegment::Proxy(sub_proxy) => (sub_proxy.clone(), None),
            };

            let wrapped_segment_guard = wrapped_segment.read();

            // Since `deleted_points` are shared between multiple ProxySegments,
            // It is possible that some other Proxy moved its point with different version already
            // If this is the case, there are multiple scenarios:
            // - Local point doesn't exist or already removed locally -> do nothing
            // - Already moved version is higher than the current one -> mark local as removed
            // - Already moved version is less than what we have in current proxy -> overwrite

            // Point doesn't exist in wrapped segment - do nothing
            let Some(local_version) = wrapped_segment_guard.point_version(point_id) else {
                return Ok(false);
            };

            // Equal or higher point version is already moved into write segment - delete from
            // wrapped segment and do not move it again
            if deleted_points_guard
                .get(&point_id)
                .is_some_and(|&deleted| deleted.local_version >= local_version)
            {
                drop(deleted_points_guard);
                self.set_deleted_offset(point_offset);
                return Ok(false);
            }

            let (all_vectors, payload) = (
                wrapped_segment_guard.all_vectors(point_id)?,
                wrapped_segment_guard.payload(point_id)?,
            );

            {
                let segment_arc = self.write_segment.get();
                let mut write_segment = segment_arc.write();

                write_segment.upsert_point(op_num, point_id, all_vectors)?;
                if !payload.is_empty() {
                    write_segment.set_full_payload(op_num, point_id, &payload)?;
                }
            };

            (point_offset, local_version)
        };

        {
            let mut deleted_points_write = RwLockUpgradableReadGuard::upgrade(deleted_points_guard);
            deleted_points_write.insert(
                point_id,
                ProxyDeletedPoint {
                    local_version,
                    operation_version: op_num,
                },
            );
        }

        self.set_deleted_offset(point_offset);

        Ok(true)
    }

    fn add_deleted_points_condition_to_filter(
        filter: Option<&Filter>,
        deleted_points: impl IntoIterator<Item = PointIdType>,
    ) -> Filter {
        #[allow(clippy::from_iter_instead_of_collect)]
        let wrapper_condition = Condition::HasId(HasIdCondition::from_iter(deleted_points));
        match filter {
            None => Filter::new_must_not(wrapper_condition),
            Some(f) => {
                let mut new_filter = f.clone();
                let must_not = new_filter.must_not;

                let new_must_not = match must_not {
                    None => Some(vec![wrapper_condition]),
                    Some(mut conditions) => {
                        conditions.push(wrapper_condition);
                        Some(conditions)
                    }
                };
                new_filter.must_not = new_must_not;
                new_filter
            }
        }
    }

    /// Propagate changes in this proxy to the wrapped segment
    ///
    /// This propagates:
    /// - delete (or moved) points
    /// - deleted payload indexes
    /// - created payload indexes
    ///
    /// This is required if making both the wrapped segment and the writable segment available in a
    /// shard holder at the same time. If the wrapped segment is thrown away, then this is not
    /// required.
    pub(super) fn propagate_to_wrapped(&self) -> OperationResult<()> {
        // Important: we must not keep a write lock on the wrapped segment for the duration of this
        // function to prevent a deadlock. The search functions conflict with it trying to take a
        // read lock on the wrapped segment as well while already holding the deleted points lock
        // (or others). Careful locking management is very important here. Instead we just take an
        // upgradable read lock, upgrading to a write lock on demand.
        // See: <https://github.com/qdrant/qdrant/pull/4206>
        let wrapped_segment = self.wrapped_segment.get();
        let mut wrapped_segment = wrapped_segment.upgradable_read();
        let op_num = wrapped_segment.version();

        // Propagate index changes before point deletions
        // Point deletions bump the segment version, can cause index changes to be ignored
        // Lock ordering is important here and must match the flush function to prevent a deadlock
        {
            let changed_indexes = self.changed_indexes.upgradable_read();
            if !changed_indexes.is_empty() {
                wrapped_segment.with_upgraded(|wrapped_segment| {
                    for (field_name, change) in changed_indexes.iter_ordered() {
                        debug_assert!(
                            change.version() >= op_num,
                            "proxied index change should have newer version than segment",
                        );
                        match change {
                            ProxyIndexChange::Create(schema, version) => {
                                wrapped_segment.create_field_index(
                                    *version,
                                    field_name,
                                    Some(schema),
                                )?;
                            }
                            ProxyIndexChange::Delete(version) => {
                                wrapped_segment.delete_field_index(*version, field_name)?;
                            }
                        }
                    }
                    OperationResult::Ok(())
                })?;
                RwLockUpgradableReadGuard::upgrade(changed_indexes).clear();
            }
        }

        // Propagate deleted points
        // Lock ordering is important here and must match the flush function to prevent a deadlock
        {
            let deleted_points = self.deleted_points.upgradable_read();
            if !deleted_points.is_empty() {
                wrapped_segment.with_upgraded(|wrapped_segment| {
                    for (point_id, versions) in deleted_points.iter() {
                        // Delete points here with their operation version, that'll bump the optimized
                        // segment version and will ensure we flush the new changes
                        debug_assert!(
                            versions.operation_version >= op_num,
                            "proxied point deletes should have newer version than segment",
                        );
                        wrapped_segment.delete_point(versions.operation_version, *point_id)?;
                    }
                    OperationResult::Ok(())
                })?;
                RwLockUpgradableReadGuard::upgrade(deleted_points).clear();

                // Note: We do not clear the deleted mask here, as it provides
                // no performance advantage and does not affect the correctness of search.
                // Points are still marked as deleted in two places, which is fine
            }
        }

        Ok(())
    }

    /// This function is a simplified version of `search_batch` intended for testing purposes.
    #[cfg(feature = "testing")]
    #[allow(clippy::too_many_arguments)]
    pub fn search(
        &self,
        vector_name: &str,
        vector: &QueryVector,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
    ) -> OperationResult<Vec<ScoredPoint>> {
        let query_context = QueryContext::default();
        let segment_query_context = query_context.get_segment_query_context();
        let result = self.search_batch(
            vector_name,
            &[vector],
            with_payload,
            with_vector,
            filter,
            top,
            params,
            &segment_query_context,
        )?;

        // This function is only for testing and no measurements are needed.
        segment_query_context
            .take_hardware_counter()
            .discard_results();

        Ok(result.into_iter().next().unwrap())
    }
}

impl SegmentEntry for ProxySegment {
    fn version(&self) -> SeqNumberType {
        cmp::max(
            self.wrapped_segment.get().read().version(),
            self.write_segment.get().read().version(),
        )
    }

    fn point_version(&self, point_id: PointIdType) -> Option<SeqNumberType> {
        // Write version is always higher if present
        self.write_segment
            .get()
            .read()
            .point_version(point_id)
            .or_else(|| self.wrapped_segment.get().read().point_version(point_id))
    }

    fn search_batch(
        &self,
        vector_name: &str,
        vectors: &[&QueryVector],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &SegmentQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        let deleted_points = self.deleted_points.read();

        // Some point might be deleted after temporary segment creation
        // We need to prevent them from being found by search request
        // That is why we need to pass additional filter for deleted points
        let do_update_filter = !deleted_points.is_empty();
        let mut wrapped_results = if do_update_filter {
            // If we are wrapping a segment with deleted points,
            // we can make this hack of replacing deleted_points of the wrapped_segment
            // with our proxied deleted_points, do avoid additional filter creation
            if let Some(deleted_points) = self.deleted_mask.as_ref() {
                let query_context_with_deleted = query_context
                    .clone_no_counters()
                    .with_deleted_points(deleted_points);

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

                let counters = query_context_with_deleted.take_hardware_counter();
                query_context.merge_hardware_counter(counters);

                res?
            } else {
                let wrapped_filter = Self::add_deleted_points_condition_to_filter(
                    filter,
                    deleted_points.keys().copied(),
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
        let mut write_results = self.write_segment.get().read().search_batch(
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

    fn upsert_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vectors: NamedVectors,
    ) -> OperationResult<bool> {
        self.move_if_exists(op_num, point_id)?;
        self.write_segment
            .get()
            .write()
            .upsert_point(op_num, point_id, vectors)
    }

    fn delete_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        let mut was_deleted = false;

        let point_offset = match &self.wrapped_segment {
            LockedSegment::Original(raw_segment) => {
                let point_offset = raw_segment.read().get_internal_id(point_id);
                if point_offset.is_some() {
                    was_deleted = self
                        .deleted_points
                        .write()
                        .insert(
                            point_id,
                            ProxyDeletedPoint {
                                local_version: op_num,
                                operation_version: op_num,
                            },
                        )
                        .is_none();
                }
                point_offset
            }
            LockedSegment::Proxy(proxy) => {
                if proxy.read().has_point(point_id) {
                    was_deleted = self
                        .deleted_points
                        .write()
                        .insert(
                            point_id,
                            ProxyDeletedPoint {
                                local_version: op_num,
                                operation_version: op_num,
                            },
                        )
                        .is_none();
                }
                None
            }
        };

        self.set_deleted_offset(point_offset);

        let was_deleted_in_writable = self
            .write_segment
            .get()
            .write()
            .delete_point(op_num, point_id)?;

        Ok(was_deleted || was_deleted_in_writable)
    }

    fn update_vectors(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vectors: NamedVectors,
    ) -> OperationResult<bool> {
        self.move_if_exists(op_num, point_id)?;
        self.write_segment
            .get()
            .write()
            .update_vectors(op_num, point_id, vectors)
    }

    fn delete_vector(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vector_name: &str,
    ) -> OperationResult<bool> {
        self.move_if_exists(op_num, point_id)?;
        self.write_segment
            .get()
            .write()
            .delete_vector(op_num, point_id, vector_name)
    }

    fn set_full_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        full_payload: &Payload,
    ) -> OperationResult<bool> {
        self.move_if_exists(op_num, point_id)?;
        self.write_segment
            .get()
            .write()
            .set_full_payload(op_num, point_id, full_payload)
    }

    fn set_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        payload: &Payload,
        key: &Option<JsonPath>,
    ) -> OperationResult<bool> {
        self.move_if_exists(op_num, point_id)?;
        self.write_segment
            .get()
            .write()
            .set_payload(op_num, point_id, payload, key)
    }

    fn delete_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<bool> {
        self.move_if_exists(op_num, point_id)?;
        self.write_segment
            .get()
            .write()
            .delete_payload(op_num, point_id, key)
    }

    fn clear_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        self.move_if_exists(op_num, point_id)?;
        self.write_segment
            .get()
            .write()
            .clear_payload(op_num, point_id)
    }

    fn vector(
        &self,
        vector_name: &str,
        point_id: PointIdType,
    ) -> OperationResult<Option<VectorInternal>> {
        return if self.deleted_points.read().contains_key(&point_id) {
            self.write_segment
                .get()
                .read()
                .vector(vector_name, point_id)
        } else {
            {
                let write_segment = self.write_segment.get();
                let segment_guard = write_segment.read();
                if segment_guard.has_point(point_id) {
                    return segment_guard.vector(vector_name, point_id);
                }
            }
            self.wrapped_segment
                .get()
                .read()
                .vector(vector_name, point_id)
        };
    }

    fn all_vectors(&self, point_id: PointIdType) -> OperationResult<NamedVectors> {
        let mut result = NamedVectors::default();
        for vector_name in self
            .wrapped_segment
            .get()
            .read()
            .config()
            .vector_data
            .keys()
        {
            if let Some(vector) = self.vector(vector_name, point_id)? {
                result.insert(vector_name.clone(), vector);
            }
        }
        for vector_name in self
            .wrapped_segment
            .get()
            .read()
            .config()
            .sparse_vector_data
            .keys()
        {
            if let Some(vector) = self.vector(vector_name, point_id)? {
                result.insert(vector_name.clone(), vector);
            }
        }
        Ok(result)
    }

    fn payload(&self, point_id: PointIdType) -> OperationResult<Payload> {
        return if self.deleted_points.read().contains_key(&point_id) {
            self.write_segment.get().read().payload(point_id)
        } else {
            {
                let write_segment = self.write_segment.get();
                let segment_guard = write_segment.read();
                if segment_guard.has_point(point_id) {
                    return segment_guard.payload(point_id);
                }
            }
            self.wrapped_segment.get().read().payload(point_id)
        };
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
    ) -> Vec<PointIdType> {
        let deleted_points = self.deleted_points.read();
        let mut read_points = if deleted_points.is_empty() {
            self.wrapped_segment
                .get()
                .read()
                .read_filtered(offset, limit, filter, is_stopped)
        } else {
            let wrapped_filter = Self::add_deleted_points_condition_to_filter(
                filter,
                deleted_points.keys().copied(),
            );
            self.wrapped_segment.get().read().read_filtered(
                offset,
                limit,
                Some(&wrapped_filter),
                is_stopped,
            )
        };
        let mut write_segment_points = self
            .write_segment
            .get()
            .read()
            .read_filtered(offset, limit, filter, is_stopped);
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
    ) -> OperationResult<Vec<(OrderValue, PointIdType)>> {
        let deleted_points = self.deleted_points.read();
        let mut read_points = if deleted_points.is_empty() {
            self.wrapped_segment
                .get()
                .read()
                .read_ordered_filtered(limit, filter, order_by, is_stopped)?
        } else {
            let wrapped_filter = Self::add_deleted_points_condition_to_filter(
                filter,
                deleted_points.keys().copied(),
            );
            self.wrapped_segment.get().read().read_ordered_filtered(
                limit,
                Some(&wrapped_filter),
                order_by,
                is_stopped,
            )?
        };
        let mut write_segment_points = self
            .write_segment
            .get()
            .read()
            .read_ordered_filtered(limit, filter, order_by, is_stopped)?;
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
    ) -> Vec<PointIdType> {
        let deleted_points = self.deleted_points.read();
        let mut read_points = if deleted_points.is_empty() {
            self.wrapped_segment
                .get()
                .read()
                .read_random_filtered(limit, filter, is_stopped)
        } else {
            let wrapped_filter = Self::add_deleted_points_condition_to_filter(
                filter,
                deleted_points.keys().copied(),
            );
            self.wrapped_segment.get().read().read_random_filtered(
                limit,
                Some(&wrapped_filter),
                is_stopped,
            )
        };
        let mut write_segment_points = self
            .write_segment
            .get()
            .read()
            .read_random_filtered(limit, filter, is_stopped);
        read_points.append(&mut write_segment_points);
        read_points.sort_unstable();
        read_points.dedup();
        read_points
    }

    /// Read points in [from; to) range
    fn read_range(&self, from: Option<PointIdType>, to: Option<PointIdType>) -> Vec<PointIdType> {
        let deleted_points = self.deleted_points.read();
        let mut read_points = self.wrapped_segment.get().read().read_range(from, to);
        if !deleted_points.is_empty() {
            read_points.retain(|idx| !deleted_points.contains_key(idx))
        }
        let mut write_segment_points = self.write_segment.get().read().read_range(from, to);
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
    ) -> OperationResult<BTreeSet<FacetValue>> {
        let mut values = self
            .wrapped_segment
            .get()
            .read()
            .unique_values(key, filter, is_stopped)?;

        values.extend(
            self.write_segment
                .get()
                .read()
                .unique_values(key, filter, is_stopped)?,
        );

        Ok(values)
    }

    fn facet(
        &self,
        request: &FacetParams,
        is_stopped: &AtomicBool,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        let deleted_points = self.deleted_points.read();
        let mut hits = if deleted_points.is_empty() {
            self.wrapped_segment
                .get()
                .read()
                .facet(request, is_stopped)?
        } else {
            let wrapped_filter = Self::add_deleted_points_condition_to_filter(
                request.filter.as_ref(),
                deleted_points.keys().copied(),
            );
            let new_request = FacetParams {
                filter: Some(wrapped_filter),
                ..request.clone()
            };
            self.wrapped_segment
                .get()
                .read()
                .facet(&new_request, is_stopped)?
        };

        let write_segment_hits = self.write_segment.get().read().facet(request, is_stopped)?;

        write_segment_hits
            .into_iter()
            .for_each(|(facet_value, count)| {
                *hits.entry(facet_value).or_insert(0) += count;
            });

        Ok(hits)
    }

    fn has_point(&self, point_id: PointIdType) -> bool {
        return if self.deleted_points.read().contains_key(&point_id) {
            self.write_segment.get().read().has_point(point_id)
        } else {
            self.write_segment.get().read().has_point(point_id)
                || self.wrapped_segment.get().read().has_point(point_id)
        };
    }

    fn is_empty(&self) -> bool {
        self.wrapped_segment.get().read().is_empty() && self.write_segment.get().read().is_empty()
    }

    fn available_point_count(&self) -> usize {
        let deleted_points_count = self.deleted_points.read().len();
        let wrapped_segment_count = self.wrapped_segment.get().read().available_point_count();
        let write_segment_count = self.write_segment.get().read().available_point_count();
        (wrapped_segment_count + write_segment_count).saturating_sub(deleted_points_count)
    }

    fn deleted_point_count(&self) -> usize {
        self.write_segment.get().read().deleted_point_count()
    }

    fn available_vectors_size_in_bytes(&self, vector_name: &str) -> OperationResult<usize> {
        let wrapped_segment = self.wrapped_segment.get();
        let wrapped_segment_guard = wrapped_segment.read();
        let wrapped_size = wrapped_segment_guard.available_vectors_size_in_bytes(vector_name)?;
        let wrapped_count = wrapped_segment_guard.available_point_count();
        drop(wrapped_segment_guard);

        let write_segment = self.write_segment.get();
        let write_segment_guard = write_segment.read();
        let write_size = write_segment_guard.available_vectors_size_in_bytes(vector_name)?;
        let write_count = write_segment_guard.available_point_count();
        drop(write_segment_guard);

        let stored_points = wrapped_count + write_count;
        // because we don't know the exact size of deleted vectors, we assume that they are the same avg size as the wrapped ones
        if stored_points > 0 {
            let deleted_points_count = self.deleted_points.read().len();
            let available_points = stored_points.saturating_sub(deleted_points_count);
            Ok(
                ((wrapped_size as u128 + write_size as u128) * available_points as u128
                    / stored_points as u128) as usize,
            )
        } else {
            Ok(0)
        }
    }

    fn estimate_point_count<'a>(&'a self, filter: Option<&'a Filter>) -> CardinalityEstimation {
        let deleted_point_count = self.deleted_points.read().len();

        let (wrapped_segment_est, total_wrapped_size) = {
            let wrapped_segment = self.wrapped_segment.get();
            let wrapped_segment_guard = wrapped_segment.read();
            (
                wrapped_segment_guard.estimate_point_count(filter),
                wrapped_segment_guard.available_point_count(),
            )
        };

        let write_segment_est = self.write_segment.get().read().estimate_point_count(filter);

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

    fn segment_type(&self) -> SegmentType {
        SegmentType::Special
    }

    fn size_info(&self) -> SegmentInfo {
        // To reduce code complexity for estimations, we use `.info()` directly here.
        self.info()
    }

    fn info(&self) -> SegmentInfo {
        let wrapped_info = self.wrapped_segment.get().read().info();
        let write_info = self.write_segment.get().read().size_info(); // Only fields set by `size_info()` needed!

        let vector_name_count =
            self.config().vector_data.len() + self.config().sparse_vector_data.len();
        let deleted_points_count = self.deleted_points.read().len();

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

    fn config(&self) -> &SegmentConfig {
        &self.wrapped_config
    }

    fn is_appendable(&self) -> bool {
        true
    }

    fn flush(&self, sync: bool, force: bool) -> OperationResult<SeqNumberType> {
        let changed_indexes_guard = self.changed_indexes.read();
        let deleted_points_guard = self.deleted_points.read();

        let (wrapped_version, wrapped_persisted_version) = {
            let wrapped_segment = self.wrapped_segment.get();
            let wrapped_segment_guard = wrapped_segment.read();
            let persisted_version = wrapped_segment_guard.flush(sync, force)?;
            let version = wrapped_segment_guard.version();
            (version, persisted_version)
        };

        // In case there are no updates happened to the proxy, this value can be
        // - either higher than wrapped segment (in case new updates were written to proxy as an appendable segment)
        // - or lower than wrapped segment (in case no updates were written to proxy, so the version is 0)
        let write_persisted_version = self.write_segment.get().read().flush(sync, force)?;

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

        // Store index change to later propagate to optimized/wrapped segment
        self.changed_indexes
            .write()
            .insert(key.clone(), ProxyIndexChange::Delete(op_num));

        self.write_segment
            .get()
            .write()
            .delete_field_index(op_num, key)
    }

    fn build_field_index(
        &self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
        field_type: Option<&PayloadFieldSchema>,
    ) -> OperationResult<Option<(PayloadFieldSchema, Vec<FieldIndex>)>> {
        if self.version() > op_num {
            return Ok(None);
        }

        self.write_segment
            .get()
            .read()
            .build_field_index(op_num, key, field_type)
    }

    fn apply_field_index(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyType,
        field_schema: PayloadFieldSchema,
        field_index: Vec<FieldIndex>,
    ) -> OperationResult<bool> {
        if self.version() > op_num {
            return Ok(false);
        }

        if !self.write_segment.get().write().apply_field_index(
            op_num,
            key.clone(),
            field_schema.clone(),
            field_index,
        )? {
            return Ok(false);
        };

        // Store index change to later propagate to optimized/wrapped segment
        self.changed_indexes
            .write()
            .insert(key.clone(), ProxyIndexChange::Create(field_schema, op_num));

        Ok(true)
    }

    fn get_indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema> {
        let mut indexed_fields = self.wrapped_segment.get().read().get_indexed_fields();

        for (field_name, change) in self.changed_indexes.read().iter_unordered() {
            match change {
                ProxyIndexChange::Create(schema, _) => {
                    indexed_fields.insert(field_name.to_owned(), schema.to_owned());
                }
                ProxyIndexChange::Delete(_) => {
                    indexed_fields.remove(field_name);
                }
            }
        }

        indexed_fields
    }

    fn check_error(&self) -> Option<SegmentFailedState> {
        self.write_segment.get().read().check_error()
    }

    fn delete_filtered<'a>(
        &'a mut self,
        op_num: SeqNumberType,
        filter: &'a Filter,
    ) -> OperationResult<usize> {
        let mut deleted_points = 0;
        // we don’t want to cancel this filtered read
        let is_stopped = AtomicBool::new(false);
        let points_to_delete =
            self.wrapped_segment
                .get()
                .read()
                .read_filtered(None, None, Some(filter), &is_stopped);
        let points_offsets_to_delete = match &self.wrapped_segment {
            LockedSegment::Original(raw_segment) => {
                let raw_segment_read = raw_segment.read();
                points_to_delete
                    .iter()
                    .filter_map(|point_id| raw_segment_read.get_internal_id(*point_id))
                    .collect()
            }
            LockedSegment::Proxy(_) => vec![],
        };

        if !points_to_delete.is_empty() {
            deleted_points += points_to_delete.len();
            let mut deleted_points_guard = self.deleted_points.write();
            deleted_points_guard.extend(points_to_delete.iter().map(|&point_id| {
                (
                    point_id,
                    ProxyDeletedPoint {
                        local_version: op_num,
                        operation_version: op_num,
                    },
                )
            }));
        }

        for point_offset in points_offsets_to_delete {
            self.set_deleted_offset(Some(point_offset));
        }

        deleted_points += self
            .write_segment
            .get()
            .write()
            .delete_filtered(op_num, filter)?;

        Ok(deleted_points)
    }

    fn vector_names(&self) -> HashSet<String> {
        self.write_segment.get().read().vector_names()
    }

    fn take_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        snapshotted_segments: &mut HashSet<String>,
    ) -> OperationResult<()> {
        log::info!("Taking a snapshot of a proxy segment");

        // Snapshot wrapped segment data into the temporary dir
        self.wrapped_segment.get().read().take_snapshot(
            temp_path,
            tar,
            format,
            snapshotted_segments,
        )?;

        // Snapshot write_segment
        self.write_segment.get().read().take_snapshot(
            temp_path,
            tar,
            format,
            snapshotted_segments,
        )?;

        Ok(())
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

/// Point persion information of points to delete from a wrapped proxy segment.
#[derive(Clone, Copy, Debug)]
pub struct ProxyDeletedPoint {
    /// Version the point had in the wrapped segment when the delete was scheduled.
    /// We use it to determine if some other proxy segment should move the point again with
    /// `move_if_exists` if it has newer point data.
    pub local_version: SeqNumberType,
    /// Version of the operation that caused the delete to be scheduled.
    /// We use it for the delete operations when propagating them to the wrapped or optimized
    /// segment.
    pub operation_version: SeqNumberType,
}

#[derive(Debug, Default)]
pub struct ProxyIndexChanges {
    changes: HashMap<PayloadKeyType, ProxyIndexChange>,
}

impl ProxyIndexChanges {
    pub fn insert(&mut self, key: PayloadKeyType, change: ProxyIndexChange) {
        self.changes.insert(key, change);
    }

    pub fn remove(&mut self, key: &PayloadKeyType) {
        self.changes.remove(key);
    }

    pub fn len(&self) -> usize {
        self.changes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn clear(&mut self) {
        self.changes.clear();
    }

    /// Iterate over proxy index changes in order of version.
    ///
    /// Index changes must be applied in order because changes with an old version will silently be
    /// rejected.
    pub fn iter_ordered(&self) -> impl Iterator<Item = (&PayloadKeyType, &ProxyIndexChange)> {
        self.changes
            .iter()
            .sorted_by_key(|(_, change)| change.version())
    }

    /// Iterate over proxy index changes in arbitrary order.
    pub fn iter_unordered(&self) -> impl Iterator<Item = (&PayloadKeyType, &ProxyIndexChange)> {
        self.changes.iter()
    }
}

#[derive(Debug)]
pub enum ProxyIndexChange {
    Create(PayloadFieldSchema, SeqNumberType),
    Delete(SeqNumberType),
}

impl ProxyIndexChange {
    pub fn version(&self) -> SeqNumberType {
        match self {
            ProxyIndexChange::Create(_, version) => *version,
            ProxyIndexChange::Delete(version) => *version,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use segment::data_types::vectors::{only_default_vector, DEFAULT_VECTOR_NAME};
    use segment::types::{FieldCondition, PayloadSchemaType};
    use serde_json::json;
    use tempfile::{Builder, TempDir};

    use super::*;
    use crate::collection_manager::fixtures::{
        build_segment_1, build_segment_2, empty_segment, random_segment,
    };

    #[test]
    fn test_writing() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let original_segment = LockedSegment::new(build_segment_1(dir.path()));
        let write_segment = LockedSegment::new(empty_segment(dir.path()));

        let mut proxy_segment = ProxySegment::new(
            original_segment,
            write_segment,
            LockedRmSet::default(),
            LockedIndexChanges::default(),
        );

        let vec4 = vec![1.1, 1.0, 0.0, 1.0];
        proxy_segment
            .upsert_point(100, 4.into(), only_default_vector(&vec4))
            .unwrap();
        let vec6 = vec![1.0, 1.0, 0.5, 1.0];
        proxy_segment
            .upsert_point(101, 6.into(), only_default_vector(&vec6))
            .unwrap();
        proxy_segment.delete_point(102, 1.into()).unwrap();

        let query_vector = [1.0, 1.0, 1.0, 1.0].into();
        let search_result = proxy_segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
            )
            .unwrap();

        eprintln!("search_result = {search_result:#?}");

        let mut seen_points: HashSet<PointIdType> = Default::default();
        for res in search_result {
            if seen_points.contains(&res.id) {
                panic!("point {} appears multiple times", res.id);
            }
            seen_points.insert(res.id);
        }

        assert!(seen_points.contains(&4.into()));
        assert!(seen_points.contains(&6.into()));
        assert!(!seen_points.contains(&1.into()));

        assert!(!proxy_segment.write_segment.get().read().has_point(2.into()));

        let payload_key = "color".parse().unwrap();
        proxy_segment
            .delete_payload(103, 2.into(), &payload_key)
            .unwrap();

        assert!(proxy_segment.write_segment.get().read().has_point(2.into()))
    }

    #[test]
    fn test_search_batch_equivalence_single() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let original_segment = LockedSegment::new(build_segment_1(dir.path()));
        let write_segment = LockedSegment::new(empty_segment(dir.path()));

        let mut proxy_segment = ProxySegment::new(
            original_segment,
            write_segment,
            LockedRmSet::default(),
            LockedIndexChanges::default(),
        );

        let vec4 = vec![1.1, 1.0, 0.0, 1.0];
        proxy_segment
            .upsert_point(100, 4.into(), only_default_vector(&vec4))
            .unwrap();
        let vec6 = vec![1.0, 1.0, 0.5, 1.0];
        proxy_segment
            .upsert_point(101, 6.into(), only_default_vector(&vec6))
            .unwrap();
        proxy_segment.delete_point(102, 1.into()).unwrap();

        let query_vector = [1.0, 1.0, 1.0, 1.0].into();
        let search_result = proxy_segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
            )
            .unwrap();

        eprintln!("search_result = {search_result:#?}");

        let query_context = QueryContext::default();
        let segment_query_context = query_context.get_segment_query_context();

        let search_batch_result = proxy_segment
            .search_batch(
                DEFAULT_VECTOR_NAME,
                &[&query_vector],
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
                &segment_query_context,
            )
            .unwrap();

        eprintln!("search_batch_result = {search_batch_result:#?}");

        assert!(!search_result.is_empty());
        assert_eq!(search_result, search_batch_result[0].clone());
        let counter = segment_query_context.take_hardware_counter();
        assert!(counter.cpu_counter().get() > 0);
        counter.discard_results();
    }

    #[test]
    fn test_search_batch_equivalence_single_random() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let original_segment = LockedSegment::new(random_segment(dir.path(), 100, 200, 4));
        let write_segment = LockedSegment::new(empty_segment(dir.path()));

        let proxy_segment = ProxySegment::new(
            original_segment,
            write_segment,
            LockedRmSet::default(),
            LockedIndexChanges::default(),
        );

        let query_vector = [1.0, 1.0, 1.0, 1.0].into();
        let search_result = proxy_segment
            .search(
                DEFAULT_VECTOR_NAME,
                &query_vector,
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
            )
            .unwrap();

        eprintln!("search_result = {search_result:#?}");

        let query_context = QueryContext::default();
        let segment_query_context = query_context.get_segment_query_context();

        let search_batch_result = proxy_segment
            .search_batch(
                DEFAULT_VECTOR_NAME,
                &[&query_vector],
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
                &segment_query_context,
            )
            .unwrap();

        segment_query_context
            .take_hardware_counter()
            .discard_results();

        eprintln!("search_batch_result = {search_batch_result:#?}");

        assert!(!search_result.is_empty());
        assert_eq!(search_result, search_batch_result[0].clone())
    }

    #[test]
    fn test_search_batch_equivalence_multi_random() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let original_segment = LockedSegment::new(random_segment(dir.path(), 100, 200, 4));
        let write_segment = LockedSegment::new(empty_segment(dir.path()));

        let proxy_segment = ProxySegment::new(
            original_segment,
            write_segment,
            LockedRmSet::default(),
            LockedIndexChanges::default(),
        );

        let q1 = [1.0, 1.0, 1.0, 0.1];
        let q2 = [1.0, 1.0, 0.1, 0.1];
        let q3 = [1.0, 0.1, 1.0, 0.1];
        let q4 = [0.1, 1.0, 1.0, 0.1];

        let query_vectors: &[&QueryVector] = &[&q1.into(), &q2.into(), &q3.into(), &q4.into()];

        let mut all_single_results = Vec::with_capacity(query_vectors.len());
        for query_vector in query_vectors {
            let res = proxy_segment
                .search(
                    DEFAULT_VECTOR_NAME,
                    query_vector,
                    &WithPayload::default(),
                    &false.into(),
                    None,
                    10,
                    None,
                )
                .unwrap();
            all_single_results.push(res);
        }

        eprintln!("search_result = {all_single_results:#?}");

        let query_context = QueryContext::default();
        let segment_query_context = query_context.get_segment_query_context();

        let search_batch_result = proxy_segment
            .search_batch(
                DEFAULT_VECTOR_NAME,
                query_vectors,
                &WithPayload::default(),
                &false.into(),
                None,
                10,
                None,
                &segment_query_context,
            )
            .unwrap();

        segment_query_context
            .take_hardware_counter()
            .discard_results();

        eprintln!("search_batch_result = {search_batch_result:#?}");

        assert_eq!(all_single_results, search_batch_result)
    }

    fn wrap_proxy(dir: &TempDir, original_segment: LockedSegment) -> ProxySegment {
        let write_segment = LockedSegment::new(empty_segment(dir.path()));

        ProxySegment::new(
            original_segment,
            write_segment,
            LockedRmSet::default(),
            LockedIndexChanges::default(),
        )
    }

    #[test]
    fn test_read_filter() {
        let is_stopped = AtomicBool::new(false);
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let original_segment = LockedSegment::new(build_segment_1(dir.path()));

        let filter = Filter::new_must_not(Condition::Field(FieldCondition::new_match(
            "color".parse().unwrap(),
            "blue".to_string().into(),
        )));

        let original_points =
            original_segment
                .get()
                .read()
                .read_filtered(None, Some(100), None, &is_stopped);

        let original_points_filtered = original_segment.get().read().read_filtered(
            None,
            Some(100),
            Some(&filter),
            &is_stopped,
        );

        let mut proxy_segment = wrap_proxy(&dir, original_segment);

        proxy_segment.delete_point(100, 2.into()).unwrap();

        let proxy_res = proxy_segment.read_filtered(None, Some(100), None, &is_stopped);
        let proxy_res_filtered =
            proxy_segment.read_filtered(None, Some(100), Some(&filter), &is_stopped);

        assert_eq!(original_points_filtered.len() - 1, proxy_res_filtered.len());
        assert_eq!(original_points.len() - 1, proxy_res.len());
    }

    #[test]
    fn test_read_range() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let original_segment = LockedSegment::new(build_segment_1(dir.path()));

        let original_points = original_segment
            .get()
            .read()
            .read_range(None, Some(10.into()));

        let mut proxy_segment = wrap_proxy(&dir, original_segment);

        proxy_segment.delete_point(100, 2.into()).unwrap();

        proxy_segment
            .set_payload(
                101,
                3.into(),
                &json!({ "color": vec!["red".to_owned()] }).into(),
                &None,
            )
            .unwrap();
        let proxy_res = proxy_segment.read_range(None, Some(10.into()));

        assert_eq!(original_points.len() - 1, proxy_res.len());
    }

    #[test]
    fn test_sync_indexes() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let original_segment = LockedSegment::new(build_segment_1(dir.path()));
        let write_segment = LockedSegment::new(empty_segment(dir.path()));

        original_segment
            .get()
            .write()
            .create_field_index(
                10,
                &"color".parse().unwrap(),
                Some(&PayloadSchemaType::Keyword.into()),
            )
            .unwrap();

        let mut proxy_segment = ProxySegment::new(
            original_segment.clone(),
            write_segment.clone(),
            LockedRmSet::default(),
            LockedIndexChanges::default(),
        );

        proxy_segment.replicate_field_indexes(0).unwrap();

        assert!(write_segment
            .get()
            .read()
            .get_indexed_fields()
            .contains_key(&"color".parse().unwrap()));

        original_segment
            .get()
            .write()
            .create_field_index(
                11,
                &"location".parse().unwrap(),
                Some(&PayloadSchemaType::Geo.into()),
            )
            .unwrap();

        original_segment
            .get()
            .write()
            .delete_field_index(12, &"color".parse().unwrap())
            .unwrap();

        proxy_segment.replicate_field_indexes(0).unwrap();

        assert!(write_segment
            .get()
            .read()
            .get_indexed_fields()
            .contains_key(&"location".parse().unwrap()));
        assert!(!write_segment
            .get()
            .read()
            .get_indexed_fields()
            .contains_key(&"color".parse().unwrap()));
    }

    #[test]
    fn test_take_snapshot() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let original_segment = LockedSegment::new(build_segment_1(dir.path()));
        let original_segment_2 = LockedSegment::new(build_segment_2(dir.path()));
        let write_segment = LockedSegment::new(empty_segment(dir.path()));

        let deleted_points = LockedRmSet::default();
        let changed_indexes = LockedIndexChanges::default();

        let mut proxy_segment = ProxySegment::new(
            original_segment,
            write_segment.clone(),
            Arc::clone(&deleted_points),
            Arc::clone(&changed_indexes),
        );

        let mut proxy_segment2 = ProxySegment::new(
            original_segment_2,
            write_segment,
            deleted_points,
            changed_indexes,
        );

        let vec4 = vec![1.1, 1.0, 0.0, 1.0];
        proxy_segment
            .upsert_point(100, 4.into(), only_default_vector(&vec4))
            .unwrap();
        let vec6 = vec![1.0, 1.0, 0.5, 1.0];
        proxy_segment
            .upsert_point(101, 6.into(), only_default_vector(&vec6))
            .unwrap();
        proxy_segment.delete_point(102, 1.into()).unwrap();

        proxy_segment2
            .upsert_point(201, 11.into(), only_default_vector(&vec6))
            .unwrap();

        let snapshot_file = Builder::new().suffix(".snapshot.tar").tempfile().unwrap();
        eprintln!("Snapshot into {:?}", snapshot_file.path());
        let tar = tar_ext::BuilderExt::new_seekable_owned(File::create(&snapshot_file).unwrap());
        let temp_dir = Builder::new().prefix("temp_dir").tempdir().unwrap();
        let temp_dir2 = Builder::new().prefix("temp_dir").tempdir().unwrap();
        let mut snapshotted_segments = HashSet::new();
        proxy_segment
            .take_snapshot(
                temp_dir.path(),
                &tar,
                SnapshotFormat::Regular,
                &mut snapshotted_segments,
            )
            .unwrap();
        proxy_segment2
            .take_snapshot(
                temp_dir2.path(),
                &tar,
                SnapshotFormat::Regular,
                &mut snapshotted_segments,
            )
            .unwrap();
        tar.blocking_finish().unwrap();

        // validate that 3 archives were created:
        // wrapped_segment1, wrapped_segment2 & shared write_segment
        let mut tar = tar::Archive::new(File::open(&snapshot_file).unwrap());
        let archive_count = tar.entries_with_seek().unwrap().count();
        assert_eq!(archive_count, 3);
        assert_eq!(snapshotted_segments.len(), 3);

        let mut tar = tar::Archive::new(File::open(&snapshot_file).unwrap());
        for entry in tar.entries_with_seek().unwrap() {
            let archive_path = entry.unwrap().path().unwrap().into_owned();
            let archive_extension = archive_path.extension().unwrap();
            // correct file extension
            assert_eq!(archive_extension, "tar");
        }
    }

    #[test]
    fn test_point_vector_count() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let original_segment = LockedSegment::new(build_segment_1(dir.path()));
        let write_segment = LockedSegment::new(empty_segment(dir.path()));

        let mut proxy_segment = ProxySegment::new(
            original_segment,
            write_segment,
            LockedRmSet::default(),
            LockedIndexChanges::default(),
        );

        // We have 5 points by default, assert counts
        let segment_info = proxy_segment.info();
        assert_eq!(segment_info.num_points, 5);
        assert_eq!(segment_info.num_vectors, 5);

        // Delete nonexistent point, counts should remain the same
        proxy_segment.delete_point(101, 99999.into()).unwrap();
        let segment_info = proxy_segment.info();
        assert_eq!(segment_info.num_points, 5);
        assert_eq!(segment_info.num_vectors, 5);

        // Delete point 1, counts should decrease by 1
        proxy_segment.delete_point(102, 4.into()).unwrap();
        let segment_info = proxy_segment.info();
        assert_eq!(segment_info.num_points, 4);
        assert_eq!(segment_info.num_vectors, 4);

        // Delete vector of point 2, vector count should now be zero
        proxy_segment
            .delete_vector(103, 2.into(), DEFAULT_VECTOR_NAME)
            .unwrap();
        let segment_info = proxy_segment.info();
        assert_eq!(segment_info.num_points, 4);
        assert_eq!(segment_info.num_vectors, 3);
    }

    #[test]
    fn test_point_vector_count_multivec() {
        use segment::segment_constructor::build_segment;
        use segment::types::{Distance, Indexes, VectorDataConfig, VectorStorageType};

        // Create proxied multivec segment
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let dim = 1;
        let config = SegmentConfig {
            vector_data: HashMap::from([
                (
                    "a".into(),
                    VectorDataConfig {
                        size: dim,
                        distance: Distance::Dot,
                        storage_type: VectorStorageType::Memory,
                        index: Indexes::Plain {},
                        quantization_config: None,
                        multivector_config: None,
                        datatype: None,
                    },
                ),
                (
                    "b".into(),
                    VectorDataConfig {
                        size: dim,
                        distance: Distance::Dot,
                        storage_type: VectorStorageType::Memory,
                        index: Indexes::Plain {},
                        quantization_config: None,
                        multivector_config: None,
                        datatype: None,
                    },
                ),
            ]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        };
        let mut original_segment = build_segment(dir.path(), &config, true).unwrap();
        let write_segment = build_segment(dir.path(), &config, true).unwrap();

        original_segment
            .upsert_point(
                100,
                4.into(),
                NamedVectors::from_pairs([("a".into(), vec![0.4]), ("b".into(), vec![0.5])]),
            )
            .unwrap();
        original_segment
            .upsert_point(
                101,
                6.into(),
                NamedVectors::from_pairs([("a".into(), vec![0.6]), ("b".into(), vec![0.7])]),
            )
            .unwrap();

        let original_segment = LockedSegment::new(original_segment);
        let write_segment = LockedSegment::new(write_segment);

        let mut proxy_segment = ProxySegment::new(
            original_segment,
            write_segment,
            LockedRmSet::default(),
            LockedIndexChanges::default(),
        );

        // Assert counts from original segment
        let segment_info = proxy_segment.info();
        assert_eq!(segment_info.num_points, 2);
        assert_eq!(segment_info.num_vectors, 4);

        // Insert point ID 8 and 10 partially, assert counts
        proxy_segment
            .upsert_point(
                102,
                8.into(),
                NamedVectors::from_pairs([("a".into(), vec![0.0])]),
            )
            .unwrap();
        proxy_segment
            .upsert_point(
                103,
                10.into(),
                NamedVectors::from_pairs([("b".into(), vec![1.0])]),
            )
            .unwrap();
        let segment_info = proxy_segment.info();
        assert_eq!(segment_info.num_points, 4);
        assert_eq!(segment_info.num_vectors, 6);

        // Delete nonexistent point, counts should remain the same
        proxy_segment.delete_point(104, 1.into()).unwrap();
        let segment_info = proxy_segment.info();
        assert_eq!(segment_info.num_points, 4);
        assert_eq!(segment_info.num_vectors, 6);

        // Delete point 4, counts should decrease by 1
        proxy_segment.delete_point(105, 4.into()).unwrap();
        let segment_info = proxy_segment.info();
        assert_eq!(segment_info.num_points, 3);
        assert_eq!(segment_info.num_vectors, 4);

        // Delete vector 'a' of point 6, vector count should decrease by 1
        proxy_segment.delete_vector(106, 6.into(), "a").unwrap();
        let segment_info = proxy_segment.info();
        assert_eq!(segment_info.num_points, 3);
        assert_eq!(segment_info.num_vectors, 3);

        // Deleting it again shouldn't chain anything
        proxy_segment.delete_vector(107, 6.into(), "a").unwrap();
        let segment_info = proxy_segment.info();
        assert_eq!(segment_info.num_points, 3);
        assert_eq!(segment_info.num_vectors, 3);

        // Replace vector 'a' for point 8, counts should remain the same
        proxy_segment
            .upsert_point(
                108,
                8.into(),
                NamedVectors::from_pairs([("a".into(), vec![0.0])]),
            )
            .unwrap();
        let segment_info = proxy_segment.info();
        assert_eq!(segment_info.num_points, 3);
        assert_eq!(segment_info.num_vectors, 3);

        // Replace both vectors for point 8, adding a new vector
        proxy_segment
            .upsert_point(
                109,
                8.into(),
                NamedVectors::from_pairs([("a".into(), vec![0.0]), ("b".into(), vec![0.0])]),
            )
            .unwrap();
        let segment_info = proxy_segment.info();
        assert_eq!(segment_info.num_points, 3);
        assert_eq!(segment_info.num_vectors, 4);
    }

    #[test]
    fn test_proxy_segment_flush() {
        let tmp_dir = tempfile::Builder::new()
            .prefix("segment_dir")
            .tempdir()
            .unwrap();

        let locked_wrapped_segment = LockedSegment::new(build_segment_1(tmp_dir.path()));
        let locked_write_segment = LockedSegment::new(empty_segment(tmp_dir.path()));

        let mut proxy_segment = ProxySegment::new(
            locked_wrapped_segment.clone(),
            locked_write_segment.clone(),
            LockedRmSet::default(),
            LockedIndexChanges::default(),
        );

        // Unwrapped `LockedSegment`s for convenient access
        let LockedSegment::Original(wrapped_segment) = locked_wrapped_segment else {
            unreachable!();
        };

        let LockedSegment::Original(write_segment) = locked_write_segment else {
            unreachable!()
        };

        // - `wrapped_segment` has unflushed data
        // - `write_segment` has no data
        // - `proxy_segment` has no in-memory data
        // - flush `proxy_segment`, ensure:
        //   - `wrapped_segment` is flushed
        //   - `ProxySegment::flush` returns `wrapped_segment`'s persisted version

        let flushed_version = proxy_segment.flush(true, false).unwrap();
        let wrapped_segment_persisted_version = *wrapped_segment.read().persisted_version.lock();
        assert_eq!(Some(flushed_version), wrapped_segment_persisted_version);

        // - `wrapped_segment` has unflushed data
        // - `write_segment` has unflushed data
        // - `proxy_segment` has no in-memory data
        // - flush `proxy_segment`, ensure:
        //   - `wrapped_segment` is flushed
        //   - `write_segment` is flushed
        //   - `ProxySegment::flush` returns `write_segment`'s persisted version

        let current_version = proxy_segment.version();

        wrapped_segment
            .write()
            .upsert_point(
                current_version + 1,
                42.into(),
                only_default_vector(&[4.0, 2.0, 0.0, 0.0]),
            )
            .unwrap();

        proxy_segment
            .upsert_point(
                current_version + 2,
                69.into(),
                only_default_vector(&[6.0, 9.0, 0.0, 0.0]),
            )
            .unwrap();

        let flushed_version = proxy_segment.flush(true, false).unwrap();
        let wrapped_segment_persisted_version = *wrapped_segment.read().persisted_version.lock();
        let write_segment_persisted_version = *write_segment.read().persisted_version.lock();

        assert_eq!(wrapped_segment_persisted_version, Some(current_version + 1));
        assert_eq!(write_segment_persisted_version, Some(current_version + 2));
        assert_eq!(Some(flushed_version), write_segment_persisted_version);

        // - `wrapped_segment` has unflushed data
        // - `write_segment` has unflushed data
        // - `proxy_segment` has in-memory data
        // - flush `proxy_segment`, ensure:
        //   - `wrapped_segment` is flushed
        //   - `write_segment` is flushed
        //   - `ProxySegment::flush` returns `wrapped_segment`'s persisted version

        let current_version = proxy_segment.version();

        wrapped_segment
            .write()
            .upsert_point(
                current_version + 1,
                666.into(),
                only_default_vector(&[6.0, 6.0, 6.0, 0.0]),
            )
            .unwrap();

        proxy_segment
            .upsert_point(
                current_version + 2,
                42.into(),
                only_default_vector(&[0.0, 0.0, 4.0, 2.0]),
            )
            .unwrap();

        let flushed_version = proxy_segment.flush(true, false).unwrap();
        let wrapped_segment_persisted_version = *wrapped_segment.read().persisted_version.lock();
        let write_segment_persisted_version = *write_segment.read().persisted_version.lock();

        assert_eq!(wrapped_segment_persisted_version, Some(current_version + 1));
        assert_eq!(write_segment_persisted_version, Some(current_version + 2));
        assert_eq!(Some(flushed_version), wrapped_segment_persisted_version);
    }
}
