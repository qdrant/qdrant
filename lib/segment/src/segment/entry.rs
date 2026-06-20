use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::fs::safe_delete_with_suffix;
use common::types::{DeferredBehavior, TelemetryDetail};
use uuid::Uuid;

use super::Segment;
use crate::common::operation_error::{OperationError, OperationResult, SegmentFailedState};
use crate::common::{Flusher, check_named_vectors, check_vector_name};
use crate::data_types::build_index_result::BuildFieldIndexResult;
use crate::data_types::facets::{FacetParams, FacetValue};
use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::order_by::{OrderBy, OrderValue};
use crate::data_types::query_context::{FormulaContext, QueryContext, SegmentQueryContext};
use crate::data_types::segment_record::SegmentRecord;
use crate::data_types::vector_name_config::VectorNameConfig;
use crate::data_types::vectors::{QueryVector, VectorInternal};
use crate::entry::entry_point::{
    NonAppendableSegmentEntry, ReadSegmentEntry, SegmentEntry, StorageSegmentEntry,
};
use crate::id_tracker::{IdTracker, IdTrackerRead, PointMappingsGuard};
use crate::index::field_index::{CardinalityEstimation, FieldIndex};
use crate::index::{BuildIndexResult, PayloadIndex, PayloadIndexRead};
use crate::json_path::JsonPath;
use crate::telemetry::SegmentTelemetry;
use crate::types::{
    ExtendedPointId, Filter, Payload, PayloadFieldSchema, PayloadKeyType, PayloadKeyTypeRef,
    PointIdType, ScoredPoint, SearchParams, SegmentConfig, SegmentInfo, SegmentType, SeqNumberType,
    VectorName, VectorNameBuf, WithPayload, WithVector,
};
use crate::vector_storage::{VectorStorage, VectorStorageRead};

/// This is a basic implementation of the trait, meaning that it implements the _actual_ operations with data and not
/// any kind of proxy or wrapping.
impl ReadSegmentEntry for Segment {
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
        self.all_vectors_with_behavior(point_id, DeferredBehavior::VisibleOnly, hw_counter)
    }

    fn all_vectors_with_behavior(
        &self,
        point_id: PointIdType,
        deferred_behavior: DeferredBehavior,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<NamedVectors<'_>> {
        self.with_view(|view| {
            let mut result = NamedVectors::default();
            for vector_name in view.vector_data.keys() {
                if let Some(vec) =
                    view.vector_with_behavior(vector_name, point_id, deferred_behavior, hw_counter)?
                {
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

    fn payload_with_behavior(
        &self,
        point_id: PointIdType,
        deferred_behavior: DeferredBehavior,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        self.with_view(|view| view.payload_with_behavior(point_id, deferred_behavior, hw_counter))
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

    fn has_point(&self, point_id: PointIdType, deferred_behavior: DeferredBehavior) -> bool {
        self.id_tracker
            .borrow()
            .internal_id_with_behavior(point_id, deferred_behavior)
            .is_some()
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

    fn available_vectors_size_in_bytes(&self, vector_name: &VectorName) -> OperationResult<usize> {
        self.with_view(|view| view.available_vectors_size_in_bytes(vector_name))
    }

    fn estimate_point_count<'a>(
        &'a self,
        filter: Option<&'a Filter>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        self.with_view(|view| view.estimate_point_count(filter, hw_counter))
    }

    fn unique_values(
        &self,
        key: &JsonPath,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<std::collections::BTreeSet<FacetValue>> {
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

    fn segment_uuid(&self) -> Uuid {
        self.uuid
    }

    fn segment_type(&self) -> SegmentType {
        self.segment_type
    }

    fn size_info(&self) -> SegmentInfo {
        self.with_view(|view| {
            view.build_size_info(self.uuid, self.segment_type, self.appendable_flag)
        })
    }

    fn info(&self) -> SegmentInfo {
        self.with_view(|view| view.build_info(self.uuid, self.segment_type, self.appendable_flag))
    }

    fn config(&self) -> &SegmentConfig {
        &self.segment_config
    }

    fn is_appendable(&self) -> bool {
        self.appendable_flag
    }
    fn get_indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema> {
        self.payload_index
            .borrow()
            .with_view(|v| v.indexed_fields())
    }

    fn vector_names(&self) -> HashSet<VectorNameBuf> {
        self.vector_data.keys().cloned().collect()
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> SegmentTelemetry {
        self.with_view(|view| {
            view.build_telemetry(
                self.uuid,
                self.segment_type,
                self.appendable_flag,
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

    fn available_point_count_without_deferred(&self) -> usize {
        self.with_view(|view| view.available_point_count_without_deferred())
    }

    fn has_deferred_points(&self) -> bool {
        self.with_view(|view| view.has_deferred_points())
    }

    fn deferred_point_count(&self) -> usize {
        self.with_view(|view| view.deferred_point_count())
    }
}

impl Segment {
    /// Whether mutating ops on this segment should be routed through the
    /// clone-and-tombstone helper.
    ///
    /// Guards on `is_appendable()` — clone-and-tombstone requires growable
    /// storages — so callers can ignore the underlying flag.
    pub fn is_append_only(&self) -> bool {
        self.is_appendable() && self.append_only_mutations
    }

    /// Iterator over all points in segment in ascending order.
    pub fn iter_points(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        let mappings =
            PointMappingsGuard::new(self.id_tracker.borrow(), |guard| guard.point_mappings());
        Box::new(IterPointsIterator::new(mappings, |mappings| {
            Box::new(mappings.borrow_dependent().iter_external())
        }))
    }
}

impl StorageSegmentEntry for Segment {
    fn version(&self) -> SeqNumberType {
        self.version.unwrap_or(0)
    }

    fn check_error(&self) -> Option<SegmentFailedState> {
        self.error_status.clone()
    }

    fn persistent_version(&self) -> SeqNumberType {
        (*self.persisted_version.lock()).unwrap_or(0)
    }

    fn flusher(&self, force: bool) -> Option<Flusher> {
        let current_persisted_version: Option<SeqNumberType> = *self.persisted_version.lock();

        match (self.version, current_persisted_version) {
            (None, _) => {
                // Segment is empty, nothing to flush
                return None;
            }
            (Some(version), Some(persisted_version)) => {
                if !force && version == persisted_version {
                    log::trace!("not flushing because version == persisted_version");
                    // Segment is already flushed
                    return None;
                }
            }
            (_, _) => {}
        }

        // Capture all flushers first to improve data consistency
        let vector_storage_flushers: Vec<_> = self
            .vector_data
            .values()
            .map(|v| v.vector_storage.borrow().flusher())
            .collect();
        let quantization_flushers: Vec<_> = self
            .vector_data
            .values()
            .filter_map(|v| v.quantized_vectors.borrow().as_ref().map(|q| q.flusher()))
            .collect();
        let state = self.get_state();
        let segment_path = self.segment_path.clone();
        let id_tracker_mapping_flusher = self.id_tracker.borrow().mapping_flusher();
        let payload_index_flusher = self.payload_index.borrow().flusher();
        let id_tracker_versions_flusher = self.id_tracker.borrow().versions_flusher();
        let persisted_version = self.persisted_version.clone();

        // Flush order is important:
        //
        // 1. Flush id mapping. So during recovery the point will be recovered in proper segment.
        // 2. Flush vectors and payloads.
        // 3. Flush id versions last. So presence of version indicates that all other data is up-to-date.
        //
        // Example of recovery from WAL in case of partial flush:
        //
        // In-memory state:
        //
        //     Segment 1                  Segment 2
        //
        //    ID-mapping     vst.1       ID-mapping     vst.2
        //   ext     int
        //  ┌───┐   ┌───┐   ┌───┐       ┌───┐   ┌───┐   ┌───┐
        //  │100├───┤1  │   │1  │       │300├───┤1  │   │1  │
        //  └───┘   └───┘   │2  │       └───┘   └───┘   │2  │
        //                  │   │                       │   │
        //  ┌───┐   ┌───┐   │   │       ┌───┐   ┌───┐   │   │
        //  │200├───┤2  │   │   │       │400├───┤2  │   │   │
        //  └───┘   └───┘   └───┘       └───┘   └───┘   └───┘
        //
        //
        //  ext - external id
        //  int - internal id
        //  vst - vector storage
        //
        //  ─────────────────────────────────────────────────
        //   After flush, segments could be partially preserved:
        //
        //  ┌───┐   ┌───┐   ┌───┐       ┌───┐   ┌───┐   ┌───┐
        //  │100├───┤1  │   │ 1 │       │300├───┤1  │   │ * │
        //  └───┘   └───┘   │   │       └───┘   └───┘   │ * │
        //                  │   │                       │ 3 │
        //                  │   │       ┌───┐   ┌───┐   │   │
        //                  │   │       │400├───┤2  │   │   │
        //                  └───┘       └───┘   └───┘   └───┘
        //  WAL:      ▲
        //            │                 ┌───┐   ┌───┐
        //  100───────┘      ┌────────► │200├───┤3  │
        //                   |          └───┘   └───┘
        //  200──────────────┘
        //
        //  300
        //
        //  400

        let is_alive_flush_lock = self.is_alive_flush_lock.handle();

        let flush_op = move || {
            let Some(is_alive_flush_guard) = is_alive_flush_lock.lock_if_alive() else {
                // Segment is removed, skip flush
                log::debug!("Segment was dropped, skip flush");
                return Ok(());
            };

            let flush_components = || {
                // Flush mapping first to prevent having orphan internal ids.

                #[expect(clippy::wildcard_enum_match_arm, reason = "error handling")]
                let wrap_err = |err, what| match err {
                    OperationError::Cancelled { .. } => err,
                    _ => OperationError::service_error(format!("Failed to flush {what}: {err}")),
                };

                // Cancelled = the storage was dropped after flusher capture (e.g. a concurrent
                // vector name deletion). Skip it but keep flushing: aborting mid-sequence would
                // leave the already-flushed components durably ahead of the rest (notably the
                // point versions), breaking the per-point consistency WAL replay relies on.
                // The drop itself is a versioned operation that replay re-applies. Field
                // indexes get the same treatment in `StructPayloadIndex::flusher`.
                let skip_if_cancelled = |result: OperationResult<()>, what| match result {
                    Ok(()) => Ok(()),
                    Err(OperationError::Cancelled { description }) => {
                        log::debug!("Skipping flush of dropped {what}: {description}");
                        Ok(())
                    }
                    Err(err) => Err(wrap_err(err, what)),
                };

                id_tracker_mapping_flusher().map_err(|err| wrap_err(err, "id_tracker mapping"))?;
                for vector_storage_flusher in vector_storage_flushers {
                    skip_if_cancelled(vector_storage_flusher(), "vector_storage")?;
                }
                for quantization_flusher in quantization_flushers {
                    skip_if_cancelled(quantization_flusher(), "quantized vectors")?;
                }
                payload_index_flusher().map_err(|err| wrap_err(err, "payload_index"))?;
                // Id Tracker contains versions of points. We need to flush it after vector_storage and payload_index flush.
                // This is because vector_storage and payload_index flush are not atomic.
                // If payload or vector flush fails, we will be able to recover data from WAL.
                // If Id Tracker flush fails, we are also able to recover data from WAL
                //  by simply overriding data in vector and payload storages.
                // Once versions are saved - points are considered persisted.
                id_tracker_versions_flusher()
                    .map_err(|err| wrap_err(err, "id_tracker versions"))?;

                Ok(())
            };

            match flush_components() {
                // Only continue if all components flushed Ok
                Ok(()) => {}

                // Return early to avoid updating persisted version
                // Flush was cancelled, bypass
                Err(OperationError::Cancelled { description }) => {
                    log::debug!("Segment flush cancelled: {description}");
                    return Ok(());
                }

                // Propagate other errors
                Err(err) => return Err(err),
            }

            let mut current_persisted_version_guard = persisted_version.lock();
            let persisted_version_value_opt = *current_persisted_version_guard;

            if persisted_version_value_opt > state.version {
                debug_assert!(
                    persisted_version_value_opt.is_some(),
                    "Persisted version should never be None if it's greater than state.version"
                );
                // Another flush beat us to it
                return Ok(());
            }

            Self::save_state(&state, &segment_path).map_err(|err| {
                OperationError::service_error(format!("Failed to flush segment state: {err}"))
            })?;

            *current_persisted_version_guard = state.version;
            debug_assert!(state.version.is_some());

            // Keep the guard till the end of the flush to prevent concurrent drop/flushes
            drop(is_alive_flush_guard);

            Ok(())
        };

        Some(Box::new(flush_op))
    }

    fn drop_data(self) -> OperationResult<()> {
        let segment_path = self.segment_path.clone();
        drop(self);
        safe_delete_with_suffix(&segment_path).map_err(|err| {
            OperationError::service_error(format!("Failed to remove segment: {err}"))
        })
    }

    fn data_path(&self) -> PathBuf {
        self.segment_path.clone()
    }
}

impl NonAppendableSegmentEntry for Segment {
    fn delete_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        let internal_id = self
            .id_tracker
            .borrow()
            .internal_id_with_behavior(point_id, DeferredBehavior::WithDeferred);
        let append_only = self.is_append_only();
        match internal_id {
            // Point does already not exist anymore
            None => Ok(false),
            Some(internal_id) => self.handle_point_version_and_failure(
                op_num,
                point_id,
                Some(internal_id),
                |segment| {
                    if append_only {
                        // Tombstone-only: leave payload row and field
                        // indexes at `internal_id` untouched so the on-disk
                        // structures stay append-only. Readers filter via
                        // the id tracker's deleted bitslice.
                        segment.delete_point_tombstone_only(internal_id)?;
                    } else {
                        segment.delete_point_internal(internal_id, hw_counter)?;
                        segment.version_tracker.set_payload(Some(op_num));
                    }

                    Ok((true, Some(internal_id)))
                },
            ),
        }
    }

    fn delete_field_index(&mut self, op_num: u64, key: PayloadKeyTypeRef) -> OperationResult<bool> {
        self.handle_segment_version_and_failure(op_num, |segment| {
            segment.payload_index.borrow_mut().drop_index(key)?;
            segment.version_tracker.set_payload_index_schema(key, None);
            Ok(true)
        })
    }

    fn delete_field_index_if_incompatible(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
        field_schema: &PayloadFieldSchema,
    ) -> OperationResult<bool> {
        self.handle_segment_version_and_failure(op_num, |segment| {
            let is_incompatible = segment
                .payload_index
                .borrow_mut()
                .drop_index_if_incompatible(key, field_schema)?;

            if is_incompatible {
                segment.version_tracker.set_payload_index_schema(key, None);
            }

            Ok(true)
        })
    }

    fn build_field_index(
        &self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
        field_type: &PayloadFieldSchema,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<BuildFieldIndexResult> {
        // Check version without updating it
        if self.version.unwrap_or(0) > op_num {
            return Ok(BuildFieldIndexResult::SkippedByVersion);
        }

        let field_index = match self
            .payload_index
            .borrow()
            .build_index(key, field_type, hw_counter)?
        {
            BuildIndexResult::Built(indexes) => indexes,
            BuildIndexResult::AlreadyBuilt => {
                return Ok(BuildFieldIndexResult::AlreadyExists);
            }
            BuildIndexResult::IncompatibleSchema => {
                // This function expects that incompatible schema is already removed
                return Ok(BuildFieldIndexResult::IncompatibleSchema);
            }
        };

        Ok(BuildFieldIndexResult::Built {
            indexes: field_index,
            schema: field_type.clone(),
        })
    }

    fn apply_field_index(
        &mut self,
        op_num: SeqNumberType,
        key: PayloadKeyType,
        schema: PayloadFieldSchema,
        field_index: Vec<FieldIndex>,
    ) -> OperationResult<bool> {
        self.handle_segment_version_and_failure(op_num, |segment| {
            segment
                .payload_index
                .borrow_mut()
                .apply_index(key.clone(), schema, field_index)?;

            segment
                .version_tracker
                .set_payload_index_schema(&key, Some(op_num));

            Ok(true)
        })
    }

    fn create_vector_name(
        &mut self,
        op_num: SeqNumberType,
        vector_name: &VectorName,
        vector_config: &VectorNameConfig,
    ) -> OperationResult<bool> {
        self.handle_segment_version_and_failure(op_num, |segment| {
            segment.create_vector_name_impl(op_num, vector_name, vector_config)
        })
    }

    fn delete_vector_name(
        &mut self,
        op_num: SeqNumberType,
        vector_name: &VectorName,
    ) -> OperationResult<bool> {
        self.handle_segment_version_and_failure(op_num, |segment| {
            segment.delete_vector_name_impl(op_num, vector_name)
        })
    }
}

impl SegmentEntry for Segment {
    fn upsert_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        mut vectors: NamedVectors,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        debug_assert!(self.is_appendable());
        check_named_vectors(&vectors, &self.segment_config)?;
        vectors.preprocess(|name| self.config().vector_data.get(name).unwrap());
        let stored_internal_point = self
            .id_tracker
            .borrow()
            .internal_id_with_behavior(point_id, DeferredBehavior::WithDeferred);
        match stored_internal_point {
            Some(existing_internal_id) => self.handle_point_mutate(
                op_num,
                point_id,
                existing_internal_id,
                hw_counter,
                |segment, internal_id| {
                    segment.replace_all_vectors(internal_id, op_num, &vectors, hw_counter)?;
                    Ok(true)
                },
                |snapshot_vectors, _payload| {
                    *snapshot_vectors = vectors.clone().into_owned();
                    Ok(true)
                },
            ),
            None => self.handle_point_version_and_failure(op_num, point_id, None, |segment| {
                let new_index =
                    segment.insert_new_vectors(point_id, op_num, &vectors, hw_counter)?;
                Ok((false, Some(new_index)))
            }),
        }
    }

    fn update_vectors(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        mut vectors: NamedVectors,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        check_named_vectors(&vectors, &self.segment_config)?;
        vectors.preprocess(|name| self.config().vector_data.get(name).unwrap());
        let internal_id = self
            .id_tracker
            .borrow()
            .internal_id_with_behavior(point_id, DeferredBehavior::WithDeferred);
        let Some(internal_id) = internal_id else {
            return Err(OperationError::PointIdError {
                missed_point_id: point_id,
            });
        };
        self.handle_point_mutate(
            op_num,
            point_id,
            internal_id,
            hw_counter,
            |segment, internal_id| {
                segment.update_vectors(internal_id, op_num, vectors.clone(), hw_counter)?;
                Ok(true)
            },
            |snapshot_vectors, _payload| {
                snapshot_vectors.merge(vectors.clone().into_owned());
                Ok(true)
            },
        )
    }

    fn delete_vector(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vector_name: &VectorName,
    ) -> OperationResult<bool> {
        check_vector_name(vector_name, &self.segment_config)?;
        let internal_id = self
            .id_tracker
            .borrow()
            .internal_id_with_behavior(point_id, DeferredBehavior::WithDeferred);
        let Some(internal_id) = internal_id else {
            return Err(OperationError::PointIdError {
                missed_point_id: point_id,
            });
        };
        // Was the vector present at the existing id? Used by both paths to
        // report `is_deleted` and gate the version_tracker update so we
        // don't bump it on a no-op.
        let was_present = !self
            .vector_data
            .get(vector_name)
            .ok_or_else(|| OperationError::vector_name_not_exists(vector_name))?
            .vector_storage
            .borrow()
            .is_deleted_vector(internal_id);
        let is_deleted = self.handle_point_mutate(
            op_num,
            point_id,
            internal_id,
            &HardwareCounterCell::disposable(),
            |segment, internal_id| {
                let vector_data = segment
                    .vector_data
                    .get(vector_name)
                    .ok_or_else(|| OperationError::vector_name_not_exists(vector_name))?;
                let mut vector_storage = vector_data.vector_storage.borrow_mut();
                vector_storage.delete_vector(internal_id)
            },
            |snapshot_vectors, _payload| {
                snapshot_vectors.remove_ref(vector_name);
                Ok(was_present)
            },
        )?;
        if is_deleted {
            self.version_tracker.set_vector(vector_name, Some(op_num));
        }
        Ok(is_deleted)
    }

    fn set_full_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        full_payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        let internal_id = self
            .id_tracker
            .borrow()
            .internal_id_with_behavior(point_id, DeferredBehavior::WithDeferred);
        let Some(internal_id) = internal_id else {
            return Err(OperationError::PointIdError {
                missed_point_id: point_id,
            });
        };
        let applied = self.handle_point_mutate(
            op_num,
            point_id,
            internal_id,
            hw_counter,
            |segment, internal_id| {
                segment.payload_index.borrow_mut().overwrite_payload(
                    internal_id,
                    full_payload,
                    hw_counter,
                )?;
                Ok(true)
            },
            |_vectors, snapshot_payload| {
                *snapshot_payload = full_payload.clone();
                Ok(true)
            },
        )?;
        if applied {
            self.version_tracker.set_payload(Some(op_num));
        }
        Ok(applied)
    }

    fn set_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        payload: &Payload,
        key: &Option<JsonPath>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        let internal_id = self
            .id_tracker
            .borrow()
            .internal_id_with_behavior(point_id, DeferredBehavior::WithDeferred);
        let Some(internal_id) = internal_id else {
            return Err(OperationError::PointIdError {
                missed_point_id: point_id,
            });
        };
        let applied = self.handle_point_mutate(
            op_num,
            point_id,
            internal_id,
            hw_counter,
            |segment, internal_id| {
                segment.payload_index.borrow_mut().set_payload(
                    internal_id,
                    payload,
                    key,
                    hw_counter,
                )?;
                Ok(true)
            },
            |_vectors, snapshot_payload| {
                match key {
                    Some(k) => snapshot_payload.merge_by_key(payload, k),
                    None => snapshot_payload.merge(payload),
                }
                Ok(true)
            },
        )?;
        if applied {
            self.version_tracker.set_payload(Some(op_num));
        }
        Ok(applied)
    }

    fn delete_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        key: PayloadKeyTypeRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        let internal_id = self
            .id_tracker
            .borrow()
            .internal_id_with_behavior(point_id, DeferredBehavior::WithDeferred);
        let Some(internal_id) = internal_id else {
            return Err(OperationError::PointIdError {
                missed_point_id: point_id,
            });
        };
        let applied = self.handle_point_mutate(
            op_num,
            point_id,
            internal_id,
            hw_counter,
            |segment, internal_id| {
                segment
                    .payload_index
                    .borrow_mut()
                    .delete_payload(internal_id, key, hw_counter)?;
                Ok(true)
            },
            |_vectors, snapshot_payload| {
                snapshot_payload.remove(key);
                Ok(true)
            },
        )?;
        if applied {
            self.version_tracker.set_payload(Some(op_num));
        }
        Ok(applied)
    }

    fn clear_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        let internal_id = self
            .id_tracker
            .borrow()
            .internal_id_with_behavior(point_id, DeferredBehavior::WithDeferred);
        let Some(internal_id) = internal_id else {
            return Err(OperationError::PointIdError {
                missed_point_id: point_id,
            });
        };
        let applied = self.handle_point_mutate(
            op_num,
            point_id,
            internal_id,
            hw_counter,
            |segment, internal_id| {
                segment
                    .payload_index
                    .borrow_mut()
                    .clear_payload(internal_id, hw_counter)?;
                Ok(true)
            },
            |_vectors, snapshot_payload| {
                *snapshot_payload = Payload::default();
                Ok(true)
            },
        )?;
        if applied {
            self.version_tracker.set_payload(Some(op_num));
        }
        Ok(applied)
    }
}

// The alias is needed because of self_cell limitation.
type BoxedPointIdIterator<'a> = Box<dyn Iterator<Item = PointIdType> + 'a>;

self_cell::self_cell! {
    struct IterPointsIterator<'a> {
        owner: PointMappingsGuard<'a>,
        #[covariant]
        dependent: BoxedPointIdIterator,
    }
}

impl<'a> Iterator for IterPointsIterator<'a> {
    type Item = PointIdType;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, dependent| dependent.next())
    }
}
