use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::thread::{self};

use common::tar_ext;
use common::types::TelemetryDetail;
use io::storage_version::VERSION_FILE;
use uuid::Uuid;

use super::Segment;
use crate::common::operation_error::OperationError::TypeInferenceError;
use crate::common::operation_error::{OperationError, OperationResult, SegmentFailedState};
use crate::common::{check_named_vectors, check_query_vectors, check_stopped, check_vector_name};
use crate::data_types::facets::{FacetParams, FacetValue};
use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::order_by::{OrderBy, OrderValue};
use crate::data_types::query_context::{QueryContext, SegmentQueryContext};
use crate::data_types::vectors::{QueryVector, VectorInternal};
use crate::entry::entry_point::SegmentEntry;
use crate::index::field_index::{CardinalityEstimation, FieldIndex};
use crate::index::{PayloadIndex, VectorIndex};
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorage;
use crate::segment::{
    DB_BACKUP_PATH, PAYLOAD_DB_BACKUP_PATH, SEGMENT_STATE_FILE, SNAPSHOT_FILES_PATH, SNAPSHOT_PATH,
};
use crate::telemetry::SegmentTelemetry;
use crate::types::{
    Filter, Payload, PayloadFieldSchema, PayloadIndexInfo, PayloadKeyType, PayloadKeyTypeRef,
    PointIdType, ScoredPoint, SearchParams, SegmentConfig, SegmentInfo, SegmentType, SeqNumberType,
    SnapshotFormat, VectorDataInfo, WithPayload, WithVector,
};
use crate::utils::path::strip_prefix;
use crate::vector_storage::VectorStorage;

/// This is a basic implementation of `SegmentEntry`,
/// meaning that it implements the _actual_ operations with data and not any kind of proxy or wrapping
impl SegmentEntry for Segment {
    fn version(&self) -> SeqNumberType {
        self.version.unwrap_or(0)
    }

    fn point_version(&self, point_id: PointIdType) -> Option<SeqNumberType> {
        let id_tracker = self.id_tracker.borrow();
        id_tracker
            .internal_id(point_id)
            .and_then(|internal_id| id_tracker.internal_version(internal_id))
    }

    fn search_batch(
        &self,
        vector_name: &str,
        query_vectors: &[&QueryVector],
        with_payload: &WithPayload,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        top: usize,
        params: Option<&SearchParams>,
        query_context: &SegmentQueryContext,
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        check_query_vectors(vector_name, query_vectors, &self.segment_config)?;
        let vector_data = &self.vector_data[vector_name];
        let vector_query_context = query_context.get_vector_context(vector_name);
        let internal_results = vector_data.vector_index.borrow().search(
            query_vectors,
            filter,
            top,
            params,
            &vector_query_context,
        )?;

        check_stopped(&vector_query_context.is_stopped())?;

        internal_results
            .into_iter()
            .map(|internal_result| {
                self.process_search_result(internal_result, with_payload, with_vector)
            })
            .collect()
    }

    fn upsert_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        mut vectors: NamedVectors,
    ) -> OperationResult<bool> {
        debug_assert!(self.is_appendable());
        check_named_vectors(&vectors, &self.segment_config)?;
        vectors.preprocess(|name| self.config().vector_data.get(name).unwrap());
        let stored_internal_point = self.id_tracker.borrow().internal_id(point_id);
        self.handle_point_version_and_failure(op_num, stored_internal_point, |segment| {
            if let Some(existing_internal_id) = stored_internal_point {
                segment.replace_all_vectors(existing_internal_id, &vectors)?;
                Ok((true, Some(existing_internal_id)))
            } else {
                let new_index = segment.insert_new_vectors(point_id, &vectors)?;
                Ok((false, Some(new_index)))
            }
        })
    }

    fn delete_point(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        let internal_id = self.id_tracker.borrow().internal_id(point_id);
        match internal_id {
            // Point does already not exist anymore
            None => Ok(false),
            Some(internal_id) => {
                self.handle_point_version_and_failure(op_num, Some(internal_id), |segment| {
                    // Mark point as deleted, drop mapping
                    segment
                        .payload_index
                        .borrow_mut()
                        .clear_payload(internal_id)?;
                    segment.id_tracker.borrow_mut().drop(point_id)?;

                    // Before, we propagated point deletions to also delete its vectors. This turns
                    // out to be problematic because this sometimes makes us loose vector data
                    // because we cannot control the order of segment flushes.
                    // Disabled until we properly fix it or find a better way to clean up old
                    // vectors.
                    //
                    // // Propagate point deletion to all its vectors
                    // for vector_data in segment.vector_data.values() {
                    //     let mut vector_storage = vector_data.vector_storage.borrow_mut();
                    //     vector_storage.delete_vector(internal_id)?;
                    // }

                    Ok((true, Some(internal_id)))
                })
            }
        }
    }

    fn update_vectors(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        mut vectors: NamedVectors,
    ) -> OperationResult<bool> {
        check_named_vectors(&vectors, &self.segment_config)?;
        vectors.preprocess(|name| self.config().vector_data.get(name).unwrap());
        let internal_id = self.id_tracker.borrow().internal_id(point_id);
        match internal_id {
            None => Err(OperationError::PointIdError {
                missed_point_id: point_id,
            }),
            Some(internal_id) => {
                self.handle_point_version_and_failure(op_num, Some(internal_id), |segment| {
                    segment.update_vectors(internal_id, vectors)?;
                    Ok((true, Some(internal_id)))
                })
            }
        }
    }

    fn delete_vector(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        vector_name: &str,
    ) -> OperationResult<bool> {
        check_vector_name(vector_name, &self.segment_config)?;
        let internal_id = self.id_tracker.borrow().internal_id(point_id);
        match internal_id {
            None => Err(OperationError::PointIdError {
                missed_point_id: point_id,
            }),
            Some(internal_id) => {
                self.handle_point_version_and_failure(op_num, Some(internal_id), |segment| {
                    let vector_data = segment.vector_data.get(vector_name).ok_or_else(|| {
                        OperationError::VectorNameNotExists {
                            received_name: vector_name.to_string(),
                        }
                    })?;
                    let mut vector_storage = vector_data.vector_storage.borrow_mut();
                    let is_deleted = vector_storage.delete_vector(internal_id)?;
                    Ok((is_deleted, Some(internal_id)))
                })
            }
        }
    }

    fn set_full_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        full_payload: &Payload,
    ) -> OperationResult<bool> {
        let internal_id = self.id_tracker.borrow().internal_id(point_id);
        self.handle_point_version_and_failure(op_num, internal_id, |segment| match internal_id {
            Some(internal_id) => {
                segment
                    .payload_index
                    .borrow_mut()
                    .overwrite_payload(internal_id, full_payload)?;
                Ok((true, Some(internal_id)))
            }
            None => Err(OperationError::PointIdError {
                missed_point_id: point_id,
            }),
        })
    }

    fn set_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        payload: &Payload,
        key: &Option<JsonPath>,
    ) -> OperationResult<bool> {
        let internal_id = self.id_tracker.borrow().internal_id(point_id);
        self.handle_point_version_and_failure(op_num, internal_id, |segment| match internal_id {
            Some(internal_id) => {
                segment
                    .payload_index
                    .borrow_mut()
                    .set_payload(internal_id, payload, key)?;
                Ok((true, Some(internal_id)))
            }
            None => Err(OperationError::PointIdError {
                missed_point_id: point_id,
            }),
        })
    }

    fn delete_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<bool> {
        let internal_id = self.id_tracker.borrow().internal_id(point_id);
        self.handle_point_version_and_failure(op_num, internal_id, |segment| match internal_id {
            Some(internal_id) => {
                segment
                    .payload_index
                    .borrow_mut()
                    .delete_payload(internal_id, key)?;
                Ok((true, Some(internal_id)))
            }
            None => Err(OperationError::PointIdError {
                missed_point_id: point_id,
            }),
        })
    }

    fn clear_payload(
        &mut self,
        op_num: SeqNumberType,
        point_id: PointIdType,
    ) -> OperationResult<bool> {
        let internal_id = self.id_tracker.borrow().internal_id(point_id);
        self.handle_point_version_and_failure(op_num, internal_id, |segment| match internal_id {
            Some(internal_id) => {
                segment
                    .payload_index
                    .borrow_mut()
                    .clear_payload(internal_id)?;
                Ok((true, Some(internal_id)))
            }
            None => Err(OperationError::PointIdError {
                missed_point_id: point_id,
            }),
        })
    }

    fn vector(
        &self,
        vector_name: &str,
        point_id: PointIdType,
    ) -> OperationResult<Option<VectorInternal>> {
        check_vector_name(vector_name, &self.segment_config)?;
        let internal_id = self.lookup_internal_id(point_id)?;
        let vector_opt = self.vector_by_offset(vector_name, internal_id)?;
        Ok(vector_opt)
    }

    fn all_vectors(&self, point_id: PointIdType) -> OperationResult<NamedVectors> {
        let mut result = NamedVectors::default();
        for vector_name in self.vector_data.keys() {
            if let Some(vec) = self.vector(vector_name, point_id)? {
                result.insert(vector_name.clone(), vec);
            }
        }
        Ok(result)
    }

    fn payload(&self, point_id: PointIdType) -> OperationResult<Payload> {
        let internal_id = self.lookup_internal_id(point_id)?;
        self.payload_by_offset(internal_id)
    }

    fn iter_points(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        // Sorry for that, but I didn't find any way easier.
        // If you try simply return iterator - it won't work because AtomicRef should exist
        // If you try to make callback instead - you won't be able to create <dyn SegmentEntry>
        // Attempt to create return borrowed value along with iterator failed because of insane lifetimes
        unsafe { self.id_tracker.as_ptr().as_ref().unwrap().iter_external() }
    }

    fn read_filtered<'a>(
        &'a self,
        offset: Option<PointIdType>,
        limit: Option<usize>,
        filter: Option<&'a Filter>,
        is_stopped: &AtomicBool,
    ) -> Vec<PointIdType> {
        match filter {
            None => self.read_by_id_stream(offset, limit),
            Some(condition) => {
                if self.should_pre_filter(condition, limit) {
                    self.filtered_read_by_index(offset, limit, condition, is_stopped)
                } else {
                    self.filtered_read_by_id_stream(offset, limit, condition, is_stopped)
                }
            }
        }
    }

    fn read_ordered_filtered<'a>(
        &'a self,
        limit: Option<usize>,
        filter: Option<&'a Filter>,
        order_by: &'a OrderBy,
        is_stopped: &AtomicBool,
    ) -> OperationResult<Vec<(OrderValue, PointIdType)>> {
        match filter {
            None => self.filtered_read_by_value_stream(order_by, limit, None, is_stopped),
            Some(filter) => {
                if self.should_pre_filter(filter, limit) {
                    self.filtered_read_by_index_ordered(order_by, limit, filter, is_stopped)
                } else {
                    self.filtered_read_by_value_stream(order_by, limit, Some(filter), is_stopped)
                }
            }
        }
    }

    fn read_random_filtered(
        &self,
        limit: usize,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
    ) -> Vec<PointIdType> {
        match filter {
            None => self.read_by_random_id(limit),
            Some(condition) => {
                if self.should_pre_filter(condition, Some(limit)) {
                    self.filtered_read_by_index_shuffled(limit, condition, is_stopped)
                } else {
                    self.filtered_read_by_random_stream(limit, condition, is_stopped)
                }
            }
        }
    }

    fn read_range(&self, from: Option<PointIdType>, to: Option<PointIdType>) -> Vec<PointIdType> {
        let id_tracker = self.id_tracker.borrow();
        let iterator = id_tracker.iter_from(from).map(|x| x.0);
        match to {
            None => iterator.collect(),
            Some(to_id) => iterator.take_while(|x| *x < to_id).collect(),
        }
    }

    fn has_point(&self, point_id: PointIdType) -> bool {
        self.id_tracker.borrow().internal_id(point_id).is_some()
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

    fn available_vectors_size_in_bytes(&self, vector_name: &str) -> OperationResult<usize> {
        check_vector_name(vector_name, &self.segment_config)?;
        Ok(self.vector_data[vector_name]
            .vector_storage
            .borrow()
            .size_of_available_vectors_in_bytes())
    }

    fn estimate_point_count<'a>(&'a self, filter: Option<&'a Filter>) -> CardinalityEstimation {
        match filter {
            None => {
                let available = self.available_point_count();
                CardinalityEstimation {
                    primary_clauses: vec![],
                    min: available,
                    exp: available,
                    max: available,
                }
            }
            Some(filter) => {
                let payload_index = self.payload_index.borrow();
                payload_index.estimate_cardinality(filter)
            }
        }
    }

    fn unique_values(
        &self,
        key: &JsonPath,
        filter: Option<&Filter>,
        is_stopped: &AtomicBool,
    ) -> OperationResult<std::collections::BTreeSet<FacetValue>> {
        self.facet_values(key, filter, is_stopped)
    }

    fn facet(
        &self,
        request: &FacetParams,
        is_stopped: &AtomicBool,
    ) -> OperationResult<HashMap<FacetValue, usize>> {
        self.approximate_facet(request, is_stopped)
    }

    fn segment_type(&self) -> SegmentType {
        self.segment_type
    }

    fn size_info(&self) -> SegmentInfo {
        let num_vectors = self
            .vector_data
            .values()
            .map(|data| data.vector_storage.borrow().available_vector_count())
            .sum();

        let mut total_average_vectors_size_bytes: usize = 0;

        let vector_data_info: HashMap<_, _> = self
            .vector_data
            .iter()
            .map(|(key, vector_data)| {
                let vector_storage = vector_data.vector_storage.borrow();
                let num_vectors = vector_storage.available_vector_count();
                let vector_index = vector_data.vector_index.borrow();
                let is_indexed = vector_index.is_index();

                let average_vector_size_bytes = if num_vectors > 0 {
                    vector_storage.size_of_available_vectors_in_bytes() / num_vectors
                } else {
                    0
                };
                total_average_vectors_size_bytes += average_vector_size_bytes;

                let vector_data_info = VectorDataInfo {
                    num_vectors,
                    num_indexed_vectors: if is_indexed {
                        vector_index.indexed_vector_count()
                    } else {
                        0
                    },
                    num_deleted_vectors: vector_storage.deleted_vector_count(),
                };
                (key.to_string(), vector_data_info)
            })
            .collect();

        let num_indexed_vectors = if self.segment_type == SegmentType::Indexed {
            self.vector_data
                .values()
                .map(|data| data.vector_index.borrow().indexed_vector_count())
                .sum()
        } else {
            0
        };

        let num_points = self.available_point_count();

        let vectors_size_bytes = total_average_vectors_size_bytes * num_points;

        // Unwrap and default to 0 here because the RocksDB storage is the only faillible one, and we will remove it eventually.
        let payloads_size_bytes = self
            .payload_storage
            .borrow()
            .get_storage_size_bytes()
            .unwrap_or(0);

        SegmentInfo {
            segment_type: self.segment_type,
            num_vectors,
            num_indexed_vectors,
            num_points: self.available_point_count(),
            num_deleted_vectors: self.deleted_point_count(),
            vectors_size_bytes,  // Considers vector storage, but not indices
            payloads_size_bytes, // Considers payload storage, but not indices
            ram_usage_bytes: 0,  // ToDo: Implement
            disk_usage_bytes: 0, // ToDo: Implement
            is_appendable: self.appendable_flag,
            index_schema: HashMap::new(),
            vector_data: vector_data_info,
        }
    }

    fn info(&self) -> SegmentInfo {
        let payload_index = self.payload_index.borrow();
        let schema = payload_index
            .indexed_fields()
            .into_iter()
            .map(|(key, index_schema)| {
                let points_count = payload_index.indexed_points(&key);
                (key, PayloadIndexInfo::new(index_schema, points_count))
            })
            .collect();

        let mut info = self.size_info();
        info.index_schema = schema;

        info
    }

    fn config(&self) -> &SegmentConfig {
        &self.segment_config
    }

    fn is_appendable(&self) -> bool {
        self.appendable_flag
    }

    fn flush(&self, sync: bool, force: bool) -> OperationResult<SeqNumberType> {
        let current_persisted_version: Option<SeqNumberType> = *self.persisted_version.lock();
        if !sync && self.is_background_flushing() {
            return Ok(current_persisted_version.unwrap_or(0));
        }

        let mut background_flush_lock = self.lock_flushing()?;
        match (self.version, current_persisted_version) {
            (None, _) => {
                // Segment is empty, nothing to flush
                return Ok(current_persisted_version.unwrap_or(0));
            }
            (Some(version), Some(persisted_version)) => {
                if !force && version == persisted_version {
                    // Segment is already flushed
                    return Ok(persisted_version);
                }
            }
            (_, _) => {}
        }

        let vector_storage_flushers: Vec<_> = self
            .vector_data
            .values()
            .map(|v| v.vector_storage.borrow().flusher())
            .collect();
        let state = self.get_state();
        let current_path = self.current_path.clone();
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

        let flush_op = move || {
            // Flush mapping first to prevent having orphan internal ids.
            id_tracker_mapping_flusher().map_err(|err| {
                OperationError::service_error(format!("Failed to flush id_tracker mapping: {err}"))
            })?;
            for vector_storage_flusher in vector_storage_flushers {
                vector_storage_flusher().map_err(|err| {
                    OperationError::service_error(format!("Failed to flush vector_storage: {err}"))
                })?;
            }
            payload_index_flusher().map_err(|err| {
                OperationError::service_error(format!("Failed to flush payload_index: {err}"))
            })?;
            // Id Tracker contains versions of points. We need to flush it after vector_storage and payload_index flush.
            // This is because vector_storage and payload_index flush are not atomic.
            // If payload or vector flush fails, we will be able to recover data from WAL.
            // If Id Tracker flush fails, we are also able to recover data from WAL
            //  by simply overriding data in vector and payload storages.
            // Once versions are saved - points are considered persisted.
            id_tracker_versions_flusher().map_err(|err| {
                OperationError::service_error(format!("Failed to flush id_tracker versions: {err}"))
            })?;
            Self::save_state(&state, &current_path).map_err(|err| {
                OperationError::service_error(format!("Failed to flush segment state: {err}"))
            })?;
            *persisted_version.lock() = state.version;

            debug_assert!(state.version.is_some());
            Ok(state.version.unwrap_or(0))
        };

        if sync {
            flush_op()
        } else {
            *background_flush_lock = Some(
                thread::Builder::new()
                    .name("background_flush".to_string())
                    .spawn(flush_op)
                    .unwrap(),
            );
            Ok(current_persisted_version.unwrap_or(0))
        }
    }

    fn drop_data(self) -> OperationResult<()> {
        let current_path = self.current_path.clone();
        drop(self);
        let mut deleted_path = current_path.clone();
        deleted_path.set_extension("deleted");
        fs::rename(&current_path, &deleted_path)?;
        fs::remove_dir_all(&deleted_path).map_err(|err| {
            OperationError::service_error(format!(
                "Can't remove segment data at {}, error: {}",
                deleted_path.to_str().unwrap_or_default(),
                err
            ))
        })
    }

    fn data_path(&self) -> PathBuf {
        self.current_path.clone()
    }

    fn delete_field_index(&mut self, op_num: u64, key: PayloadKeyTypeRef) -> OperationResult<bool> {
        self.handle_segment_version_and_failure(op_num, |segment| {
            segment.payload_index.borrow_mut().drop_index(key)?;
            Ok(true)
        })
    }

    fn build_field_index(
        &self,
        op_num: SeqNumberType,
        key: PayloadKeyTypeRef,
        field_type: Option<&PayloadFieldSchema>,
    ) -> OperationResult<Option<(PayloadFieldSchema, Vec<FieldIndex>)>> {
        // Check version without updating it
        if self.version.unwrap_or(0) > op_num {
            return Ok(None);
        }

        match field_type {
            Some(schema) => {
                let res = self
                    .payload_index
                    .borrow()
                    .build_index(key, schema)?
                    .map(|field_index| (schema.to_owned(), field_index));

                Ok(res)
            }
            None => match self.infer_from_payload_data(key)? {
                None => Err(TypeInferenceError {
                    field_name: key.clone(),
                }),
                Some(schema_type) => {
                    let schema = schema_type.into();

                    let res = self
                        .payload_index
                        .borrow()
                        .build_index(key, &schema)?
                        .map(|field_index| (schema, field_index));

                    Ok(res)
                }
            },
        }
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
                .apply_index(key, schema, field_index)?;
            Ok(true)
        })
    }

    fn get_indexed_fields(&self) -> HashMap<PayloadKeyType, PayloadFieldSchema> {
        self.payload_index.borrow().indexed_fields()
    }

    fn check_error(&self) -> Option<SegmentFailedState> {
        self.error_status.clone()
    }

    fn delete_filtered<'a>(
        &'a mut self,
        op_num: SeqNumberType,
        filter: &'a Filter,
    ) -> OperationResult<usize> {
        let mut deleted_points = 0;
        let is_stopped = AtomicBool::new(false);
        for point_id in self.read_filtered(None, None, Some(filter), &is_stopped) {
            deleted_points += usize::from(self.delete_point(op_num, point_id)?);
        }

        Ok(deleted_points)
    }

    fn vector_names(&self) -> HashSet<String> {
        self.vector_data.keys().cloned().collect()
    }

    fn take_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        snapshotted_segments: &mut HashSet<String>,
    ) -> OperationResult<()> {
        let segment_id = self
            .current_path
            .file_stem()
            .and_then(|f| f.to_str())
            .unwrap();

        if !snapshotted_segments.insert(segment_id.to_string()) {
            // Already snapshotted.
            return Ok(());
        }

        log::debug!("Taking snapshot of segment {:?}", self.current_path);

        // flush segment to capture latest state
        self.flush(true, false)?;

        match format {
            SnapshotFormat::Ancient => {
                debug_assert!(false, "Unsupported snapshot format: {format:?}");
                return Err(OperationError::service_error(format!(
                    "Unsupported snapshot format: {format:?}"
                )));
            }
            SnapshotFormat::Regular => {
                tar.blocking_write_fn(Path::new(&format!("{segment_id}.tar")), |writer| {
                    let tar = tar_ext::BuilderExt::new_streaming_borrowed(writer);
                    let tar = tar.descend(Path::new(SNAPSHOT_PATH))?;
                    snapshot_files(self, temp_path, &tar)
                })??;
            }
            SnapshotFormat::Streamable => {
                let tar = tar.descend(Path::new(&segment_id))?;
                snapshot_files(self, temp_path, &tar)?;
            }
        }

        Ok(())
    }

    fn get_telemetry_data(&self, detail: TelemetryDetail) -> SegmentTelemetry {
        let vector_index_searches: Vec<_> = self
            .vector_data
            .iter()
            .map(|(k, v)| {
                let mut telemetry = v.vector_index.borrow().get_telemetry_data(detail);
                telemetry.index_name = Some(k.clone());
                telemetry
            })
            .collect();

        SegmentTelemetry {
            info: self.info(),
            config: self.config().clone(),
            vector_index_searches,
            payload_field_indices: self.payload_index.borrow().get_telemetry_data(),
        }
    }

    fn fill_query_context(&self, query_context: &mut QueryContext) {
        query_context.add_available_point_count(self.available_point_count());

        for (vector_name, idf) in query_context.mut_idf().iter_mut() {
            if let Some(vector_data) = self.vector_data.get(vector_name) {
                vector_data.vector_index.borrow().fill_idf_statistics(idf);
            }
        }
    }
}

fn snapshot_files(
    segment: &Segment,
    temp_path: &Path,
    tar: &tar_ext::BuilderExt<impl Write + Seek>,
) -> OperationResult<()> {
    // use temp_path for intermediary files
    let temp_path = temp_path.join(format!("segment-{}", Uuid::new_v4()));
    let db_backup_path = temp_path.join(DB_BACKUP_PATH);
    let payload_index_db_backup_path = temp_path.join(PAYLOAD_DB_BACKUP_PATH);

    {
        let db = segment.database.read();
        crate::rocksdb_backup::create(&db, &db_backup_path)?;
    }

    segment
        .payload_index
        .borrow()
        .take_database_snapshot(&payload_index_db_backup_path)?;

    tar.blocking_append_dir_all(&temp_path, Path::new(""))?;

    // remove tmp directory in background
    let _ = thread::spawn(move || {
        let res = fs::remove_dir_all(&temp_path);
        if let Err(err) = res {
            log::error!(
                "Failed to remove tmp directory at {}: {err:?}",
                temp_path.display(),
            );
        }
    });

    let tar = tar.descend(Path::new(SNAPSHOT_FILES_PATH))?;
    for vector_data in segment.vector_data.values() {
        for file in vector_data.vector_index.borrow().files() {
            tar.blocking_append_file(&file, strip_prefix(&file, &segment.current_path)?)?;
        }

        for file in vector_data.vector_storage.borrow().files() {
            tar.blocking_append_file(&file, strip_prefix(&file, &segment.current_path)?)?;
        }

        if let Some(quantized_vectors) = vector_data.quantized_vectors.borrow().as_ref() {
            for file in quantized_vectors.files() {
                tar.blocking_append_file(&file, strip_prefix(&file, &segment.current_path)?)?;
            }
        }
    }

    for file in segment.payload_index.borrow().files() {
        tar.blocking_append_file(&file, strip_prefix(&file, &segment.current_path)?)?;
    }

    for file in segment.payload_storage.borrow().files() {
        tar.blocking_append_file(&file, strip_prefix(&file, &segment.current_path)?)?;
    }

    for file in segment.id_tracker.borrow().files() {
        tar.blocking_append_file(&file, strip_prefix(&file, &segment.current_path)?)?;
    }

    tar.blocking_append_file(
        &segment.current_path.join(SEGMENT_STATE_FILE),
        Path::new(SEGMENT_STATE_FILE),
    )?;

    tar.blocking_append_file(
        &segment.current_path.join(VERSION_FILE),
        Path::new(VERSION_FILE),
    )?;

    Ok(())
}
