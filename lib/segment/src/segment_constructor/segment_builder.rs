use std::cmp;
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::{AtomicRef, AtomicRefCell};
use common::cpu::CpuPermit;
use common::types::PointOffsetType;
use io::storage_version::StorageVersion;
use parking_lot::RwLock;

use super::{
    create_id_tracker, create_payload_storage, create_sparse_vector_index,
    create_sparse_vector_storage, create_vector_index, get_payload_index_path,
    get_vector_index_path, get_vector_storage_path, new_segment_path, open_segment_db,
    open_vector_storage,
};
use crate::common::error_logging::LogError;
use crate::common::operation_error::{check_process_stopped, OperationError, OperationResult};
use crate::entry::entry_point::SegmentEntry;
use crate::id_tracker::{IdTracker, IdTrackerEnum};
use crate::index::sparse_index::sparse_vector_index::SparseVectorIndexOpenArgs;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::PayloadIndex;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::payload_storage::PayloadStorage;
use crate::segment::{Segment, SegmentVersion};
use crate::segment_constructor::load_segment;
use crate::types::{
    ExtendedPointId, PayloadContainer, PayloadFieldSchema, PayloadKeyType, SegmentConfig,
    SegmentState, SeqNumberType,
};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

/// Structure for constructing segment out of several other segments
pub struct SegmentBuilder {
    version: SeqNumberType,
    id_tracker: IdTrackerEnum,
    payload_storage: PayloadStorageEnum,
    vector_storages: HashMap<String, VectorStorageEnum>,
    segment_config: SegmentConfig,

    // The path, where fully created segment will be moved
    destination_path: PathBuf,
    // Path to the temporary segment directory
    temp_path: PathBuf,
    indexed_fields: HashMap<PayloadKeyType, PayloadFieldSchema>,
}

impl SegmentBuilder {
    pub fn new(
        segment_path: &Path,
        temp_dir: &Path,
        segment_config: &SegmentConfig,
    ) -> OperationResult<Self> {
        // When we build a new segment, it is empty at first,
        // so we can ignore the `stopped` flag
        let stopped = AtomicBool::new(false);

        let temp_path = new_segment_path(temp_dir);

        let database = open_segment_db(&temp_path, segment_config)?;

        let id_tracker = create_id_tracker(database.clone())?;

        let payload_storage = create_payload_storage(database.clone(), segment_config)?;

        let mut vector_storages = HashMap::new();

        for (vector_name, vector_config) in &segment_config.vector_data {
            let vector_storage_path = get_vector_storage_path(&temp_path, vector_name);
            let vector_storage = open_vector_storage(
                &database,
                vector_config,
                &stopped,
                &vector_storage_path,
                vector_name,
            )?;

            vector_storages.insert(vector_name.to_owned(), vector_storage);
        }

        #[allow(clippy::for_kv_map)]
        for (vector_name, _sparse_vector_config) in &segment_config.sparse_vector_data {
            // `_sparse_vector_config` should be used, once we are able to initialize storage with
            // different datatypes

            let vector_storage =
                create_sparse_vector_storage(database.clone(), vector_name, &stopped)?;
            vector_storages.insert(vector_name.to_owned(), vector_storage);
        }

        let destination_path = segment_path.join(temp_path.file_name().unwrap());

        Ok(SegmentBuilder {
            version: Default::default(), // default version is 0
            id_tracker,
            payload_storage,
            vector_storages,
            segment_config: segment_config.clone(),

            destination_path,
            temp_path,
            indexed_fields: Default::default(),
        })
    }

    pub fn remove_indexed_field(&mut self, field: &PayloadKeyType) {
        self.indexed_fields.remove(field);
    }

    pub fn add_indexed_field(&mut self, field: PayloadKeyType, schema: PayloadFieldSchema) {
        self.indexed_fields.insert(field, schema);
    }

    pub fn update(
        &mut self,
        segments: &[Arc<RwLock<Segment>>],
        stopped: &AtomicBool,
    ) -> OperationResult<bool> {
        let mut points_to_apply = vec![];

        let locked_segments: Vec<_> = segments.iter().map(|segment| segment.read()).collect();
        {
            let mut all_point_ids = HashSet::new();
            // let mut id_tracker: Vec<Arc<AtomicRefCell<IdTrackerSS>>> = vec![];

            let mut payload_key = None;

            for segment in segments {
                let segment_guard = segment.read();
                all_point_ids.extend(segment_guard.iter_points());

                if payload_key.is_none() {
                    payload_key = segment_guard
                        .payload_index
                        .borrow()
                        .field_indexes
                        .iter()
                        .nth(0)
                        .map(|i| i.0.clone());
                }
            }

            for point in all_point_ids.iter() {
                let mut max_verison = 0;
                let mut seg_idx = 0;
                let mut internal_id: Option<PointOffsetType> = None;
                for (i, locked_seg) in locked_segments.iter().enumerate() {
                    if !locked_seg.has_point(*point) {
                        continue;
                    }

                    let version = locked_seg.point_version(*point).unwrap_or_default();
                    if version > max_verison {
                        max_verison = version;
                        seg_idx = i;
                        internal_id = locked_seg.get_internal_id(*point);
                    }
                }

                points_to_apply.push((*point, internal_id.unwrap(), seg_idx, max_verison));
            }

            if let Some(payload_key) = payload_key {
                let get_payload =
                    |idx: usize, point: ExtendedPointId| -> Option<serde_json::Value> {
                        locked_segments[idx]
                            .payload(point)
                            .ok()
                            .and_then(|i| i.get_value(&payload_key).get(0).cloned().cloned())
                    };

                points_to_apply.sort_unstable_by(|(a_id, _, a_index, _), (b_id, _, b_index, _)| {
                    let a_payload = get_payload(*a_index, *a_id);
                    let b_payload = get_payload(*b_index, *b_id);
                    let a_hash = a_payload.map(|i| hash_value(&i));
                    let b_hash = b_payload.map(|i| hash_value(&i));
                    a_hash.cmp(&b_hash)
                });
            }
        }

        self.version = cmp::max(
            self.version,
            locked_segments.iter().map(|i| i.version()).max().unwrap(),
        );

        let vector_storages: Vec<_> = locked_segments.iter().map(|i| &i.vector_data).collect();

        let mut new_internal_range = None;
        for (vector_name, vector_storage) in &mut self.vector_storages {
            check_process_stopped(stopped)?;

            let other_vector_storages: Vec<_> = vector_storages
                .iter()
                .map(|i| &i.get(vector_name).unwrap().vector_storage)
                .collect();
            let other_vector_storages: Vec<AtomicRef<VectorStorageEnum>> =
                other_vector_storages.iter().map(|i| i.borrow()).collect();

            let mut iter =
                points_to_apply
                    .iter()
                    .map(|(_, internal_point_id, src_seg_index, _)| {
                        let other_vector_storage = &other_vector_storages[*src_seg_index];

                        let vec = other_vector_storage.get_vector(*internal_point_id);
                        let deleted = other_vector_storage.is_deleted_vector(*internal_point_id);

                        (*internal_point_id, vec, deleted)
                    });
            let internal_range = vector_storage.update_from(&mut iter, stopped)?;
            match new_internal_range.clone() {
                Some(new_internal_range) => {
                    if new_internal_range != internal_range {
                        return Err(OperationError::service_error(
                            "Internal ids range mismatch between self segment vectors and other segment vectors",
                        ));
                    }
                }
                None => new_internal_range = Some(internal_range.clone()),
            }
        }

        let payloads: Vec<_> = locked_segments
            .iter()
            .map(|i| i.payload_index.borrow())
            .collect();

        if let Some(new_internal_range) = new_internal_range {
            assert_eq!(new_internal_range.len(), points_to_apply.len());

            let internal_id_iter = new_internal_range.zip(points_to_apply.iter());

            for (new_internal_id, (external_id, old_internal_id, idx, version)) in internal_id_iter
            {
                check_process_stopped(stopped)?;

                self.id_tracker.set_link(*external_id, new_internal_id)?;
                self.id_tracker
                    .set_internal_version(new_internal_id, *version)?;

                let other_payload = payloads[*idx].payload(*old_internal_id)?;
                // Propagate payload to new segment
                if !other_payload.is_empty() {
                    self.payload_storage
                        .assign(new_internal_id, &other_payload)?;
                }
            }
        }

        for payload in payloads {
            for (field, payload_schema) in payload.indexed_fields() {
                self.indexed_fields.insert(field, payload_schema);
            }
        }

        self.id_tracker.mapping_flusher()()?;
        self.id_tracker.versions_flusher()()?;

        Ok(true)
    }

    /// Update current segment builder with all (not deleted) vectors and payload form `other` segment
    /// Perform index building at the end of update
    ///
    /// # Arguments
    ///
    /// * `other` - segment to add into construction
    ///
    /// # Result
    ///
    /// * `bool` - if `true` - data successfully added, if `false` - process was interrupted
    ///
    /* pub fn update_from(&mut self, other: &Segment, stopped: &AtomicBool) -> OperationResult<bool> {
        self.version = cmp::max(self.version, other.version());

        let other_id_tracker = other.id_tracker.borrow();
        let other_vector_storages: HashMap<_, _> = other
            .vector_data
            .iter()
            .map(|(vector_name, vector_data)| {
                (vector_name.to_owned(), vector_data.vector_storage.borrow())
            })
            .collect();
        let other_payload_index = other.payload_index.borrow();

        let id_tracker = &mut self.id_tracker;

        if self.vector_storages.len() != other_vector_storages.len() {
            return Err(OperationError::service_error(
                format!("Self and other segments have different vector names count. Self count: {}, other count: {}", self.vector_storages.len(), other_vector_storages.len()),
            ));
        }

        let mut new_internal_range = None;
        for (vector_name, vector_storage) in &mut self.vector_storages {
            check_process_stopped(stopped)?;
            let other_vector_storage = other_vector_storages.get(vector_name).ok_or_else(|| {
                OperationError::service_error(format!(
                    "Cannot update from other segment because if missing vector name {vector_name}"
                ))
            })?;
            let internal_range = 0..2;
            /* let internal_range = vector_storage.update_from(
                other_vector_storage,
                &mut other_id_tracker.iter_ids(),
                stopped,
            )?; */
            match new_internal_range.clone() {
                Some(new_internal_range) => {
                    if new_internal_range != internal_range {
                        return Err(OperationError::service_error(
                            "Internal ids range mismatch between self segment vectors and other segment vectors",
                        ));
                    }
                }
                None => new_internal_range = Some(internal_range.clone()),
            }
        }

        if let Some(new_internal_range) = new_internal_range {
            let internal_id_iter = new_internal_range.zip(other_id_tracker.iter_ids());

            for (new_internal_id, old_internal_id) in internal_id_iter {
                check_process_stopped(stopped)?;

                let external_id =
                    if let Some(external_id) = other_id_tracker.external_id(old_internal_id) {
                        external_id
                    } else {
                        log::warn!(
                            "Cannot find external id for internal id {old_internal_id}, skipping"
                        );
                        continue;
                    };

                let other_version = other_id_tracker
                    .internal_version(old_internal_id)
                    .unwrap_or_else(|| {
                        log::debug!(
                            "Internal version not found for internal id {old_internal_id}, using 0"
                        );
                        0
                    });

                match id_tracker.internal_id(external_id) {
                    None => {
                        // New point, just insert
                        id_tracker.set_link(external_id, new_internal_id)?;
                        id_tracker.set_internal_version(new_internal_id, other_version)?;
                        let other_payload = other_payload_index.payload(old_internal_id)?;
                        // Propagate payload to new segment
                        if !other_payload.is_empty() {
                            self.payload_storage
                                .assign(new_internal_id, &other_payload)?;
                        }
                    }
                    Some(existing_internal_id) => {
                        // Point exists in both: newly constructed and old segments, so we need to merge them
                        // Based on version
                        let existing_version =
                            id_tracker.internal_version(existing_internal_id).unwrap();
                        let remove_id = if existing_version < other_version {
                            // Other version is the newest, remove the existing one and replace
                            id_tracker.drop(external_id)?;
                            id_tracker.set_link(external_id, new_internal_id)?;
                            id_tracker.set_internal_version(new_internal_id, other_version)?;
                            self.payload_storage.drop(existing_internal_id)?;
                            let other_payload = other_payload_index.payload(old_internal_id)?;
                            // Propagate payload to new segment
                            if !other_payload.is_empty() {
                                self.payload_storage
                                    .assign(new_internal_id, &other_payload)?;
                            }
                            existing_internal_id
                        } else {
                            // Old version is still good, do not move anything else
                            // Mark newly added vector as removed
                            new_internal_id
                        };
                        for vector_storage in self.vector_storages.values_mut() {
                            vector_storage.delete_vector(remove_id)?;
                        }
                    }
                }
            }
        }

        for (field, payload_schema) in other.payload_index.borrow().indexed_fields() {
            self.indexed_fields.insert(field, payload_schema);
        }

        id_tracker.mapping_flusher()()?;
        id_tracker.versions_flusher()()?;

        Ok(true)
    } */

    pub fn build(self, permit: CpuPermit, stopped: &AtomicBool) -> Result<Segment, OperationError> {
        let (temp_path, destination_path) = {
            let SegmentBuilder {
                version,
                id_tracker,
                payload_storage,
                mut vector_storages,
                segment_config,
                destination_path,
                temp_path,
                indexed_fields,
            } = self;

            let appendable_flag = segment_config.is_appendable();

            payload_storage.flusher()()?;
            let payload_storage_arc = Arc::new(AtomicRefCell::new(payload_storage));

            id_tracker.mapping_flusher()()?;
            id_tracker.versions_flusher()()?;
            let id_tracker_arc = Arc::new(AtomicRefCell::new(id_tracker));

            let payload_index_path = get_payload_index_path(temp_path.as_path());

            let mut payload_index = StructPayloadIndex::open(
                payload_storage_arc,
                id_tracker_arc.clone(),
                &payload_index_path,
                appendable_flag,
            )?;

            for (field, payload_schema) in indexed_fields {
                payload_index.set_indexed(&field, payload_schema)?;
                check_process_stopped(stopped)?;
            }

            payload_index.flusher()()?;
            let payload_index_arc = Arc::new(AtomicRefCell::new(payload_index));

            // Arc permit to share it with each vector store
            let permit = Arc::new(permit);

            let mut quantized_vectors = Self::update_quantization(
                &segment_config,
                &vector_storages,
                temp_path.as_path(),
                &permit,
                stopped,
            )?;

            for (vector_name, vector_config) in &segment_config.vector_data {
                let vector_index_path = get_vector_index_path(&temp_path, vector_name);

                let Some(vector_storage) = vector_storages.remove(vector_name) else {
                    return Err(OperationError::service_error(format!(
                        "Vector storage for vector name {vector_name} not found on segment build"
                    )));
                };

                vector_storage.flusher()()?;

                let vector_storage_arc = Arc::new(AtomicRefCell::new(vector_storage));

                let quantized_vectors = quantized_vectors.remove(vector_name);
                let quantized_vectors_arc = Arc::new(AtomicRefCell::new(quantized_vectors));

                create_vector_index(
                    vector_config,
                    &vector_index_path,
                    id_tracker_arc.clone(),
                    vector_storage_arc,
                    payload_index_arc.clone(),
                    quantized_vectors_arc,
                    Some(permit.clone()),
                    stopped,
                )?;
            }

            for (vector_name, sparse_vector_config) in &segment_config.sparse_vector_data {
                let vector_index_path = get_vector_index_path(&temp_path, vector_name);

                let Some(vector_storage) = vector_storages.remove(vector_name) else {
                    return Err(OperationError::service_error(format!(
                        "Vector storage for vector name {vector_name} not found on sparse segment build"
                    )));
                };

                vector_storage.flusher()()?;

                let vector_storage_arc = Arc::new(AtomicRefCell::new(vector_storage));

                create_sparse_vector_index(SparseVectorIndexOpenArgs {
                    config: sparse_vector_config.index,
                    id_tracker: id_tracker_arc.clone(),
                    vector_storage: vector_storage_arc.clone(),
                    payload_index: payload_index_arc.clone(),
                    path: &vector_index_path,
                    stopped,
                    tick_progress: || (),
                })?;
            }

            // We're done with CPU-intensive tasks, release CPU permit
            debug_assert_eq!(
                Arc::strong_count(&permit),
                1,
                "Must release CPU permit Arc everywhere",
            );
            drop(permit);

            // Finalize the newly created segment by saving config and version
            Segment::save_state(
                &SegmentState {
                    version: Some(version),
                    config: segment_config,
                },
                &temp_path,
            )?;

            // After version is saved, segment can be loaded on restart
            SegmentVersion::save(&temp_path)?;
            // All temp data is evicted from RAM
            (temp_path, destination_path)
        };

        // Move fully constructed segment into collection directory and load back to RAM
        std::fs::rename(temp_path, &destination_path)
            .describe("Moving segment data after optimization")?;

        let loaded_segment = load_segment(&destination_path, stopped)?.ok_or_else(|| {
            OperationError::service_error(format!(
                "Segment loading error: {}",
                destination_path.display()
            ))
        })?;
        Ok(loaded_segment)
    }

    fn update_quantization(
        segment_config: &SegmentConfig,
        vector_storages: &HashMap<String, VectorStorageEnum>,
        temp_path: &Path,
        permit: &CpuPermit,
        stopped: &AtomicBool,
    ) -> OperationResult<HashMap<String, QuantizedVectors>> {
        let config = segment_config.clone();

        let mut quantized_vectors_map = HashMap::new();

        for (vector_name, vector_storage) in vector_storages {
            let Some(vector_config) = config.vector_data.get(vector_name) else {
                continue;
            };

            let is_appendable = vector_config.is_appendable();

            // Don't build quantization for appendable vectors
            if is_appendable {
                continue;
            }

            let max_threads = permit.num_cpus as usize;

            if let Some(quantization) = config.quantization_config(vector_name) {
                let segment_path = temp_path;

                check_process_stopped(stopped)?;

                let vector_storage_path = get_vector_storage_path(segment_path, vector_name);

                let quantized_vectors = QuantizedVectors::create(
                    vector_storage,
                    quantization,
                    &vector_storage_path,
                    max_threads,
                    stopped,
                )?;

                quantized_vectors_map.insert(vector_name.to_owned(), quantized_vectors);
            }
        }
        Ok(quantized_vectors_map)
    }
}

fn hash_value(val: &serde_json::Value) -> u64 {
    let mut hash = DefaultHasher::default();
    hash_value_to(val, &mut hash);
    hash.finish()
}

fn hash_value_to<H: Hasher>(val: &serde_json::Value, hash: &mut H) {
    match val {
        serde_json::Value::Null => (),
        serde_json::Value::Bool(b) => b.hash(hash),
        serde_json::Value::Number(n) => n.hash(hash),
        serde_json::Value::String(s) => s.hash(hash),
        serde_json::Value::Array(a) => {
            for i in a {
                hash_value_to(i, hash);
            }
        }
        serde_json::Value::Object(o) => {
            // The 'preserve_order' feature for serde_json is enabled, so iterating
            // here is ok.
            for (k, v) in o {
                k.hash(hash);
                hash_value_to(v, hash);
            }
        }
    }
}
