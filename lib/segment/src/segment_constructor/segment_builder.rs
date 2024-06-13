use std::cmp;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::cpu::CpuPermit;
use io::storage_version::StorageVersion;

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
use crate::index::{PayloadIndex, VectorIndex};
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::payload_storage::PayloadStorage;
use crate::segment::{Segment, SegmentVersion};
use crate::segment_constructor::load_segment;
use crate::types::{
    PayloadFieldSchema, PayloadKeyType, SegmentConfig, SegmentState, SeqNumberType,
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
    pub fn update_from(&mut self, other: &Segment, stopped: &AtomicBool) -> OperationResult<bool> {
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
            let internal_range = vector_storage.update_from(
                other_vector_storage,
                &mut other_id_tracker.iter_ids(),
                stopped,
            )?;
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
    }

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

                let mut vector_index = create_vector_index(
                    vector_config,
                    &vector_index_path,
                    id_tracker_arc.clone(),
                    vector_storage_arc,
                    payload_index_arc.clone(),
                    quantized_vectors_arc,
                )?;

                vector_index.build_index(permit.clone(), stopped)?;
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

                let mut vector_index = create_sparse_vector_index(SparseVectorIndexOpenArgs {
                    config: sparse_vector_config.index,
                    id_tracker: id_tracker_arc.clone(),
                    vector_storage: vector_storage_arc.clone(),
                    payload_index: payload_index_arc.clone(),
                    path: &vector_index_path,
                    stopped,
                })?;

                vector_index.build_index(permit.clone(), stopped)?;
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
