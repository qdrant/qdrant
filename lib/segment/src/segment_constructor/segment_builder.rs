use std::cmp;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use ahash::AHasher;
use atomic_refcell::AtomicRefCell;
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
use crate::index::field_index::FieldIndex;
use crate::index::sparse_index::sparse_vector_index::SparseVectorIndexOpenArgs;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::PayloadIndex;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::payload_storage::PayloadStorage;
use crate::segment::{Segment, SegmentVersion};
use crate::segment_constructor::load_segment;
use crate::types::{
    ExtendedPointId, PayloadFieldSchema, PayloadKeyType, SegmentConfig, SegmentState, SeqNumberType,
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

    // Payload key to deframent data to
    defragment_key: Option<PayloadKeyType>,
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
            defragment_key: None,
        })
    }

    pub fn set_defragment_key(&mut self, key: PayloadKeyType) {
        self.defragment_key = Some(key);
    }

    pub fn remove_indexed_field(&mut self, field: &PayloadKeyType) {
        self.indexed_fields.remove(field);
    }

    pub fn add_indexed_field(&mut self, field: PayloadKeyType, schema: PayloadFieldSchema) {
        self.indexed_fields.insert(field, schema);
    }

    /// Update current segment builder with all (not deleted) vectors and payload from `segments`.
    /// Also defragments if the `defragment_key` is set.
    /// However only points in the same call get defragmented and grouped together.
    /// Therefore this function should only be called once, unless this behavior is desired.
    ///
    /// # Result
    ///
    /// * `bool` - if `true` - data successfully added, if `false` - process was interrupted
    ///
    pub fn update(
        &mut self,
        segments: &[Arc<RwLock<Segment>>],
        stopped: &AtomicBool,
    ) -> OperationResult<bool> {
        if segments.is_empty() {
            return Ok(true);
        }

        let segment_guards: Vec<_> = segments.iter().map(|segment| segment.read()).collect();

        let mut merged_points: HashMap<ExtendedPointId, PositionedPointMetadata> = HashMap::new();

        for (segment_index, segment) in segment_guards.iter().enumerate() {
            for external_id in segment.iter_points() {
                let version = segment.point_version(external_id).unwrap_or(0);
                merged_points
                    .entry(external_id)
                    .and_modify(|entry| {
                        if entry.version < version {
                            entry.segment_index = segment_index;
                            entry.version = version;
                        }
                    })
                    .or_insert_with(|| {
                        let internal_id = segment.get_internal_id(external_id).unwrap();
                        PositionedPointMetadata {
                            segment_index,
                            internal_id,
                            version,
                        }
                    });
            }
        }

        let payloads: Vec<_> = segment_guards
            .iter()
            .map(|i| i.payload_index.borrow())
            .collect();

        let mut points_to_insert: Vec<_> = merged_points
            .into_iter()
            .map(|(external_id, pd)| {
                let mut hasher = AHasher::default();

                // If payload key is set, sort the points to add. For this we fetch the payload and
                // hash it.
                let payload_index = self.defragment_key.as_ref().and_then(|defragment_key| {
                    payloads[pd.segment_index].field_indexes.get(defragment_key)
                });
                if let Some(payload_indexes) = payload_index {
                    // Try to find an index we can use for hashing.
                    for payload_index in payload_indexes {
                        match payload_index {
                            FieldIndex::IntMapIndex(index) => {
                                if let Some(numbers) = index.get_values(pd.internal_id) {
                                    for number in numbers {
                                        number.hash(&mut hasher);
                                    }
                                }
                                break;
                            }
                            FieldIndex::KeywordIndex(index) => {
                                if let Some(keywords) = index.get_values(pd.internal_id) {
                                    for keyword in keywords {
                                        keyword.hash(&mut hasher);
                                    }
                                }
                                break;
                            }
                            _ => (),
                        }
                    }
                }

                PositionedPointMetadataOrderable {
                    segment_index: pd.segment_index,
                    internal_id: pd.internal_id,
                    external_id,
                    version: pd.version,
                    hash: hasher.finish(),
                }
            })
            .collect();

        points_to_insert.sort_unstable_by_key(|i| i.hash);

        let src_segment_max_version = segment_guards.iter().map(|i| i.version()).max().unwrap();
        self.version = cmp::max(self.version, src_segment_max_version);

        let vector_storages: Vec<_> = segment_guards.iter().map(|i| &i.vector_data).collect();

        let mut new_internal_range = None;
        for (vector_name, vector_storage) in &mut self.vector_storages {
            check_process_stopped(stopped)?;

            let other_vector_storages = vector_storages
                .iter()
                .map(|i| {
                    let other_vector_storage = i.get(vector_name).ok_or_else(|| {
                        OperationError::service_error(format!(
                    "Cannot update from other segment because if missing vector name {vector_name}"
                        ))
                    })?;

                    Ok(other_vector_storage.vector_storage.borrow())
                })
                .collect::<Result<Vec<_>, OperationError>>()?;

            let mut iter = points_to_insert.iter().map(|point_data| {
                let other_vector_storage = &other_vector_storages[point_data.segment_index];
                let vec = other_vector_storage.get_vector(point_data.internal_id);
                let vector_deleted = other_vector_storage.is_deleted_vector(point_data.internal_id);
                (point_data.internal_id, vec, vector_deleted)
            });

            let internal_range = vector_storage.update_from(&mut iter, stopped)?;

            match &new_internal_range {
                Some(new_internal_range) => {
                    if new_internal_range != &internal_range {
                        return Err(OperationError::service_error(
                            "Internal ids range mismatch between self segment vectors and other segment vectors",
                        ));
                    }
                }
                None => new_internal_range = Some(internal_range),
            }
        }

        if let Some(new_internal_range) = new_internal_range {
            let internal_id_iter = new_internal_range.zip(points_to_insert.iter());

            for (new_internal_id, point_data) in internal_id_iter {
                check_process_stopped(stopped)?;

                let old_internal_id = point_data.internal_id;

                let other_payload = payloads[point_data.segment_index].payload(old_internal_id)?;

                match self.id_tracker.internal_id(point_data.external_id) {
                    Some(existing_internal_id) => {
                        let existing_external_version = self
                            .id_tracker
                            .internal_version(existing_internal_id)
                            .unwrap();

                        let remove_id = if existing_external_version < point_data.version {
                            // Other version is the newest, remove the existing one and replace
                            self.id_tracker.drop(point_data.external_id)?;
                            self.id_tracker
                                .set_link(point_data.external_id, new_internal_id)?;
                            self.id_tracker
                                .set_internal_version(new_internal_id, point_data.version)?;
                            self.payload_storage.drop(existing_internal_id)?;

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
                    None => {
                        self.id_tracker
                            .set_link(point_data.external_id, new_internal_id)?;
                        self.id_tracker
                            .set_internal_version(new_internal_id, point_data.version)?;
                    }
                }

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
                defragment_key: _,
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

/// Internal point ID and metadata of a point.
struct PositionedPointMetadata {
    segment_index: usize,
    internal_id: PointOffsetType,
    version: SeqNumberType,
}

/// All Point ids and metadata of a point including external id and a hash to sort it.
struct PositionedPointMetadataOrderable {
    segment_index: usize,
    internal_id: PointOffsetType,
    external_id: ExtendedPointId,
    version: SeqNumberType,
    hash: u64,
}
