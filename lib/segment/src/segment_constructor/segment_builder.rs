use std::cmp;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use ahash::AHasher;
use atomic_refcell::AtomicRefCell;
use bitvec::macros::internal::funty::Integral;
use common::cpu::CpuPermit;
use common::types::PointOffsetType;
use io::storage_version::StorageVersion;
use tempfile::TempDir;
use uuid::Uuid;

use super::{
    create_mutable_id_tracker, create_payload_storage, create_sparse_vector_index,
    create_sparse_vector_storage, create_vector_index, get_payload_index_path,
    get_vector_index_path, get_vector_storage_path, new_segment_path, open_segment_db,
    open_vector_storage,
};
use crate::common::error_logging::LogError;
use crate::common::operation_error::{check_process_stopped, OperationError, OperationResult};
use crate::entry::entry_point::SegmentEntry;
use crate::id_tracker::immutable_id_tracker::ImmutableIdTracker;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
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
    // The temporary segment directory
    temp_dir: TempDir,
    indexed_fields: HashMap<PayloadKeyType, PayloadFieldSchema>,

    // Payload key to defragment data to
    defragment_keys: Vec<PayloadKeyType>,
}

impl SegmentBuilder {
    pub fn new(
        segments_path: &Path,
        temp_dir: &Path,
        segment_config: &SegmentConfig,
    ) -> OperationResult<Self> {
        // When we build a new segment, it is empty at first,
        // so we can ignore the `stopped` flag
        let stopped = AtomicBool::new(false);

        let temp_dir = create_temp_dir(temp_dir)?;

        let database = open_segment_db(temp_dir.path(), segment_config)?;

        let id_tracker = if segment_config.is_appendable() {
            IdTrackerEnum::MutableIdTracker(create_mutable_id_tracker(database.clone())?)
        } else {
            IdTrackerEnum::InMemoryIdTracker(InMemoryIdTracker::new())
        };

        let payload_storage =
            create_payload_storage(database.clone(), segment_config, temp_dir.path())?;

        let mut vector_storages = HashMap::new();

        for (vector_name, vector_config) in &segment_config.vector_data {
            let vector_storage_path = get_vector_storage_path(temp_dir.path(), vector_name);
            let vector_storage = open_vector_storage(
                &database,
                vector_config,
                &stopped,
                &vector_storage_path,
                vector_name,
            )?;

            vector_storages.insert(vector_name.to_owned(), vector_storage);
        }

        for (vector_name, sparse_vector_config) in &segment_config.sparse_vector_data {
            let vector_storage_path = get_vector_storage_path(temp_dir.path(), vector_name);

            let vector_storage = create_sparse_vector_storage(
                database.clone(),
                &vector_storage_path,
                vector_name,
                &sparse_vector_config.storage_type,
                &stopped,
            )?;

            vector_storages.insert(vector_name.to_owned(), vector_storage);
        }

        let destination_path = new_segment_path(segments_path);

        Ok(SegmentBuilder {
            version: Default::default(), // default version is 0
            id_tracker,
            payload_storage,
            vector_storages,
            segment_config: segment_config.clone(),

            destination_path,
            temp_dir,
            indexed_fields: Default::default(),
            defragment_keys: vec![],
        })
    }

    pub fn set_defragment_keys(&mut self, keys: Vec<PayloadKeyType>) {
        self.defragment_keys = keys;
    }

    pub fn remove_indexed_field(&mut self, field: &PayloadKeyType) {
        self.indexed_fields.remove(field);
    }

    pub fn add_indexed_field(&mut self, field: PayloadKeyType, schema: PayloadFieldSchema) {
        self.indexed_fields.insert(field, schema);
    }

    /// Get ordering value from the payload index
    ///
    /// Ordering value is used to sort points to keep points with the same payload together
    /// Under the assumption that points are queried together, this will reduce the number of
    /// random disk reads.
    ///
    /// Note: This value doesn't guarantee strict ordering in ambiguous cases.
    ///       It should only be used in optimization purposes, not for correctness.
    fn _get_ordering_value(internal_id: PointOffsetType, indices: &[FieldIndex]) -> u64 {
        let mut ordering = 0;
        for payload_index in indices {
            match payload_index {
                FieldIndex::IntMapIndex(index) => {
                    if let Some(numbers) = index.get_values(internal_id) {
                        for number in numbers {
                            ordering = ordering.wrapping_add(*number as u64);
                        }
                    }
                    break;
                }
                FieldIndex::KeywordIndex(index) => {
                    if let Some(keywords) = index.get_values(internal_id) {
                        for keyword in keywords {
                            let mut hasher = AHasher::default();
                            keyword.hash(&mut hasher);
                            ordering = ordering.wrapping_add(hasher.finish());
                        }
                    }
                    break;
                }
                FieldIndex::IntIndex(index) => {
                    if let Some(numbers) = index.get_values(internal_id) {
                        for number in numbers {
                            ordering = ordering.wrapping_add(number as u64);
                        }
                    }
                    break;
                }
                FieldIndex::FloatIndex(index) => {
                    if let Some(numbers) = index.get_values(internal_id) {
                        for number in numbers {
                            // Bit-level conversion of f64 to u64 preserves ordering
                            // (for positive numbers)
                            //
                            // 0.001 -> 4562254508917369340
                            // 0.01  -> 4576918229304087675
                            // 0.05  -> 4587366580439587226
                            // 0.1   -> 4591870180066957722
                            // 1     -> 4607182418800017408
                            // 2     -> 4611686018427387904
                            // 10    -> 4621819117588971520
                            ordering = ordering.wrapping_add(number.to_bits());
                        }
                    }
                    break;
                }
                FieldIndex::DatetimeIndex(index) => {
                    if let Some(dates) = index.get_values(internal_id) {
                        for date in dates {
                            ordering = ordering.wrapping_add(date as u64);
                        }
                    }
                    break;
                }
                FieldIndex::UuidMapIndex(index) => {
                    if let Some(ids) = index.get_values(internal_id) {
                        uuid_hash(&mut ordering, ids.copied());
                    }
                    break;
                }
                FieldIndex::UuidIndex(index) => {
                    if let Some(ids) = index.get_values(internal_id) {
                        uuid_hash(&mut ordering, ids);
                    }
                    break;
                }
                FieldIndex::GeoIndex(_) => {}
                FieldIndex::FullTextIndex(_) => {}
                FieldIndex::BoolIndex(_) => {}
            }
        }
        ordering
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
    pub fn update(&mut self, segments: &[&Segment], stopped: &AtomicBool) -> OperationResult<bool> {
        if segments.is_empty() {
            return Ok(true);
        }

        let mut merged_points: HashMap<ExtendedPointId, PositionedPointMetadata> = HashMap::new();

        for (segment_index, segment) in segments.iter().enumerate() {
            for external_id in segment.iter_points() {
                let version = segment.point_version(external_id).unwrap_or(0);
                let internal_id = segment.get_internal_id(external_id).unwrap();
                merged_points
                    .entry(external_id)
                    .and_modify(|entry| {
                        if entry.version < version {
                            entry.segment_index = segment_index;
                            entry.version = version;
                            entry.internal_id = internal_id;
                        }
                    })
                    .or_insert_with(|| PositionedPointMetadata {
                        segment_index,
                        internal_id,
                        external_id,
                        version,
                        ordering: 0,
                    });
            }
        }

        let payloads: Vec<_> = segments.iter().map(|i| i.payload_index.borrow()).collect();

        let mut points_to_insert: Vec<_> = merged_points.into_values().collect();

        for defragment_key in &self.defragment_keys {
            for point_data in &mut points_to_insert {
                let Some(payload_indices) = payloads[point_data.segment_index]
                    .field_indexes
                    .get(defragment_key)
                else {
                    continue;
                };

                point_data.ordering = point_data.ordering.wrapping_add(Self::_get_ordering_value(
                    point_data.internal_id,
                    payload_indices,
                ));
            }
        }

        if !self.defragment_keys.is_empty() {
            points_to_insert.sort_unstable_by_key(|i| i.ordering);
        }

        let src_segment_max_version = segments.iter().map(|i| i.version()).max().unwrap();
        self.version = cmp::max(self.version, src_segment_max_version);

        let vector_storages: Vec<_> = segments.iter().map(|i| &i.vector_data).collect();

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
                (vec, vector_deleted)
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

                let other_payload =
                    payloads[point_data.segment_index].get_payload(old_internal_id)?;

                match self.id_tracker.internal_id(point_data.external_id) {
                    Some(existing_internal_id) => {
                        debug_assert!(
                            false,
                            "This code should not be reachable, cause points were resolved with `merged_points`"
                        );

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
                            self.payload_storage.clear(existing_internal_id)?;

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
                    self.payload_storage.set(new_internal_id, &other_payload)?;
                }
            }
        }

        for payload in payloads {
            for (field, payload_schema) in payload.indexed_fields() {
                self.indexed_fields.insert(field, payload_schema);
            }
        }

        Ok(true)
    }

    pub fn build(
        self,
        mut permit: CpuPermit,
        stopped: &AtomicBool,
    ) -> Result<Segment, OperationError> {
        let (temp_dir, destination_path) = {
            let SegmentBuilder {
                version,
                id_tracker,
                payload_storage,
                mut vector_storages,
                segment_config,
                destination_path,
                temp_dir,
                indexed_fields,
                defragment_keys: _,
            } = self;

            let appendable_flag = segment_config.is_appendable();

            payload_storage.flusher()()?;
            let payload_storage_arc = Arc::new(AtomicRefCell::new(payload_storage));

            let id_tracker = match id_tracker {
                IdTrackerEnum::InMemoryIdTracker(in_memory_id_tracker) => {
                    let (versions, mappings) = in_memory_id_tracker.into_internal();
                    let immutable_id_tracker =
                        ImmutableIdTracker::new(temp_dir.path(), &versions, mappings)?;
                    IdTrackerEnum::ImmutableIdTracker(immutable_id_tracker)
                }
                IdTrackerEnum::MutableIdTracker(_) => id_tracker,
                IdTrackerEnum::ImmutableIdTracker(_) => {
                    unreachable!("ImmutableIdTracker should not be used for building segment")
                }
            };

            id_tracker.mapping_flusher()()?;
            id_tracker.versions_flusher()()?;
            let id_tracker_arc = Arc::new(AtomicRefCell::new(id_tracker));

            let mut quantized_vectors = Self::update_quantization(
                &segment_config,
                &vector_storages,
                temp_dir.path(),
                &permit,
                stopped,
            )?;

            let mut vector_storages_arc = HashMap::new();

            for vector_name in segment_config.vector_data.keys() {
                let Some(vector_storage) = vector_storages.remove(vector_name) else {
                    return Err(OperationError::service_error(format!(
                        "Vector storage for vector name {vector_name} not found on segment build"
                    )));
                };

                vector_storage.flusher()()?;

                let vector_storage_arc = Arc::new(AtomicRefCell::new(vector_storage));

                vector_storages_arc.insert(vector_name.to_owned(), vector_storage_arc);
            }

            for vector_name in segment_config.sparse_vector_data.keys() {
                let Some(vector_storage) = vector_storages.remove(vector_name) else {
                    return Err(OperationError::service_error(format!(
                        "Vector storage for vector name {vector_name} not found on sparse segment build"
                    )));
                };

                vector_storage.flusher()()?;

                let vector_storage_arc = Arc::new(AtomicRefCell::new(vector_storage));

                vector_storages_arc.insert(vector_name.to_owned(), vector_storage_arc);
            }

            let payload_index_path = get_payload_index_path(temp_dir.path());

            let mut payload_index = StructPayloadIndex::open(
                payload_storage_arc,
                id_tracker_arc.clone(),
                vector_storages_arc.clone(),
                &payload_index_path,
                appendable_flag,
            )?;

            for (field, payload_schema) in indexed_fields {
                payload_index.set_indexed(&field, payload_schema)?;
                check_process_stopped(stopped)?;
            }

            payload_index.flusher()()?;
            let payload_index_arc = Arc::new(AtomicRefCell::new(payload_index));

            // Try to lock GPU device.
            #[cfg(feature = "gpu")]
            let gpu_devices_manager = crate::index::hnsw_index::gpu::GPU_DEVICES_MANAGER.read();
            #[cfg(feature = "gpu")]
            let gpu_device = gpu_devices_manager
                .as_ref()
                .map(|devices_manager| devices_manager.lock_device(stopped))
                .transpose()?
                .flatten();
            #[cfg(not(feature = "gpu"))]
            let gpu_device = None;

            // If GPU is enabled, release all CPU cores except one.
            if let Some(_gpu_device) = &gpu_device {
                if permit.num_cpus > 1 {
                    permit.release_count(permit.num_cpus - 1);
                }
            }

            // Arc permit to share it with each vector store
            let permit = Arc::new(permit);

            for (vector_name, vector_config) in &segment_config.vector_data {
                let vector_storage_arc = vector_storages_arc.remove(vector_name).unwrap();
                let vector_index_path = get_vector_index_path(temp_dir.path(), vector_name);
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
                    gpu_device.as_ref(),
                    stopped,
                )?;
            }

            for (vector_name, sparse_vector_config) in &segment_config.sparse_vector_data {
                let vector_index_path = get_vector_index_path(temp_dir.path(), vector_name);

                let vector_storage_arc = vector_storages_arc.remove(vector_name).unwrap();

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
                temp_dir.path(),
            )?;

            // After version is saved, segment can be loaded on restart
            SegmentVersion::save(temp_dir.path())?;
            // All temp data is evicted from RAM
            (temp_dir, destination_path)
        };

        // Move fully constructed segment into collection directory and load back to RAM
        std::fs::rename(temp_dir.into_path(), &destination_path)
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

fn uuid_hash<I>(hash: &mut u64, ids: I)
where
    I: Iterator<Item = u128>,
{
    for id in ids {
        let uuid = Uuid::from_u128(id);

        // Not all Uuid versions hold timestamp data. The most common version, v4 for example is completely
        // random and can't be sorted. To still allow defragmentation, we assume that usually the same
        // version gets used for a payload key and implement an alternative sorting criteria, that just
        // takes the Uuids bytes to group equal Uuids together.
        if let Some(timestamp) = uuid.get_timestamp() {
            *hash = hash.wrapping_add(timestamp.to_gregorian().0);
        } else {
            // First part of u128
            *hash = hash.wrapping_add((id >> 64) as u64);

            // Second part of u128
            *hash = hash.wrapping_add(id as u64);
        }
    }
}

fn create_temp_dir(parent_path: &Path) -> Result<TempDir, OperationError> {
    // Ensure parent path exists
    std::fs::create_dir_all(parent_path)
        .and_then(|_| TempDir::with_prefix_in("segment_builder_", parent_path))
        .map_err(|err| {
            OperationError::service_error(format!(
                "Could not create temp directory in `{}`: {}",
                parent_path.display(),
                err
            ))
        })
}

/// Internal point ID and metadata of a point.
struct PositionedPointMetadata {
    segment_index: usize,
    internal_id: PointOffsetType,
    external_id: ExtendedPointId,
    version: SeqNumberType,
    ordering: u64,
}
