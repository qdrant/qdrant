use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic_refcell::AtomicRefCell;
use common::budget::ResourcePermit;
use common::flags::FeatureFlags;
use fs_err as fs;
use fs_err::File;
use io::storage_version::StorageVersion;
use log::info;
use parking_lot::Mutex;
#[cfg(feature = "rocksdb")]
use parking_lot::RwLock;
use rand::Rng;
#[cfg(feature = "rocksdb")]
use rocksdb::DB;
use serde::Deserialize;
use uuid::Uuid;

#[cfg(feature = "rocksdb")]
use super::rocksdb_builder::RocksDbBuilder;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::vectors::DEFAULT_VECTOR_NAME;
use crate::id_tracker::immutable_id_tracker::ImmutableIdTracker;
use crate::id_tracker::mutable_id_tracker::MutableIdTracker;
#[cfg(feature = "rocksdb")]
use crate::id_tracker::simple_id_tracker::SimpleIdTracker;
use crate::id_tracker::{IdTracker, IdTrackerEnum, IdTrackerSS};
use crate::index::VectorIndexEnum;
use crate::index::hnsw_index::gpu::gpu_devices_manager::LockedGpuDevice;
use crate::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use crate::index::plain_vector_index::PlainVectorIndex;
use crate::index::sparse_index::sparse_index_config::SparseIndexType;
use crate::index::sparse_index::sparse_vector_index::{
    self, SparseVectorIndex, SparseVectorIndexOpenArgs,
};
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::payload_storage::mmap_payload_storage::MmapPayloadStorage;
#[cfg(feature = "rocksdb")]
use crate::payload_storage::on_disk_payload_storage::OnDiskPayloadStorage;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
#[cfg(feature = "rocksdb")]
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::segment::{SEGMENT_STATE_FILE, Segment, SegmentVersion, VectorData};
#[cfg(feature = "rocksdb")]
use crate::types::MultiVectorConfig;
use crate::types::{
    Distance, HnswGlobalConfig, Indexes, PayloadStorageType, SegmentConfig, SegmentState,
    SegmentType, SeqNumberType, SparseVectorStorageType, VectorDataConfig, VectorName,
    VectorStorageDatatype, VectorStorageType,
};
use crate::vector_storage::dense::appendable_dense_vector_storage::{
    open_appendable_in_ram_vector_storage, open_appendable_memmap_vector_storage,
    open_appendable_memmap_vector_storage_byte, open_appendable_memmap_vector_storage_half,
};
use crate::vector_storage::dense::memmap_dense_vector_storage::{
    open_memmap_vector_storage, open_memmap_vector_storage_byte, open_memmap_vector_storage_half,
};
#[cfg(feature = "rocksdb")]
use crate::vector_storage::dense::simple_dense_vector_storage::open_simple_dense_vector_storage;
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::{
    open_appendable_in_ram_multi_vector_storage, open_appendable_memmap_multi_vector_storage,
};
#[cfg(feature = "rocksdb")]
use crate::vector_storage::multi_dense::simple_multi_dense_vector_storage::open_simple_multi_dense_vector_storage;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

pub const PAYLOAD_INDEX_PATH: &str = "payload_index";
pub const VECTOR_STORAGE_PATH: &str = "vector_storage";
pub const VECTOR_INDEX_PATH: &str = "vector_index";

fn sp<T>(t: T) -> Arc<AtomicRefCell<T>> {
    Arc::new(AtomicRefCell::new(t))
}

pub fn get_vector_name_with_prefix(prefix: &str, vector_name: &VectorName) -> String {
    if !vector_name.is_empty() {
        format!("{prefix}-{vector_name}")
    } else {
        prefix.to_owned()
    }
}

pub fn get_vector_storage_path(segment_path: &Path, vector_name: &VectorName) -> PathBuf {
    segment_path.join(get_vector_name_with_prefix(
        VECTOR_STORAGE_PATH,
        vector_name,
    ))
}

pub fn get_vector_index_path(segment_path: &Path, vector_name: &VectorName) -> PathBuf {
    segment_path.join(get_vector_name_with_prefix(VECTOR_INDEX_PATH, vector_name))
}

pub(crate) fn open_vector_storage(
    #[cfg(feature = "rocksdb")] db_builder: &mut RocksDbBuilder,
    vector_config: &VectorDataConfig,
    #[cfg(feature = "rocksdb")] stopped: &AtomicBool,
    vector_storage_path: &Path,
    #[cfg(feature = "rocksdb")] vector_name: &VectorName,
) -> OperationResult<VectorStorageEnum> {
    let storage_element_type = vector_config.datatype.unwrap_or_default();

    match vector_config.storage_type {
        // In memory - RocksDB disabled
        #[cfg(not(feature = "rocksdb"))]
        VectorStorageType::Memory => Err(OperationError::service_error(
            "Failed to load 'Memory' storage type, RocksDB disabled in this Qdrant version",
        )),

        // In memory - RocksDB enabled
        #[cfg(feature = "rocksdb")]
        VectorStorageType::Memory => {
            use crate::common::rocksdb_wrapper::DB_VECTOR_CF;

            let db_column_name = get_vector_name_with_prefix(DB_VECTOR_CF, vector_name);

            if let Some(multi_vec_config) = &vector_config.multivector_config {
                open_simple_multi_dense_vector_storage(
                    storage_element_type,
                    db_builder.require()?,
                    &db_column_name,
                    vector_config.size,
                    vector_config.distance,
                    *multi_vec_config,
                    stopped,
                )
            } else {
                open_simple_dense_vector_storage(
                    storage_element_type,
                    db_builder.require()?,
                    &db_column_name,
                    vector_config.size,
                    vector_config.distance,
                    stopped,
                )
            }
        }
        // Mmap on disk, not appendable
        VectorStorageType::Mmap => {
            if let Some(multi_vec_config) = &vector_config.multivector_config {
                // there are no mmap multi vector storages, appendable only
                open_appendable_memmap_multi_vector_storage(
                    storage_element_type,
                    vector_storage_path,
                    vector_config.size,
                    vector_config.distance,
                    *multi_vec_config,
                )
            } else {
                match storage_element_type {
                    VectorStorageDatatype::Float32 => open_memmap_vector_storage(
                        vector_storage_path,
                        vector_config.size,
                        vector_config.distance,
                    ),
                    VectorStorageDatatype::Uint8 => open_memmap_vector_storage_byte(
                        vector_storage_path,
                        vector_config.size,
                        vector_config.distance,
                    ),
                    VectorStorageDatatype::Float16 => open_memmap_vector_storage_half(
                        vector_storage_path,
                        vector_config.size,
                        vector_config.distance,
                    ),
                }
            }
        }
        // Chunked mmap on disk, appendable
        VectorStorageType::ChunkedMmap => {
            if let Some(multi_vec_config) = &vector_config.multivector_config {
                open_appendable_memmap_multi_vector_storage(
                    storage_element_type,
                    vector_storage_path,
                    vector_config.size,
                    vector_config.distance,
                    *multi_vec_config,
                )
            } else {
                match storage_element_type {
                    VectorStorageDatatype::Float32 => open_appendable_memmap_vector_storage(
                        vector_storage_path,
                        vector_config.size,
                        vector_config.distance,
                    ),
                    VectorStorageDatatype::Uint8 => open_appendable_memmap_vector_storage_byte(
                        vector_storage_path,
                        vector_config.size,
                        vector_config.distance,
                    ),
                    VectorStorageDatatype::Float16 => open_appendable_memmap_vector_storage_half(
                        vector_storage_path,
                        vector_config.size,
                        vector_config.distance,
                    ),
                }
            }
        }
        VectorStorageType::InRamChunkedMmap => {
            if let Some(multi_vec_config) = &vector_config.multivector_config {
                open_appendable_in_ram_multi_vector_storage(
                    storage_element_type,
                    vector_storage_path,
                    vector_config.size,
                    vector_config.distance,
                    *multi_vec_config,
                )
            } else {
                open_appendable_in_ram_vector_storage(
                    storage_element_type,
                    vector_storage_path,
                    vector_config.size,
                    vector_config.distance,
                )
            }
        }
    }
}

pub(crate) fn create_payload_storage(
    #[cfg(feature = "rocksdb")] db_builder: &mut RocksDbBuilder,
    segment_path: &Path,
    config: &SegmentConfig,
) -> OperationResult<PayloadStorageEnum> {
    let payload_storage = match config.payload_storage_type {
        #[cfg(feature = "rocksdb")]
        PayloadStorageType::InMemory => {
            PayloadStorageEnum::from(SimplePayloadStorage::open(db_builder.require()?)?)
        }
        #[cfg(feature = "rocksdb")]
        PayloadStorageType::OnDisk => {
            PayloadStorageEnum::from(OnDiskPayloadStorage::open(db_builder.require()?)?)
        }
        PayloadStorageType::Mmap => PayloadStorageEnum::from(MmapPayloadStorage::open_or_create(
            segment_path.to_path_buf(),
            false,
        )?),
        PayloadStorageType::InRamMmap => PayloadStorageEnum::from(
            MmapPayloadStorage::open_or_create(segment_path.to_path_buf(), true)?,
        ),
    };
    Ok(payload_storage)
}

pub(crate) fn create_mutable_id_tracker(segment_path: &Path) -> OperationResult<MutableIdTracker> {
    MutableIdTracker::open(segment_path)
}

#[cfg(feature = "rocksdb")]
pub(crate) fn create_rocksdb_id_tracker(
    database: Arc<RwLock<DB>>,
) -> OperationResult<SimpleIdTracker> {
    SimpleIdTracker::open(database)
}

pub(crate) fn create_immutable_id_tracker(
    segment_path: &Path,
) -> OperationResult<ImmutableIdTracker> {
    ImmutableIdTracker::open(segment_path)
}

pub(crate) fn get_payload_index_path(segment_path: &Path) -> PathBuf {
    segment_path.join(PAYLOAD_INDEX_PATH)
}

pub(crate) struct VectorIndexOpenArgs<'a> {
    pub path: &'a Path,
    pub id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    pub vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    pub payload_index: Arc<AtomicRefCell<StructPayloadIndex>>,
    pub quantized_vectors: Arc<AtomicRefCell<Option<QuantizedVectors>>>,
}

pub struct VectorIndexBuildArgs<'a, R: Rng + ?Sized> {
    pub permit: Arc<ResourcePermit>,
    /// Vector indices from other segments, used to speed up index building.
    /// May or may not contain the same vectors.
    pub old_indices: &'a [Arc<AtomicRefCell<VectorIndexEnum>>],
    pub gpu_device: Option<&'a LockedGpuDevice<'a>>,
    pub rng: &'a mut R,
    pub stopped: &'a AtomicBool,
    pub hnsw_global_config: &'a HnswGlobalConfig,
    pub feature_flags: FeatureFlags,
}

pub(crate) fn open_vector_index(
    vector_config: &VectorDataConfig,
    open_args: VectorIndexOpenArgs,
) -> OperationResult<VectorIndexEnum> {
    let VectorIndexOpenArgs {
        path,
        id_tracker,
        vector_storage,
        payload_index,
        quantized_vectors,
    } = open_args;
    Ok(match &vector_config.index {
        Indexes::Plain {} => VectorIndexEnum::Plain(PlainVectorIndex::new(
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
        )),
        Indexes::Hnsw(hnsw_config) => VectorIndexEnum::Hnsw(HNSWIndex::open(HnswIndexOpenArgs {
            path,
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
            hnsw_config: hnsw_config.clone(),
        })?),
    })
}

pub(crate) fn build_vector_index<R: Rng + ?Sized>(
    vector_config: &VectorDataConfig,
    open_args: VectorIndexOpenArgs,
    build_args: VectorIndexBuildArgs<R>,
) -> OperationResult<VectorIndexEnum> {
    let VectorIndexOpenArgs {
        path,
        id_tracker,
        vector_storage,
        payload_index,
        quantized_vectors,
    } = open_args;
    Ok(match &vector_config.index {
        Indexes::Plain {} => VectorIndexEnum::Plain(PlainVectorIndex::new(
            id_tracker,
            vector_storage,
            quantized_vectors,
            payload_index,
        )),
        Indexes::Hnsw(hnsw_config) => VectorIndexEnum::Hnsw(HNSWIndex::build(
            HnswIndexOpenArgs {
                path,
                id_tracker,
                vector_storage,
                quantized_vectors,
                payload_index,
                hnsw_config: hnsw_config.clone(),
            },
            build_args,
        )?),
    })
}

#[cfg(feature = "testing")]
pub fn create_sparse_vector_index_test(
    args: SparseVectorIndexOpenArgs<impl FnMut()>,
) -> OperationResult<VectorIndexEnum> {
    create_sparse_vector_index(args)
}

pub(crate) fn create_sparse_vector_index(
    args: SparseVectorIndexOpenArgs<impl FnMut()>,
) -> OperationResult<VectorIndexEnum> {
    let vector_index = match (
        args.config.index_type,
        args.config.datatype.unwrap_or_default(),
        sparse_vector_index::USE_COMPRESSED,
    ) {
        (_, a @ (VectorStorageDatatype::Float16 | VectorStorageDatatype::Uint8), false) => {
            Err(OperationError::ValidationError {
                description: format!("{a:?} datatype is not supported"),
            })?
        }

        (SparseIndexType::MutableRam, _, _) => {
            VectorIndexEnum::SparseRam(SparseVectorIndex::open(args)?)
        }

        // Non-compressed
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float32, false) => {
            VectorIndexEnum::SparseImmutableRam(SparseVectorIndex::open(args)?)
        }
        (SparseIndexType::Mmap, VectorStorageDatatype::Float32, false) => {
            VectorIndexEnum::SparseMmap(SparseVectorIndex::open(args)?)
        }

        // Compressed
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float32, true) => {
            VectorIndexEnum::SparseCompressedImmutableRamF32(SparseVectorIndex::open(args)?)
        }
        (SparseIndexType::Mmap, VectorStorageDatatype::Float32, true) => {
            VectorIndexEnum::SparseCompressedMmapF32(SparseVectorIndex::open(args)?)
        }
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Float16, true) => {
            VectorIndexEnum::SparseCompressedImmutableRamF16(SparseVectorIndex::open(args)?)
        }
        (SparseIndexType::Mmap, VectorStorageDatatype::Float16, true) => {
            VectorIndexEnum::SparseCompressedMmapF16(SparseVectorIndex::open(args)?)
        }
        (SparseIndexType::ImmutableRam, VectorStorageDatatype::Uint8, true) => {
            VectorIndexEnum::SparseCompressedImmutableRamU8(SparseVectorIndex::open(args)?)
        }
        (SparseIndexType::Mmap, VectorStorageDatatype::Uint8, true) => {
            VectorIndexEnum::SparseCompressedMmapU8(SparseVectorIndex::open(args)?)
        }
    };

    Ok(vector_index)
}

pub(crate) fn create_sparse_vector_storage(
    #[cfg(feature = "rocksdb")] db_builder: &mut RocksDbBuilder,
    path: &Path,
    #[cfg(feature = "rocksdb")] vector_name: &VectorName,
    storage_type: &SparseVectorStorageType,
    #[cfg(feature = "rocksdb")] stopped: &AtomicBool,
) -> OperationResult<VectorStorageEnum> {
    match storage_type {
        #[cfg(feature = "rocksdb")]
        SparseVectorStorageType::OnDisk => {
            use crate::common::rocksdb_wrapper::DB_VECTOR_CF;
            use crate::vector_storage::sparse::simple_sparse_vector_storage::open_simple_sparse_vector_storage;

            let db_column_name = get_vector_name_with_prefix(DB_VECTOR_CF, vector_name);
            let storage =
                open_simple_sparse_vector_storage(db_builder.require()?, &db_column_name, stopped)?;

            Ok(storage)
        }
        SparseVectorStorageType::Mmap => {
            let mmap_storage = MmapSparseVectorStorage::open_or_create(path)?;
            Ok(VectorStorageEnum::SparseMmap(mmap_storage))
        }
    }
}

fn create_segment(
    initial_version: Option<SeqNumberType>,
    version: Option<SeqNumberType>,
    segment_path: &Path,
    config: &SegmentConfig,
    stopped: &AtomicBool,
    create: bool,
) -> OperationResult<Segment> {
    #[cfg(feature = "rocksdb")]
    let mut db_builder = RocksDbBuilder::new(segment_path, config)?;

    let payload_storage = sp(create_payload_storage(
        #[cfg(feature = "rocksdb")]
        &mut db_builder,
        segment_path,
        config,
    )?);

    let appendable_flag = config.is_appendable();

    let use_mutable_id_tracker =
        appendable_flag || !ImmutableIdTracker::mappings_file_path(segment_path).is_file();
    let id_tracker = create_segment_id_tracker(
        use_mutable_id_tracker,
        segment_path,
        #[cfg(feature = "rocksdb")]
        &mut db_builder,
    )?;

    let mut vector_storages = HashMap::new();

    for (vector_name, vector_config) in &config.vector_data {
        let vector_storage_path = get_vector_storage_path(segment_path, vector_name);

        // Select suitable vector storage type based on configuration
        let vector_storage = sp(open_vector_storage(
            #[cfg(feature = "rocksdb")]
            &mut db_builder,
            vector_config,
            #[cfg(feature = "rocksdb")]
            stopped,
            &vector_storage_path,
            #[cfg(feature = "rocksdb")]
            vector_name,
        )?);

        vector_storages.insert(vector_name.to_owned(), vector_storage);
    }

    for (vector_name, sparse_config) in config.sparse_vector_data.iter() {
        let vector_storage_path = get_vector_storage_path(segment_path, vector_name);

        // Select suitable sparse vector storage type based on configuration
        let vector_storage = sp(create_sparse_vector_storage(
            #[cfg(feature = "rocksdb")]
            &mut db_builder,
            &vector_storage_path,
            #[cfg(feature = "rocksdb")]
            vector_name,
            &sparse_config.storage_type,
            #[cfg(feature = "rocksdb")]
            stopped,
        )?);

        vector_storages.insert(vector_name.to_owned(), vector_storage);
    }

    let payload_index_path = get_payload_index_path(segment_path);
    let payload_index: Arc<AtomicRefCell<StructPayloadIndex>> = sp(StructPayloadIndex::open(
        payload_storage.clone(),
        id_tracker.clone(),
        vector_storages.clone(),
        &payload_index_path,
        appendable_flag,
        create,
    )?);

    let mut vector_data = HashMap::new();
    for (vector_name, vector_config) in &config.vector_data {
        let vector_storage_path = get_vector_storage_path(segment_path, vector_name);
        let vector_storage = vector_storages.remove(vector_name).unwrap();

        let vector_index_path = get_vector_index_path(segment_path, vector_name);
        // Warn when number of points between ID tracker and storage differs
        let point_count = id_tracker.borrow().total_point_count();
        let vector_count = vector_storage.borrow().total_vector_count();
        if vector_count != point_count {
            log::debug!(
                "Mismatch of point and vector counts ({point_count} != {vector_count}, storage: {})",
                vector_storage_path.display(),
            );
        }

        let quantized_vectors = sp(
            if let Some(quantization_config) = config.quantization_config(vector_name) {
                let quantized_data_path = vector_storage_path;
                QuantizedVectors::load(
                    quantization_config,
                    &vector_storage.borrow(),
                    &quantized_data_path,
                    stopped,
                )?
            } else {
                None
            },
        );

        let vector_index: Arc<AtomicRefCell<VectorIndexEnum>> = sp(open_vector_index(
            vector_config,
            VectorIndexOpenArgs {
                path: &vector_index_path,
                id_tracker: id_tracker.clone(),
                vector_storage: vector_storage.clone(),
                payload_index: payload_index.clone(),
                quantized_vectors: quantized_vectors.clone(),
            },
        )?);

        check_process_stopped(stopped)?;

        vector_data.insert(
            vector_name.to_owned(),
            VectorData {
                vector_index,
                vector_storage,
                quantized_vectors,
            },
        );
    }

    for (vector_name, sparse_vector_config) in &config.sparse_vector_data {
        let vector_storage_path = get_vector_storage_path(segment_path, vector_name);
        let vector_index_path = get_vector_index_path(segment_path, vector_name);
        let vector_storage = vector_storages.remove(vector_name).unwrap();

        // Warn when number of points between ID tracker and storage differs
        let point_count = id_tracker.borrow().total_point_count();
        let vector_count = vector_storage.borrow().total_vector_count();
        if vector_count != point_count {
            log::debug!(
                "Mismatch of point and vector counts ({point_count} != {vector_count}, storage: {})",
                vector_storage_path.display(),
            );
        }

        let vector_index = sp(create_sparse_vector_index(SparseVectorIndexOpenArgs {
            config: sparse_vector_config.index,
            id_tracker: id_tracker.clone(),
            vector_storage: vector_storage.clone(),
            payload_index: payload_index.clone(),
            path: &vector_index_path,
            stopped,
            tick_progress: || (),
        })?);

        check_process_stopped(stopped)?;

        vector_data.insert(
            vector_name.to_owned(),
            VectorData {
                vector_storage,
                vector_index,
                quantized_vectors: sp(None),
            },
        );
    }

    let segment_type = if config.is_any_vector_indexed() {
        SegmentType::Indexed
    } else {
        SegmentType::Plain
    };

    Ok(Segment {
        initial_version,
        version,
        persisted_version: Arc::new(Mutex::new(version)),
        current_path: segment_path.to_owned(),
        version_tracker: Default::default(),
        id_tracker,
        vector_data,
        segment_type,
        appendable_flag,
        payload_index,
        payload_storage,
        segment_config: config.clone(),
        error_status: None,
        #[cfg(feature = "rocksdb")]
        database: db_builder.build(),
    })
}

fn create_segment_id_tracker(
    mutable_id_tracker: bool,
    segment_path: &Path,
    #[cfg(feature = "rocksdb")] db_builder: &mut RocksDbBuilder,
) -> OperationResult<Arc<AtomicRefCell<IdTrackerEnum>>> {
    if !mutable_id_tracker {
        return Ok(sp(IdTrackerEnum::ImmutableIdTracker(
            create_immutable_id_tracker(segment_path)?,
        )));
    }

    // Determine whether we use the new (file based) or old (RocksDB) mutable ID tracker
    // Decide based on the feature flag and state on disk
    #[cfg(feature = "rocksdb")]
    {
        use crate::common::rocksdb_wrapper::DB_MAPPING_CF;

        let use_rocksdb_mutable_tracker = if let Some(db) = db_builder.read() {
            // New ID tracker is enabled by default, but we still use the old tracker if we have
            // any mappings stored in RocksDB
            //
            // TODO(1.15 or later): remove this check and use new mutable ID tracker unconditionally
            if let Some(cf) = db.cf_handle(DB_MAPPING_CF) {
                let count = db
                    .property_int_value_cf(cf, rocksdb::properties::ESTIMATE_NUM_KEYS)
                    .map_err(|err| {
                        OperationError::service_error(format!(
                            "Failed to get estimated number of keys from RocksDB: {err}"
                        ))
                    })?
                    .unwrap_or_default();

                count > 0
            } else {
                false
            }
        } else {
            false
        };

        if use_rocksdb_mutable_tracker {
            let id_tracker = create_rocksdb_id_tracker(db_builder.require()?)?;

            // Actively migrate RocksDB based ID tracker into mutable ID tracker
            if common::flags::feature_flags().migrate_rocksdb_id_tracker {
                let id_tracker = migrate_rocksdb_id_tracker_to_mutable(id_tracker, segment_path)?;
                return Ok(sp(IdTrackerEnum::MutableIdTracker(id_tracker)));
            }

            return Ok(sp(IdTrackerEnum::RocksDbIdTracker(id_tracker)));
        }
    }

    Ok(sp(IdTrackerEnum::MutableIdTracker(
        create_mutable_id_tracker(segment_path)?,
    )))
}

pub fn load_segment(path: &Path, stopped: &AtomicBool) -> OperationResult<Option<Segment>> {
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext == "deleted")
        .unwrap_or(false)
    {
        log::warn!("Segment is marked as deleted, skipping: {}", path.display());
        // Skip deleted segments
        return Ok(None);
    }

    let Some(stored_version) = SegmentVersion::load(path)? else {
        // Assume segment was not properly saved.
        // Server might have crashed before saving the segment fully.
        log::warn!(
            "Segment version file not found, skipping: {}",
            path.display()
        );
        return Ok(None);
    };

    let app_version = SegmentVersion::current();

    if stored_version != app_version {
        info!("Migrating segment {stored_version} -> {app_version}");

        if stored_version > app_version {
            return Err(OperationError::service_error(format!(
                "Data version {stored_version} is newer than application version {app_version}. \
                Please upgrade the application. Compatibility is not guaranteed."
            )));
        }

        if stored_version.major == 0 && stored_version.minor < 3 {
            return Err(OperationError::service_error(format!(
                "Segment version({stored_version}) is not compatible with current version({app_version})"
            )));
        }

        if stored_version.major == 0 && stored_version.minor == 3 {
            let segment_state = load_segment_state_v3(path)?;
            Segment::save_state(&segment_state, path)?;
        } else if stored_version.major == 0 && stored_version.minor <= 5 {
            let segment_state = load_segment_state_v5(path)?;
            Segment::save_state(&segment_state, path)?;
        }

        SegmentVersion::save(path)?
    }

    #[cfg_attr(not(feature = "rocksdb"), expect(unused_mut))]
    let mut segment_state = Segment::load_state(path)?;

    #[cfg_attr(not(feature = "rocksdb"), expect(unused_mut))]
    let mut segment = create_segment(
        segment_state.initial_version,
        segment_state.version,
        path,
        &segment_state.config,
        stopped,
        false,
    )?;

    #[cfg(feature = "rocksdb")]
    {
        if common::flags::feature_flags().migrate_rocksdb_vector_storage {
            migrate_all_rocksdb_dense_vector_storages(path, &mut segment, &mut segment_state)?;
            migrate_all_rocksdb_sparse_vector_storages(path, &mut segment, &mut segment_state)?;
        }

        if common::flags::feature_flags().migrate_rocksdb_payload_storage {
            migrate_rocksdb_payload_storage(path, &mut segment, &mut segment_state)?;
        }
    }

    Ok(Some(segment))
}

pub fn new_segment_path(segments_path: &Path) -> PathBuf {
    segments_path.join(Uuid::new_v4().to_string())
}

/// Build segment instance using given configuration.
/// Builder will generate folder for the segment and store all segment information inside it.
///
/// # Arguments
///
/// * `segments_path` - Path to the segments directory. Segment folder will be created in this directory
/// * `config` - Segment configuration
/// * `ready` - Whether the segment is ready after building; will save segment version
///
/// To load a segment, saving the segment version is required. If `ready` is false, the version
/// will not be stored. Then the segment is skipped on restart when trying to load it again. In
/// that case, the segment version must be stored manually to make it ready.
pub fn build_segment(
    segments_path: &Path,
    config: &SegmentConfig,
    ready: bool,
) -> OperationResult<Segment> {
    let segment_path = new_segment_path(segments_path);

    fs::create_dir_all(&segment_path)?;

    let segment = create_segment(
        None,
        None,
        &segment_path,
        config,
        &AtomicBool::new(false),
        true,
    )?;
    segment.save_current_state()?;

    // Version is the last file to save, as it will be used to check if segment was built correctly.
    // If it is not saved, segment will be skipped.
    if ready {
        SegmentVersion::save(&segment_path)?;
    }

    Ok(segment)
}

/// Load v0.3.* segment data and migrate to current version
#[allow(deprecated)]
fn load_segment_state_v3(segment_path: &Path) -> OperationResult<SegmentState> {
    use crate::compat::{SegmentConfigV5, StorageTypeV5, VectorDataConfigV5};

    #[derive(Deserialize)]
    #[serde(rename_all = "snake_case")]
    #[deprecated]
    pub struct SegmentStateV3 {
        pub version: SeqNumberType,
        pub config: SegmentConfigV3,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "snake_case")]
    #[deprecated]
    pub struct SegmentConfigV3 {
        /// Size of a vectors used
        pub vector_size: usize,
        /// Type of distance function used for measuring distance between vectors
        pub distance: Distance,
        /// Type of index used for search
        pub index: Indexes,
        /// Type of vector storage
        pub storage_type: StorageTypeV5,
        /// Defines payload storage type
        #[serde(default)]
        pub payload_storage_type: PayloadStorageType,
    }

    let path = segment_path.join(SEGMENT_STATE_FILE);

    let mut contents = String::new();

    let mut file = File::open(&path)?;
    file.read_to_string(&mut contents)?;

    serde_json::from_str::<SegmentStateV3>(&contents)
        .map(|state| {
            // Construct V5 version, then convert into current
            let vector_data = VectorDataConfigV5 {
                size: state.config.vector_size,
                distance: state.config.distance,
                hnsw_config: None,
                quantization_config: None,
                on_disk: None,
            };
            let segment_config = SegmentConfigV5 {
                vector_data: HashMap::from([(DEFAULT_VECTOR_NAME.to_owned(), vector_data)]),
                index: state.config.index,
                storage_type: state.config.storage_type,
                payload_storage_type: state.config.payload_storage_type,
                quantization_config: None,
            };

            SegmentState {
                initial_version: None,
                version: Some(state.version),
                config: segment_config.into(),
            }
        })
        .map_err(|err| {
            OperationError::service_error(format!(
                "Failed to read segment {}. Error: {}",
                path.to_str().unwrap(),
                err
            ))
        })
}

/// Load v0.5.0 segment data and migrate to current version
#[allow(deprecated)]
fn load_segment_state_v5(segment_path: &Path) -> OperationResult<SegmentState> {
    use crate::compat::SegmentStateV5;

    let path = segment_path.join(SEGMENT_STATE_FILE);

    let mut contents = String::new();

    let mut file = File::open(&path)?;
    file.read_to_string(&mut contents)?;

    serde_json::from_str::<SegmentStateV5>(&contents)
        .map(Into::into)
        .map_err(|err| {
            OperationError::service_error(format!(
                "Failed to read segment {}. Error: {}",
                path.to_str().unwrap(),
                err
            ))
        })
}

/// Migrate a RocksDB based ID tracker into a mutable ID tracker.
///
/// Creates a new mutable ID tracker and copies all mappings from the RocksDB based ID tracker into
/// it. The persisted RocksDB data is deleted so that only the new tracker will be loaded next
/// time. The new ID tracker is returned.
#[cfg(feature = "rocksdb")]
pub fn migrate_rocksdb_id_tracker_to_mutable(
    old_id_tracker: SimpleIdTracker,
    segment_path: &Path,
) -> OperationResult<MutableIdTracker> {
    log::info!(
        "Migrating {} points in ID tracker from RocksDB into new format",
        old_id_tracker.total_point_count(),
    );

    fn migrate(
        old_id_tracker: &SimpleIdTracker,
        segment_path: &Path,
    ) -> OperationResult<MutableIdTracker> {
        // Construct mutable ID tracker
        let mut new_id_tracker = create_mutable_id_tracker(segment_path)?;
        debug_assert_eq!(
            new_id_tracker.total_point_count(),
            0,
            "new mutable ID tracker must be empty",
        );

        // Set external ID to internal ID mapping
        for (external_id, internal_id) in old_id_tracker.iter_from(None) {
            new_id_tracker.set_link(external_id, internal_id)?;
        }

        // Copy all point versions and set known mappings
        for (internal_id, version) in old_id_tracker.iter_versions() {
            new_id_tracker.set_internal_version(internal_id, version)?;
        }

        // Flush mappings and versions
        new_id_tracker.mapping_flusher()()?;
        new_id_tracker.versions_flusher()()?;

        Ok(new_id_tracker)
    }

    let new_id_tracker = match migrate(&old_id_tracker, segment_path) {
        Ok(new_id_tracker) => new_id_tracker,
        // On migration error, clean up and remove all new ID tracker files
        Err(err) => {
            for file in MutableIdTracker::segment_files(segment_path) {
                if let Err(err) = fs::remove_file(&file) {
                    log::error!(
                        "ID tracker migration to mutable failed, failed to remove mutable file {} for cleanup: {err}",
                        file.display(),
                    );
                }
            }
            return Err(err);
        }
    };

    // Destroy persisted RocksDB ID tracker data
    old_id_tracker.destroy()?;

    Ok(new_id_tracker)
}

#[cfg(feature = "rocksdb")]
fn migrate_all_rocksdb_dense_vector_storages(
    path: &Path,
    segment: &mut Segment,
    segment_state: &mut SegmentState,
) -> OperationResult<()> {
    use std::ops::Deref;

    for (vector_name, data) in &mut segment.vector_data {
        // Only convert simple dense and multi dense vector storages
        if !matches!(
            data.vector_storage.borrow().deref(),
            VectorStorageEnum::DenseSimple(_)
                | VectorStorageEnum::DenseSimpleByte(_)
                | VectorStorageEnum::DenseSimpleHalf(_)
                | VectorStorageEnum::MultiDenseSimple(_)
                | VectorStorageEnum::MultiDenseSimpleByte(_)
                | VectorStorageEnum::MultiDenseSimpleHalf(_)
        ) {
            continue;
        }

        let vector_storage_path = get_vector_storage_path(path, vector_name);
        let vector_config = segment_state.config.vector_data.get(vector_name).unwrap();
        let multivector_config = vector_config.multivector_config;

        // Actively migrate away from RocksDB
        let new_storage = if let Some(multi_vector_config) = multivector_config {
            migrate_rocksdb_multi_dense_vector_storage_to_mmap(
                data.vector_storage.borrow().deref(),
                vector_config.size,
                multi_vector_config,
                &vector_storage_path,
            )?
        } else {
            migrate_rocksdb_dense_vector_storage_to_mmap(
                data.vector_storage.borrow().deref(),
                vector_config.size,
                &vector_storage_path,
            )?
        };

        let old_storage = std::mem::replace(&mut *data.vector_storage.borrow_mut(), new_storage);

        // Update storage type
        segment_state
            .config
            .vector_data
            .get_mut(vector_name)
            .unwrap()
            .storage_type = VectorStorageType::InRamChunkedMmap;
        Segment::save_state(segment_state, path)?;

        // Destroy persisted RocksDB dense vector data
        match old_storage {
            VectorStorageEnum::DenseSimple(storage) => storage.destroy()?,
            VectorStorageEnum::DenseSimpleByte(storage) => storage.destroy()?,
            VectorStorageEnum::DenseSimpleHalf(storage) => storage.destroy()?,
            VectorStorageEnum::MultiDenseSimple(storage) => storage.destroy()?,
            VectorStorageEnum::MultiDenseSimpleByte(storage) => storage.destroy()?,
            VectorStorageEnum::MultiDenseSimpleHalf(storage) => storage.destroy()?,
            _ => unreachable!("unexpected vector storage type"),
        }

        // Also update config in already loaded segment
        segment.segment_config = segment_state.config.clone();
    }

    Ok(())
}

/// Migrate a RocksDB based dense vector storage into the mmap format
///
/// Creates a new mutable in-memory vector storage on top of memory maps, and copies all vectors
/// from the RocksDB based storage into it. The new vector storage is returned.
///
/// Warning: the old vector storage is not destroyed, so it must be done manually
#[cfg(feature = "rocksdb")]
pub fn migrate_rocksdb_dense_vector_storage_to_mmap(
    old_storage: &VectorStorageEnum,
    dim: usize,
    vector_storage_path: &Path,
) -> OperationResult<VectorStorageEnum> {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::types::PointOffsetType;

    use crate::vector_storage::Sequential;
    use crate::vector_storage::dense::appendable_dense_vector_storage::find_storage_files;

    log::info!(
        "Migrating {} points in dense vector storage from RocksDB into new format",
        old_storage.total_vector_count(),
    );

    fn migrate(
        old_storage: &VectorStorageEnum,
        dim: usize,
        vector_storage_path: &Path,
    ) -> OperationResult<VectorStorageEnum> {
        // Construct mmap based dense vector storage
        let mut new_storage = open_appendable_in_ram_vector_storage(
            old_storage.datatype(),
            vector_storage_path,
            dim,
            old_storage.distance(),
        )?;
        debug_assert_eq!(
            new_storage.total_vector_count(),
            0,
            "new dense vector storage must be empty",
        );

        // Copy all vectors and deletes into new storage
        let hw_counter = HardwareCounterCell::disposable();
        for internal_id in 0..old_storage.total_vector_count() as PointOffsetType {
            let vector = old_storage.get_vector::<Sequential>(internal_id);
            new_storage.insert_vector(internal_id, vector.as_vec_ref(), &hw_counter)?;

            let is_deleted = old_storage.is_deleted_vector(internal_id);
            if is_deleted {
                new_storage.delete_vector(internal_id)?;
            }
        }

        // Flush new storage
        new_storage.flusher()()?;

        Ok(new_storage)
    }

    let new_storage = match migrate(old_storage, dim, vector_storage_path) {
        Ok(new_storage) => new_storage,
        // On migration error, clean up and remove all new storage files
        Err(err) => {
            let files = find_storage_files(vector_storage_path);
            match files {
                Ok(files) => {
                    for file in files {
                        if let Err(err) = fs::remove_file(&file) {
                            log::error!(
                                "Dense vector storage migration to mmap failed, failed to remove mmap file {} for cleanup: {err}",
                                file.display(),
                            );
                        }
                    }
                }
                Err(err) => {
                    log::error!(
                        "Dense vector storage migration to mmap failed, failed to list its storage files, they are left behind: {err}",
                    );
                }
            }
            return Err(err);
        }
    };

    Ok(new_storage)
}

/// Migrate a RocksDB based multi dense vector storage into the mmap format
///
/// Creates a new mutable in-memory vector storage on top of memory maps, and copies all vectors
/// from the RocksDB based storage into it. The new vector storage is returned.
///
/// Warning: the old vector storage is not destroyed, so it must be done manually
#[cfg(feature = "rocksdb")]
pub fn migrate_rocksdb_multi_dense_vector_storage_to_mmap(
    old_storage: &VectorStorageEnum,
    dim: usize,
    multi_vector_config: MultiVectorConfig,
    vector_storage_path: &Path,
) -> OperationResult<VectorStorageEnum> {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::types::PointOffsetType;

    use crate::vector_storage::Sequential;
    use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::find_storage_files;

    log::info!(
        "Migrating {} points in multi dense vector storage from RocksDB into new format",
        old_storage.total_vector_count(),
    );

    fn migrate(
        old_storage: &VectorStorageEnum,
        dim: usize,
        multi_vector_config: MultiVectorConfig,
        vector_storage_path: &Path,
    ) -> OperationResult<VectorStorageEnum> {
        // Construct mmap based multi dense vector storage
        let mut new_storage = open_appendable_in_ram_multi_vector_storage(
            old_storage.datatype(),
            vector_storage_path,
            dim,
            old_storage.distance(),
            multi_vector_config,
        )?;
        debug_assert_eq!(
            new_storage.total_vector_count(),
            0,
            "new multi dense vector storage must be empty",
        );

        // Copy all vectors and deletes into new storage
        let hw_counter = HardwareCounterCell::disposable();
        for internal_id in 0..old_storage.total_vector_count() as PointOffsetType {
            let vector = old_storage.get_vector::<Sequential>(internal_id);
            new_storage.insert_vector(internal_id, vector.as_vec_ref(), &hw_counter)?;

            let is_deleted = old_storage.is_deleted_vector(internal_id);
            if is_deleted {
                new_storage.delete_vector(internal_id)?;
            }
        }

        // Flush new storage
        new_storage.flusher()()?;

        Ok(new_storage)
    }

    let new_storage = match migrate(old_storage, dim, multi_vector_config, vector_storage_path) {
        Ok(new_storage) => new_storage,
        // On migration error, clean up and remove all new storage files
        Err(err) => {
            let files = find_storage_files(vector_storage_path);
            match files {
                Ok(files) => {
                    for file in files {
                        if let Err(err) = fs::remove_file(&file) {
                            log::error!(
                                "Multi dense vector storage migration to mmap failed, failed to remove mmap file {} for cleanup: {err}",
                                file.display(),
                            );
                        }
                    }
                }
                Err(err) => {
                    log::error!(
                        "Multi dense vector storage migration to mmap failed, failed to list its storage files, they are left behind: {err}",
                    );
                }
            }
            return Err(err);
        }
    };

    Ok(new_storage)
}

#[cfg(feature = "rocksdb")]
fn migrate_all_rocksdb_sparse_vector_storages(
    path: &Path,
    segment: &mut Segment,
    segment_state: &mut SegmentState,
) -> OperationResult<()> {
    use std::ops::Deref;

    for (vector_name, data) in &mut segment.vector_data {
        // Only convert simple sparse vector storages
        if !matches!(
            data.vector_storage.borrow().deref(),
            VectorStorageEnum::SparseSimple(_),
        ) {
            continue;
        }

        let vector_storage_path = get_vector_storage_path(path, vector_name);

        // Actively migrate away from RocksDB
        let new_storage = migrate_rocksdb_sparse_vector_storage_to_mmap(
            data.vector_storage.borrow().deref(),
            &vector_storage_path,
        )?;

        let old_storage = std::mem::replace(&mut *data.vector_storage.borrow_mut(), new_storage);

        // Update storage type
        segment_state
            .config
            .sparse_vector_data
            .get_mut(vector_name)
            .unwrap()
            .storage_type = SparseVectorStorageType::Mmap;
        Segment::save_state(segment_state, path)?;

        // Destroy persisted RocksDB sparse vector data
        match old_storage {
            VectorStorageEnum::SparseSimple(storage) => storage.destroy()?,
            _ => unreachable!("unexpected vector storage type"),
        }

        // Also update config in already loaded segment
        segment.segment_config = segment_state.config.clone();
    }

    Ok(())
}

/// Migrate a RocksDB based sparse vector storage into the mmap format
///
/// Creates a new mutable in-memory vector storage on top of memory maps, and copies all vectors
/// from the RocksDB based storage into it. The new vector storage is returned.
///
/// Warning: the old vector storage is not destroyed, so it must be done manually
#[cfg(feature = "rocksdb")]
pub fn migrate_rocksdb_sparse_vector_storage_to_mmap(
    old_storage: &VectorStorageEnum,
    vector_storage_path: &Path,
) -> OperationResult<VectorStorageEnum> {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::types::PointOffsetType;

    use crate::vector_storage::Sequential;
    use crate::vector_storage::sparse::mmap_sparse_vector_storage::find_storage_files;

    log::info!(
        "Migrating {} points in sparse vector storage from RocksDB into new format",
        old_storage.total_vector_count(),
    );

    fn migrate(
        old_storage: &VectorStorageEnum,
        vector_storage_path: &Path,
    ) -> OperationResult<VectorStorageEnum> {
        // Construct mmap based sparse vector storage
        let mut new_storage = VectorStorageEnum::SparseMmap(
            MmapSparseVectorStorage::open_or_create(vector_storage_path)?,
        );
        debug_assert_eq!(
            new_storage.total_vector_count(),
            0,
            "new sparse vector storage must be empty",
        );

        // Copy all vectors and deletes into new storage
        let hw_counter = HardwareCounterCell::disposable();
        for internal_id in 0..old_storage.total_vector_count() as PointOffsetType {
            let vector = old_storage.get_vector::<Sequential>(internal_id);
            new_storage.insert_vector(internal_id, vector.as_vec_ref(), &hw_counter)?;

            let is_deleted = old_storage.is_deleted_vector(internal_id);
            if is_deleted {
                new_storage.delete_vector(internal_id)?;
            }
        }

        // Flush new storage
        new_storage.flusher()()?;

        Ok(new_storage)
    }

    let new_storage = match migrate(old_storage, vector_storage_path) {
        Ok(new_storage) => new_storage,
        // On migration error, clean up and remove all new storage files
        Err(err) => {
            let files = find_storage_files(vector_storage_path);
            match files {
                Ok(files) => {
                    for file in files {
                        if let Err(err) = fs::remove_file(&file) {
                            log::error!(
                                "Sparse vector storage migration to mmap failed, failed to remove mmap file {} for cleanup: {err}",
                                file.display(),
                            );
                        }
                    }
                }
                Err(err) => {
                    log::error!(
                        "Sparse vector storage migration to mmap failed, failed to list its storage files, they are left behind: {err}",
                    );
                }
            }
            return Err(err);
        }
    };

    Ok(new_storage)
}

#[cfg(feature = "rocksdb")]
fn migrate_rocksdb_payload_storage(
    path: &Path,
    segment: &mut Segment,
    segment_state: &mut SegmentState,
) -> OperationResult<()> {
    use std::ops::Deref;

    use crate::payload_storage::PayloadStorage;

    if !matches!(
        segment.payload_storage.borrow().deref(),
        PayloadStorageEnum::SimplePayloadStorage(_) | PayloadStorageEnum::OnDiskPayloadStorage(_),
    ) {
        return Ok(());
    }

    // Actively migrate away from RocksDB
    let new_storage =
        migrate_rocksdb_payload_storage_to_mmap(segment.payload_storage.borrow().deref(), path)?;

    let old_storage = std::mem::replace(&mut *segment.payload_storage.borrow_mut(), new_storage);

    // Update storage type
    segment_state.config.payload_storage_type = if old_storage.is_on_disk() {
        PayloadStorageType::Mmap
    } else {
        PayloadStorageType::InRamMmap
    };
    Segment::save_state(segment_state, path)?;

    // Destroy persisted RocksDB payload data
    match old_storage {
        PayloadStorageEnum::SimplePayloadStorage(storage) => storage.destroy()?,
        PayloadStorageEnum::OnDiskPayloadStorage(storage) => storage.destroy()?,
        PayloadStorageEnum::MmapPayloadStorage(_) => {
            unreachable!("unexpected payload storage type")
        }
        #[cfg(feature = "testing")]
        PayloadStorageEnum::InMemoryPayloadStorage(_) => {
            unreachable!("unexpected payload storage type")
        }
    }

    // Also update config in already loaded segment
    segment.segment_config = segment_state.config.clone();

    Ok(())
}

/// Migrate a RocksDB based payload storage storage into the mmap format
///
/// Creates a new payload storage on top of memory maps, and copies all payloads
/// from the RocksDB based storage into it. The new payload storage is returned.
///
/// Warning: the old payload storage is not destroyed, so it must be done manually
#[cfg(feature = "rocksdb")]
pub fn migrate_rocksdb_payload_storage_to_mmap(
    old_storage: &PayloadStorageEnum,
    segment_path: &Path,
) -> OperationResult<PayloadStorageEnum> {
    use common::counter::hardware_counter::HardwareCounterCell;

    use crate::payload_storage::{PayloadStorage, mmap_payload_storage};

    log::info!(
        "Migrating {} of payload storage from RocksDB into new format",
        common::bytes::bytes_to_human(old_storage.get_storage_size_bytes().unwrap_or(0)),
    );

    fn migrate(
        old_storage: &PayloadStorageEnum,
        segment_path: &Path,
    ) -> OperationResult<PayloadStorageEnum> {
        // Construct mmap based payload storage
        let mut new_storage = PayloadStorageEnum::from(MmapPayloadStorage::open_or_create(
            segment_path.to_path_buf(),
            !old_storage.is_on_disk(),
        )?);

        // Copy all payloads and deletes into new storage
        let hw_counter = HardwareCounterCell::disposable();
        old_storage.iter(
            |internal_id, payload| {
                new_storage.set(internal_id, payload, &hw_counter)?;
                Ok(true)
            },
            &hw_counter,
        )?;

        // Flush new storage
        new_storage.flusher()()?;

        Ok(new_storage)
    }

    let new_storage = match migrate(old_storage, segment_path) {
        Ok(new_storage) => new_storage,
        // On migration error, clean up and remove all new storage files
        Err(err) => {
            let storage_dir = mmap_payload_storage::storage_dir(segment_path);
            if storage_dir.is_dir()
                && let Err(err) = fs::remove_dir_all(&storage_dir)
            {
                log::error!(
                    "Payload storage migration to mmap failed, failed to remove mmap files in {} for cleanup: {err}",
                    storage_dir.display(),
                );
            }
            return Err(err);
        }
    };

    Ok(new_storage)
}
