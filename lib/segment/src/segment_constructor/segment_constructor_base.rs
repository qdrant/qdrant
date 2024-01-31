use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use log::info;
use parking_lot::Mutex;
use semver::Version;
use serde::Deserialize;
use uuid::Uuid;

use crate::common::operation_error::{check_process_stopped, OperationError, OperationResult};
use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
use crate::common::version::StorageVersion;
use crate::data_types::vectors::DEFAULT_VECTOR_NAME;
use crate::id_tracker::simple_id_tracker::SimpleIdTracker;
use crate::id_tracker::IdTracker;
use crate::index::hnsw_index::graph_links::{GraphLinksMmap, GraphLinksRam};
use crate::index::hnsw_index::hnsw::HNSWIndex;
use crate::index::plain_payload_index::PlainIndex;
use crate::index::sparse_index::sparse_index_config::SparseIndexType;
use crate::index::sparse_index::sparse_vector_index::SparseVectorIndex;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::VectorIndexEnum;
use crate::payload_storage::on_disk_payload_storage::OnDiskPayloadStorage;
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::segment::{Segment, SegmentVersion, VectorData, SEGMENT_STATE_FILE};
use crate::types::{
    Distance, Indexes, PayloadStorageType, SegmentConfig, SegmentState, SegmentType, SeqNumberType,
    VectorStorageType,
};
use crate::vector_storage::appendable_mmap_vector_storage::open_appendable_memmap_vector_storage;
use crate::vector_storage::memmap_vector_storage::open_memmap_vector_storage;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::simple_dense_vector_storage::open_simple_vector_storage;
use crate::vector_storage::simple_sparse_vector_storage::open_simple_sparse_vector_storage;
use crate::vector_storage::VectorStorage;

pub const PAYLOAD_INDEX_PATH: &str = "payload_index";
pub const VECTOR_STORAGE_PATH: &str = "vector_storage";
pub const VECTOR_INDEX_PATH: &str = "vector_index";

fn sp<T>(t: T) -> Arc<AtomicRefCell<T>> {
    Arc::new(AtomicRefCell::new(t))
}

fn get_vector_name_with_prefix(prefix: &str, vector_name: &str) -> String {
    if !vector_name.is_empty() {
        format!("{prefix}-{vector_name}")
    } else {
        prefix.to_owned()
    }
}

pub fn get_vector_storage_path(segment_path: &Path, vector_name: &str) -> PathBuf {
    segment_path.join(get_vector_name_with_prefix(
        VECTOR_STORAGE_PATH,
        vector_name,
    ))
}

pub fn get_vector_index_path(segment_path: &Path, vector_name: &str) -> PathBuf {
    segment_path.join(get_vector_name_with_prefix(VECTOR_INDEX_PATH, vector_name))
}

fn create_segment(
    version: Option<SeqNumberType>,
    segment_path: &Path,
    config: &SegmentConfig,
    stopped: &AtomicBool,
) -> OperationResult<Segment> {
    let vector_db_names: Vec<String> = config
        .vector_data
        .keys()
        .map(|vector_name| get_vector_name_with_prefix(DB_VECTOR_CF, vector_name))
        .chain(
            config
                .sparse_vector_data
                .keys()
                .map(|vector_name| get_vector_name_with_prefix(DB_VECTOR_CF, vector_name)),
        )
        .collect();
    let database = open_db(segment_path, &vector_db_names)
        .map_err(|err| OperationError::service_error(format!("RocksDB open error: {err}")))?;

    let payload_storage = match config.payload_storage_type {
        PayloadStorageType::InMemory => sp(SimplePayloadStorage::open(database.clone())?.into()),
        PayloadStorageType::OnDisk => sp(OnDiskPayloadStorage::open(database.clone())?.into()),
    };

    let id_tracker = sp(SimpleIdTracker::open(database.clone())?);

    let appendable_flag = config
        .vector_data
        .values()
        .map(|vector_config| vector_config.is_appendable())
        .chain(
            config
                .sparse_vector_data
                .values()
                .map(|sparse_vector_config| sparse_vector_config.is_appendable()),
        )
        .all(|v| v);

    let payload_index_path = segment_path.join(PAYLOAD_INDEX_PATH);
    let payload_index: Arc<AtomicRefCell<StructPayloadIndex>> = sp(StructPayloadIndex::open(
        payload_storage,
        id_tracker.clone(),
        &payload_index_path,
        appendable_flag,
    )?);

    let mut vector_data = HashMap::new();
    for (vector_name, vector_config) in &config.vector_data {
        let vector_storage_path = get_vector_storage_path(segment_path, vector_name);
        let vector_index_path = get_vector_index_path(segment_path, vector_name);

        // Select suitable vector storage type based on configuration
        let vector_storage = match vector_config.storage_type {
            // In memory
            VectorStorageType::Memory => {
                let db_column_name = get_vector_name_with_prefix(DB_VECTOR_CF, vector_name);
                open_simple_vector_storage(
                    database.clone(),
                    &db_column_name,
                    vector_config.size,
                    vector_config.distance,
                    stopped,
                )?
            }
            // Mmap on disk, not appendable
            VectorStorageType::Mmap => open_memmap_vector_storage(
                &vector_storage_path,
                vector_config.size,
                vector_config.distance,
            )?,
            // Chunked mmap on disk, appendable
            VectorStorageType::ChunkedMmap => open_appendable_memmap_vector_storage(
                &vector_storage_path,
                vector_config.size,
                vector_config.distance,
                stopped,
            )?,
        };

        // Warn when number of points between ID tracker and storage differs
        let point_count = id_tracker.borrow().total_point_count();
        let vector_count = vector_storage.borrow().total_vector_count();
        if vector_count != point_count {
            log::debug!(
                "Mismatch of point and vector counts ({point_count} != {vector_count}, storage: {})",
                vector_storage_path.display(),
            );
        }

        let quantized_vectors = sp(if config.quantization_config(vector_name).is_some() {
            let quantized_data_path = vector_storage_path;
            if QuantizedVectors::config_exists(&quantized_data_path) {
                let quantized_vectors =
                    QuantizedVectors::load(&vector_storage.borrow(), &quantized_data_path)?;
                Some(quantized_vectors)
            } else {
                None
            }
        } else {
            None
        });

        let vector_index: Arc<AtomicRefCell<VectorIndexEnum>> = match &vector_config.index {
            Indexes::Plain {} => sp(VectorIndexEnum::Plain(PlainIndex::new(
                id_tracker.clone(),
                vector_storage.clone(),
                payload_index.clone(),
            ))),
            Indexes::Hnsw(vector_hnsw_config) => sp(if vector_hnsw_config.on_disk == Some(true) {
                VectorIndexEnum::HnswMmap(HNSWIndex::<GraphLinksMmap>::open(
                    &vector_index_path,
                    id_tracker.clone(),
                    vector_storage.clone(),
                    quantized_vectors.clone(),
                    payload_index.clone(),
                    vector_hnsw_config.clone(),
                )?)
            } else {
                VectorIndexEnum::HnswRam(HNSWIndex::<GraphLinksRam>::open(
                    &vector_index_path,
                    id_tracker.clone(),
                    vector_storage.clone(),
                    quantized_vectors.clone(),
                    payload_index.clone(),
                    vector_hnsw_config.clone(),
                )?)
            }),
        };

        check_process_stopped(stopped)?;

        vector_data.insert(
            vector_name.to_owned(),
            VectorData {
                vector_storage,
                vector_index,
                quantized_vectors,
            },
        );
    }

    for (vector_name, sparse_vector_config) in &config.sparse_vector_data {
        let vector_storage_path = get_vector_storage_path(segment_path, vector_name);
        let vector_index_path = get_vector_index_path(segment_path, vector_name);

        let db_column_name = get_vector_name_with_prefix(DB_VECTOR_CF, vector_name);
        let vector_storage =
            open_simple_sparse_vector_storage(database.clone(), &db_column_name, stopped)?;

        // Warn when number of points between ID tracker and storage differs
        let point_count = id_tracker.borrow().total_point_count();
        let vector_count = vector_storage.borrow().total_vector_count();
        if vector_count != point_count {
            log::debug!(
                "Mismatch of point and vector counts ({point_count} != {vector_count}, storage: {})",
                vector_storage_path.display(),
            );
        }

        let vector_index = match sparse_vector_config.index.index_type {
            SparseIndexType::Mmap => sp(VectorIndexEnum::SparseMmap(SparseVectorIndex::open(
                sparse_vector_config.index,
                id_tracker.clone(),
                vector_storage.clone(),
                payload_index.clone(),
                &vector_index_path,
                stopped,
            )?)),
            SparseIndexType::MutableRam | SparseIndexType::ImmutableRam => {
                sp(VectorIndexEnum::SparseRam(SparseVectorIndex::open(
                    sparse_vector_config.index,
                    id_tracker.clone(),
                    vector_storage.clone(),
                    payload_index.clone(),
                    &vector_index_path,
                    stopped,
                )?))
            }
        };

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
        version,
        persisted_version: Arc::new(Mutex::new(version)),
        current_path: segment_path.to_owned(),
        id_tracker,
        vector_data,
        segment_type,
        appendable_flag,
        payload_index,
        segment_config: config.clone(),
        error_status: None,
        database,
        flush_thread: Mutex::new(None),
    })
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

    if !SegmentVersion::check_exists(path) {
        // Assume segment was not properly saved.
        // Server might have crashed before saving the segment fully.
        log::warn!(
            "Segment version file not found, skipping: {}",
            path.display()
        );
        return Ok(None);
    }

    let stored_version: Version = SegmentVersion::load(path)?.parse()?;
    let app_version: Version = SegmentVersion::current().parse()?;

    if stored_version != app_version {
        info!("Migrating segment {} -> {}", stored_version, app_version,);

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

    let segment_state = Segment::load_state(path)?;

    let segment = create_segment(segment_state.version, path, &segment_state.config, stopped)?;

    Ok(Some(segment))
}

/// Build segment instance using given configuration.
/// Builder will generate folder for the segment and store all segment information inside it.
///
/// # Arguments
///
/// * `path` - A path to collection. Segment folder will be created in this directory
/// * `config` - Segment configuration
/// * `ready` - Whether the segment is ready after building; will save segment version
///
/// To load a segment, saving the segment version is required. If `ready` is false, the version
/// will not be stored. Then the segment is skipped on restart when trying to load it again. In
/// that case, the segment version must be stored manually to make it ready.
pub fn build_segment(path: &Path, config: &SegmentConfig, ready: bool) -> OperationResult<Segment> {
    let segment_path = path.join(Uuid::new_v4().to_string());

    std::fs::create_dir_all(&segment_path)?;

    let segment = create_segment(None, &segment_path, config, &AtomicBool::new(false))?;
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
