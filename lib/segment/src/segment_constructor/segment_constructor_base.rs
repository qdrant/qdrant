use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use log::info;
use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
use crate::common::version::StorageVersion;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::id_tracker::simple_id_tracker::SimpleIdTracker;
use crate::index::hnsw_index::hnsw::HNSWIndex;
use crate::index::plain_payload_index::PlainIndex;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::index::VectorIndexSS;
use crate::payload_storage::on_disk_payload_storage::OnDiskPayloadStorage;
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::segment::{
    Segment, SegmentVersion, VectorData, DEFAULT_VECTOR_NAME, SEGMENT_STATE_FILE,
};
use crate::types::{
    Distance, Indexes, PayloadStorageType, SegmentConfig, SegmentState, SegmentType, SeqNumberType,
    StorageType, VectorDataConfig,
};
use crate::vector_storage::memmap_vector_storage::open_memmap_vector_storage;
use crate::vector_storage::simple_vector_storage::open_simple_vector_storage;
use crate::vector_storage::VectorStorageSS;

fn sp<T>(t: T) -> Arc<AtomicRefCell<T>> {
    Arc::new(AtomicRefCell::new(t))
}

fn create_segment(
    version: SeqNumberType,
    segment_path: &Path,
    config: &SegmentConfig,
) -> OperationResult<Segment> {
    let get_vector_name_with_prefix = |prefix: &str, vector_name: &str| {
        if !vector_name.is_empty() {
            format!("{}-{}", prefix, vector_name)
        } else {
            prefix.to_owned()
        }
    };
    let vector_db_names: Vec<String> = config
        .vector_data
        .iter()
        .map(|(vector_name, _)| get_vector_name_with_prefix(DB_VECTOR_CF, vector_name))
        .collect();
    let database = open_db(segment_path, &vector_db_names)
        .map_err(|err| OperationError::service_error(&format!("RocksDB open error: {}", err)))?;

    let payload_storage = match config.payload_storage_type {
        PayloadStorageType::InMemory => sp(SimplePayloadStorage::open(database.clone())?.into()),
        PayloadStorageType::OnDisk => sp(OnDiskPayloadStorage::open(database.clone())?.into()),
    };

    let id_tracker = sp(SimpleIdTracker::open(database.clone())?);

    let payload_index_path = segment_path.join("payload_index");
    let payload_index: Arc<AtomicRefCell<StructPayloadIndex>> = sp(StructPayloadIndex::open(
        payload_storage,
        id_tracker.clone(),
        &payload_index_path,
    )?);

    let mut vector_data = HashMap::new();
    for (vector_name, vector_config) in &config.vector_data {
        let vector_storage_path =
            segment_path.join(&get_vector_name_with_prefix("vector_storage", vector_name));
        let vector_index_path =
            segment_path.join(&get_vector_name_with_prefix("vector_index", vector_name));

        let vector_storage: Arc<AtomicRefCell<VectorStorageSS>> = match config.storage_type {
            StorageType::InMemory => {
                let db_column_name = get_vector_name_with_prefix(DB_VECTOR_CF, vector_name);
                open_simple_vector_storage(
                    database.clone(),
                    &db_column_name,
                    vector_config.vector_size,
                    vector_config.distance,
                )?
            }
            StorageType::Mmap => open_memmap_vector_storage(
                &vector_storage_path,
                vector_config.vector_size,
                vector_config.distance,
            )?,
        };

        let vector_index: Arc<AtomicRefCell<VectorIndexSS>> = match config.index {
            Indexes::Plain { .. } => sp(PlainIndex::new(
                vector_storage.clone(),
                payload_index.clone(),
            )),
            Indexes::Hnsw(hnsw_config) => sp(HNSWIndex::open(
                &vector_index_path,
                vector_storage.clone(),
                payload_index.clone(),
                hnsw_config,
            )?),
        };

        vector_data.insert(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorData {
                vector_storage,
                vector_index,
            },
        );
    }

    let segment_type = match config.index {
        Indexes::Plain { .. } => SegmentType::Plain,
        Indexes::Hnsw { .. } => SegmentType::Indexed,
    };

    let appendable_flag =
        segment_type == SegmentType::Plain {} && config.storage_type == StorageType::InMemory;

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

pub fn load_segment(path: &Path) -> OperationResult<Option<Segment>> {
    if !SegmentVersion::check_exists(path) {
        // Assume segment was not properly saved.
        // Server might have crashed before saving the segment fully.
        log::warn!(
            "Segment version file not found, skipping: {}",
            path.display()
        );
        return Ok(None);
    }

    let stored_version = SegmentVersion::load(path)?;
    if stored_version != SegmentVersion::current() {
        info!(
            "Migrating segment {} -> {}",
            stored_version,
            SegmentVersion::current()
        );
        SegmentVersion::save(path)?
    }

    let segment_config_path = path.join(SEGMENT_STATE_FILE);
    let mut contents = String::new();

    let mut file = File::open(segment_config_path)?;
    file.read_to_string(&mut contents)?;

    let segment_state = load_segment_state(&contents, path)?;

    Ok(Some(create_segment(
        segment_state.version,
        path,
        &segment_state.config,
    )?))
}

/// Build segment instance using given configuration.
/// Builder will generate folder for the segment and store all segment information inside it.
///
/// # Arguments
///
/// * `path` - A path to collection. Segment folder will be created in this directory
/// * `config` - Segment configuration
///
///
pub fn build_segment(path: &Path, config: &SegmentConfig) -> OperationResult<Segment> {
    let segment_path = path.join(Uuid::new_v4().to_string());

    create_dir_all(&segment_path)?;

    let segment = create_segment(0, &segment_path, config)?;
    segment.save_current_state()?;

    // Version is the last file to save, as it will be used to check if segment was built correctly.
    // If it is not saved, segment will be skipped.
    SegmentVersion::save(&segment_path)?;

    Ok(segment)
}

fn load_segment_state(contents: &str, path: &Path) -> OperationResult<SegmentState> {
    #[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
    #[serde(rename_all = "snake_case")]
    pub struct ObsoleteSegmentState {
        pub version: SeqNumberType,
        pub config: ObsoleteSegmentConfig,
    }

    #[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
    #[serde(rename_all = "snake_case")]
    pub struct ObsoleteSegmentConfig {
        /// Size of a vectors used
        pub vector_size: usize,
        /// Type of distance function used for measuring distance between vectors
        pub distance: Distance,
        /// Type of index used for search
        pub index: Indexes,
        /// Type of vector storage
        pub storage_type: StorageType,
        /// Defines payload storage type
        #[serde(default)]
        pub payload_storage_type: PayloadStorageType,
    }

    let from_obsolete = |state: ObsoleteSegmentState| {
        let vector_data = VectorDataConfig {
            vector_size: state.config.vector_size,
            distance: state.config.distance,
        };
        SegmentState {
            version: state.version,
            config: SegmentConfig {
                vector_data: HashMap::from([(DEFAULT_VECTOR_NAME.to_owned(), vector_data)]),
                index: state.config.index,
                storage_type: state.config.storage_type,
                payload_storage_type: state.config.payload_storage_type,
            },
        }
    };

    let segment_state = serde_json::from_str::<SegmentState>(contents).map_err(|err| {
        OperationError::service_error(&format!(
            "Failed to read segment {}. Error: {}",
            path.to_str().unwrap(),
            err
        ))
    });
    if segment_state.is_err() {
        // try to load obsolete format
        match serde_json::from_str::<ObsoleteSegmentState>(contents) {
            Ok(obsolete_segment_state) => Ok(from_obsolete(obsolete_segment_state)),
            Err(_) => segment_state,
        }
    } else {
        segment_state
    }
}
