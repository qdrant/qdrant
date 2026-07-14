use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use atomic_refcell::AtomicRefCell;
use common::defaults::log_load_timing;
use common::is_alive_lock::IsAliveLock;
use common::types::PointOffsetType;
use common::universal_io::MmapFs;
use parking_lot::Mutex;
use uuid::Uuid;

use super::id_tracker::create_segment_id_tracker;
use super::paths::{get_payload_index_path, get_vector_index_path, get_vector_storage_path};
use super::payload_storage::create_payload_storage;
use super::sp;
use super::sparse_vector_index::open_or_create_sparse_vector_index;
use super::vector_index::{VectorIndexOpenArgs, open_vector_index};
use super::vector_storage::{create_sparse_vector_storage, open_vector_storage};
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::id_tracker::{IdTrackerEnum, IdTrackerFormat, IdTrackerRead};
use crate::index::VectorIndexEnum;
use crate::index::sparse_index::sparse_vector_index::SparseVectorIndexOpenArgs;
use crate::index::struct_payload_index::{IndexLoadMode, StorageType, StructPayloadIndex};
use crate::segment::{Segment, VectorData};
use crate::types::{
    SegmentConfig, SegmentType, SeqNumberType, SparseVectorDataConfig, VectorDataConfig, VectorName,
};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

/// Assemble a [`Segment`] from its on-disk components at `segment_path`.
///
/// Opens the payload storage, id tracker, every (dense and sparse) vector
/// storage, the payload index, and finally each vector's index, wiring them all
/// into a [`Segment`]. Used by both [`super::load_segment`] (`LoadExisting`)
/// and [`super::build_segment`] (`CreateIfMissing`).
#[allow(clippy::too_many_arguments)]
pub(super) fn create_segment(
    initial_version: Option<SeqNumberType>,
    version: Option<SeqNumberType>,
    segment_path: &Path,
    uuid: Uuid,
    deferred_internal_id: Option<PointOffsetType>,
    config: &SegmentConfig,
    stopped: &AtomicBool,
    load_mode: IndexLoadMode,
) -> OperationResult<Segment> {
    let started = Instant::now();
    let payload_storage = sp(create_payload_storage(segment_path, config)?);
    log_load_timing(segment_path, "payload_storage", started);

    let appendable_flag = config.is_appendable();

    // Limit deferred segment feature to appendable segments.
    let deferred_internal_id = deferred_internal_id.filter(|_| appendable_flag);

    let id_tracker_format = IdTrackerFormat::detect_local(segment_path, appendable_flag);
    let started = Instant::now();
    let id_tracker =
        create_segment_id_tracker(id_tracker_format, segment_path, deferred_internal_id)?;
    log_load_timing(segment_path, "id_tracker", started);

    let mut vector_storages = HashMap::new();

    for (vector_name, vector_config) in &config.vector_data {
        let vector_storage_path = get_vector_storage_path(segment_path, vector_name);

        let started = Instant::now();
        let vector_storage = sp(open_vector_storage(vector_config, &vector_storage_path)?);
        log_load_timing(
            segment_path,
            &format!("vector_storage dense '{vector_name}'"),
            started,
        );

        vector_storages.insert(vector_name.to_owned(), vector_storage);
    }

    for (vector_name, sparse_config) in config.sparse_vector_data.iter() {
        let vector_storage_path = get_vector_storage_path(segment_path, vector_name);

        let started = Instant::now();
        let vector_storage = sp(create_sparse_vector_storage(
            &vector_storage_path,
            &sparse_config.storage_type,
        )?);
        log_load_timing(
            segment_path,
            &format!("vector_storage sparse '{vector_name}'"),
            started,
        );

        vector_storages.insert(vector_name.to_owned(), vector_storage);
    }

    let payload_index_path = get_payload_index_path(segment_path);
    let started = Instant::now();
    let payload_index: Arc<AtomicRefCell<StructPayloadIndex>> = sp(StructPayloadIndex::open(
        payload_storage.clone(),
        id_tracker.clone(),
        vector_storages.clone(),
        &payload_index_path,
        StorageType::from_appendable(appendable_flag),
        load_mode,
    )?);
    log_load_timing(segment_path, "payload_index", started);

    let mut vector_data = HashMap::new();
    for (vector_name, vector_config) in &config.vector_data {
        let vector_storage = vector_storages.remove(vector_name).unwrap();
        let data = open_dense_vector_data(
            segment_path,
            vector_name,
            vector_config,
            config,
            &id_tracker,
            &payload_index,
            vector_storage,
            stopped,
        )?;
        vector_data.insert(vector_name.to_owned(), data);
    }

    for (vector_name, sparse_vector_config) in &config.sparse_vector_data {
        let vector_storage = vector_storages.remove(vector_name).unwrap();
        let data = open_sparse_vector_data(
            segment_path,
            vector_name,
            sparse_vector_config,
            &id_tracker,
            &payload_index,
            vector_storage,
            stopped,
        )?;
        vector_data.insert(vector_name.to_owned(), data);
    }

    let segment_type = if config.is_any_vector_indexed() {
        SegmentType::Indexed
    } else {
        SegmentType::Plain
    };

    Ok(Segment {
        uuid,
        initial_version,
        version,
        persisted_version: Arc::new(Mutex::new(version)),
        is_alive_flush_lock: IsAliveLock::new(),
        segment_path: segment_path.to_owned(),
        version_tracker: Default::default(),
        id_tracker,
        vector_data,
        segment_type,
        appendable_flag,
        append_only_mutations: common::flags::feature_flags().append_only_mutations,
        payload_index,
        payload_storage,
        segment_config: config.clone(),
        error_status: None,
    })
}

/// Open one dense vector's quantized vectors and vector index into a [`VectorData`].
#[allow(clippy::too_many_arguments)]
fn open_dense_vector_data(
    segment_path: &Path,
    vector_name: &VectorName,
    vector_config: &VectorDataConfig,
    config: &SegmentConfig,
    id_tracker: &Arc<AtomicRefCell<IdTrackerEnum>>,
    payload_index: &Arc<AtomicRefCell<StructPayloadIndex>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    stopped: &AtomicBool,
) -> OperationResult<VectorData> {
    let vector_storage_path = get_vector_storage_path(segment_path, vector_name);
    let vector_index_path = get_vector_index_path(segment_path, vector_name);

    prefill_vector_storage(&vector_storage, id_tracker, &vector_storage_path)?;

    let started = Instant::now();
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
    log_load_timing(
        segment_path,
        &format!("quantized_vectors '{vector_name}'"),
        started,
    );

    let started = Instant::now();
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
    log_load_timing(
        segment_path,
        &format!("vector_index dense '{vector_name}'"),
        started,
    );

    check_process_stopped(stopped)?;

    Ok(VectorData {
        vector_index,
        vector_storage,
        quantized_vectors,
    })
}

/// Open one sparse vector's index into a [`VectorData`] (sparse vectors are never quantized).
fn open_sparse_vector_data(
    segment_path: &Path,
    vector_name: &VectorName,
    sparse_vector_config: &SparseVectorDataConfig,
    id_tracker: &Arc<AtomicRefCell<IdTrackerEnum>>,
    payload_index: &Arc<AtomicRefCell<StructPayloadIndex>>,
    vector_storage: Arc<AtomicRefCell<VectorStorageEnum>>,
    stopped: &AtomicBool,
) -> OperationResult<VectorData> {
    let vector_storage_path = get_vector_storage_path(segment_path, vector_name);
    let vector_index_path = get_vector_index_path(segment_path, vector_name);

    prefill_vector_storage(&vector_storage, id_tracker, &vector_storage_path)?;

    let started = Instant::now();
    let vector_index = sp(open_or_create_sparse_vector_index(
        SparseVectorIndexOpenArgs {
            fs: &MmapFs,
            config: sparse_vector_config.index,
            id_tracker: id_tracker.clone(),
            vector_storage: vector_storage.clone(),
            payload_index: payload_index.clone(),
            path: &vector_index_path,
            stopped,
            tick_progress: || (),
        },
    )?);
    log_load_timing(
        segment_path,
        &format!("vector_index sparse '{vector_name}'"),
        started,
    );

    check_process_stopped(stopped)?;

    Ok(VectorData {
        vector_storage,
        vector_index,
        quantized_vectors: sp(None),
    })
}

/// Ensure the vector storage is sized to match the id_tracker's point count.
///
/// This can be out of sync when a named vector was added to an existing segment.
fn prefill_vector_storage(
    vector_storage: &Arc<AtomicRefCell<VectorStorageEnum>>,
    id_tracker: &Arc<AtomicRefCell<IdTrackerEnum>>,
    vector_storage_path: &Path,
) -> OperationResult<()> {
    let point_count = id_tracker.borrow().total_point_count();
    let vector_count = vector_storage.borrow().total_vector_count();
    if vector_count != point_count {
        log::debug!(
            "Mismatch of point and vector counts ({point_count} != {vector_count}, storage: {}), pre-filling deleted entries",
            vector_storage_path.display(),
        );
        vector_storage
            .borrow_mut()
            .prefill_deleted_entries(point_count)?;
    }
    Ok(())
}
