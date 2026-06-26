use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::storage_version::StorageVersion;
use common::types::PointOffsetType;
use common::universal_io::{Populate, UniversalReadFileOps, read_json_via};
use uuid::Uuid;

use super::{ReadOnlySegment, ReadOnlyVectorData};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::immutable_id_tracker;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::UniversalReadExt;
use crate::index::read_only::{ReadOnlyVectorIndexOpenArgs, VectorIndexReadEnum};
use crate::index::struct_payload_index::read_only::ReadOnlyStructPayloadIndex;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::segment::{SEGMENT_STATE_FILE, SegmentVersion};
use crate::segment_constructor::{
    get_payload_index_path, get_vector_index_path, get_vector_storage_path,
};
use crate::types::{
    PayloadStorageType, SegmentConfig, SegmentState, SegmentType, VectorDataConfig, VectorName,
    VectorNameBuf,
};
use crate::vector_storage::VectorStorageRead;
use crate::vector_storage::quantized::quantized_vectors::ReadOnlyQuantizedVectors;
use crate::vector_storage::read_only::VectorStorageReadEnum;
use crate::vector_storage::sparse::read_only::ReadOnlySparseVectorStorage;

impl<S: UniversalReadExt + 'static> ReadOnlySegment<S> {
    /// Read-only mirror of `load_segment`: assembles every read-only component
    /// from `fs` (id tracker, payload storage+index, per-vector storage/index). No writes.
    pub fn open(
        fs: &S::Fs,
        segment_path: &Path,
        uuid: Uuid,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OperationResult<Self> {
        if SegmentVersion::load_universal(fs, segment_path)?.is_none() {
            return Err(OperationError::service_error(format!(
                "Segment version file not found in segment: {}",
                segment_path.display(),
            )));
        }

        let SegmentState {
            initial_version: _,
            version: _,
            config,
        } = read_json_via(fs, segment_path.join(SEGMENT_STATE_FILE))?;

        let is_appendable = config.is_appendable();
        let deferred_internal_id = deferred_internal_id.filter(|_| is_appendable);

        let payload_populate = match config.payload_storage_type {
            PayloadStorageType::InRamMmap => Populate::PreferBackground,
            PayloadStorageType::Mmap => Populate::No,
        };
        let payload_storage = Arc::new(AtomicRefCell::new(ReadOnlyPayloadStorage::open(
            fs,
            segment_path.to_path_buf(),
            payload_populate,
        )?));

        // Appendable (mutable-format) segments have no immutable mappings file.
        let use_appendable_id_tracker =
            is_appendable || !fs.exists(&immutable_id_tracker::mappings_path(segment_path))?;
        let id_tracker = Arc::new(AtomicRefCell::new(ReadOnlyIdTrackerEnum::open(
            fs,
            segment_path,
            use_appendable_id_tracker,
            deferred_internal_id,
        )?));

        // Open all vector storages up front: the payload index needs them.
        let mut vector_storages: HashMap<
            VectorNameBuf,
            Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
        > = HashMap::new();
        for (vector_name, vector_config) in &config.vector_data {
            let path = get_vector_storage_path(segment_path, vector_name);
            let storage =
                VectorStorageReadEnum::open(fs, vector_config, &path)?.ok_or_else(|| {
                    OperationError::service_error(format!(
                        "Read-only dense vector storage '{vector_name}' was not found, or is corrupted.",
                    ))
                })?;
            vector_storages.insert(vector_name.clone(), Arc::new(AtomicRefCell::new(storage)));
        }
        for vector_name in config.sparse_vector_data.keys() {
            let path = get_vector_storage_path(segment_path, vector_name);
            let storage = VectorStorageReadEnum::Sparse(Box::new(
                ReadOnlySparseVectorStorage::open(fs, &path)?,
            ));
            vector_storages.insert(vector_name.clone(), Arc::new(AtomicRefCell::new(storage)));
        }

        let payload_index = Arc::new(AtomicRefCell::new(ReadOnlyStructPayloadIndex::open(
            fs,
            payload_storage.clone(),
            id_tracker.clone(),
            vector_storages.clone(),
            &get_payload_index_path(segment_path),
        )?));

        let mut vector_data = HashMap::new();
        for (vector_name, vector_config) in &config.vector_data {
            let vector_storage = vector_storages.remove(vector_name).unwrap();
            let data = ReadOnlyVectorData::open_dense(
                fs,
                segment_path,
                vector_name,
                vector_config,
                &config,
                id_tracker.clone(),
                payload_index.clone(),
                vector_storage,
            )?;
            vector_data.insert(vector_name.clone(), data);
        }
        for vector_name in config.sparse_vector_data.keys() {
            let vector_storage = vector_storages.remove(vector_name).unwrap();
            let data = ReadOnlyVectorData::open_sparse(
                fs,
                segment_path,
                vector_name,
                id_tracker.clone(),
                payload_index.clone(),
                vector_storage,
            )?;
            vector_data.insert(vector_name.clone(), data);
        }

        let segment_type = if config.is_any_vector_indexed() {
            SegmentType::Indexed
        } else {
            SegmentType::Plain
        };

        Ok(Self {
            uuid,
            segment_path: segment_path.to_path_buf(),
            id_tracker,
            vector_data,
            payload_index,
            payload_storage,
            pending_reload: AtomicRefCell::new(Default::default()),
            segment_type,
            segment_config: config,
        })
    }
}

impl<S: UniversalReadExt + 'static> ReadOnlyVectorData<S> {
    /// Open one dense vector's quantized vectors and index over `fs`, mirroring
    /// `open_dense_vector_data`. No `prefill`: read-only never writes.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn open_dense(
        fs: &S::Fs,
        segment_path: &Path,
        vector_name: &VectorName,
        vector_config: &VectorDataConfig,
        segment_config: &SegmentConfig,
        id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
        payload_index: Arc<AtomicRefCell<ReadOnlyStructPayloadIndex<S>>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
    ) -> OperationResult<Self> {
        let vector_storage_path = get_vector_storage_path(segment_path, vector_name);
        let vector_index_path = get_vector_index_path(segment_path, vector_name);

        let quantized_vectors = if segment_config.quantization_config(vector_name).is_some() {
            let (distance, datatype, on_disk) = {
                let storage = vector_storage.borrow();
                (storage.distance(), storage.datatype(), storage.is_on_disk())
            };
            ReadOnlyQuantizedVectors::open(
                fs,
                &vector_storage_path,
                distance,
                datatype,
                vector_config.multivector_config.as_ref(),
                on_disk,
            )?
        } else {
            None
        };
        let quantized_vectors = Arc::new(AtomicRefCell::new(quantized_vectors));

        let vector_index = Arc::new(AtomicRefCell::new(VectorIndexReadEnum::open(
            vector_config,
            ReadOnlyVectorIndexOpenArgs {
                fs,
                path: &vector_index_path,
                id_tracker,
                vector_storage: vector_storage.clone(),
                payload_index,
                quantized_vectors: quantized_vectors.clone(),
            },
        )?));

        Ok(Self {
            vector_index,
            vector_storage,
            quantized_vectors,
        })
    }

    /// Open one sparse vector's index over `fs`, mirroring
    /// `open_sparse_vector_data`. Sparse vectors are never quantized.
    pub(super) fn open_sparse(
        fs: &S::Fs,
        segment_path: &Path,
        vector_name: &VectorName,
        id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
        payload_index: Arc<AtomicRefCell<ReadOnlyStructPayloadIndex<S>>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
    ) -> OperationResult<Self> {
        let vector_index_path = get_vector_index_path(segment_path, vector_name);
        let quantized_vectors = Arc::new(AtomicRefCell::new(None));

        let vector_index = Arc::new(AtomicRefCell::new(VectorIndexReadEnum::open_sparse(
            ReadOnlyVectorIndexOpenArgs {
                fs,
                path: &vector_index_path,
                id_tracker,
                vector_storage: vector_storage.clone(),
                payload_index,
                quantized_vectors: quantized_vectors.clone(),
            },
        )?));

        Ok(Self {
            vector_index,
            vector_storage,
            quantized_vectors,
        })
    }
}
