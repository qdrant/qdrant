use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::storage_version::{StorageVersion, VERSION_FILE};
use common::types::PointOffsetType;
use common::universal_io::{
    CachedFs, CachedReadFs, OkNotFound, Populate, UniversalReadFs, read_json_via,
};
use uuid::Uuid;

use super::{ReadOnlySegment, ReadOnlyVectorData};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::load_profile::LoadProfile;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::index::UniversalReadExt;
use crate::index::payload_config::PayloadConfig;
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

/// Build a per-segment [`CachedReadFs`] over `segment_path`.
///
/// The files whose names are known in advance (version file, segment state)
/// are scheduled *before* the listing snapshot is taken, so on backends with
/// background population their fetch overlaps the listing round-trip.
fn build_cached_fs<Fs: UniversalReadFs>(
    fs: &Fs,
    segment_path: &Path,
) -> OperationResult<CachedFs<Fs>> {
    let mut cached_fs = CachedFs::new(fs.clone(), segment_path)?;

    // Absence is tolerated here: the subsequent read reports it gracefully.
    for file_name in [VERSION_FILE, SEGMENT_STATE_FILE] {
        cached_fs
            .schedule_prefetch(&segment_path.join(file_name), None, None)
            .ok_not_found()?;
    }

    // Payload index config
    cached_fs
        .schedule_prefetch(
            &PayloadConfig::get_config_path(&get_payload_index_path(segment_path)),
            None,
            None,
        )
        .ok_not_found()?;

    cached_fs.cache_file_info()?;

    Ok(cached_fs)
}

/// How the payload storage of a segment with `config` is brought into memory.
fn payload_populate(config: &SegmentConfig) -> Populate {
    match config.payload_storage_type {
        PayloadStorageType::InRamMmap => Populate::PreferBackground,
        PayloadStorageType::Mmap => Populate::No,
    }
}

impl<S: UniversalReadExt + 'static> ReadOnlySegment<S> {
    /// Open the segment over a per-segment [`CachedReadFs`]: known files are
    /// prefetched before the listing snapshot is taken (see
    /// [`build_cached_fs`]), and probes for optional files resolve against
    /// the snapshot, without inner-filesystem round-trips.
    ///
    /// A `load_profile` — derived from the one request this open serves —
    /// overrides the config-derived placement of the components that request
    /// does not touch, parking them cold instead of warming them (see
    /// [`LoadProfile`]). Without one, every component loads as the persisted
    /// segment config says.
    pub fn open(
        fs: &S::Fs,
        segment_path: &Path,
        uuid: Uuid,
        deferred_internal_id: Option<PointOffsetType>,
        load_profile: Option<&LoadProfile>,
    ) -> OperationResult<Self> {
        let cached_fs = build_cached_fs(fs, segment_path)?;
        let (segment_config, payload_config) =
            Self::first_preopen(&cached_fs, segment_path, load_profile)?;
        Self::open_via(
            &cached_fs,
            fs,
            segment_path,
            segment_config,
            payload_config,
            uuid,
            deferred_internal_id,
            load_profile,
        )
    }

    /// Schedule the prefetch of every file the segment's components will open,
    /// returning the configs parsed along the way so [`open_via`](Self::open_via)
    /// does not have to read them a second time.
    fn first_preopen(
        fs: &impl CachedReadFs<File = S>,
        segment_path: &Path,
        load_profile: Option<&LoadProfile>,
    ) -> OperationResult<(SegmentConfig, PayloadConfig)> {
        let SegmentState {
            initial_version: _,
            version: _,
            config,
        } = read_json_via(fs, segment_path.join(SEGMENT_STATE_FILE))?;

        // Payload storage
        let payload_storage_populate = load_profile
            .and_then(|profile| profile.payload_storage_placement())
            .unwrap_or_else(|| payload_populate(&config));
        ReadOnlyPayloadStorage::preopen(fs, segment_path.to_path_buf(), payload_storage_populate)?;

        // Id tracker; always loaded — every request resolves ids through it.
        ReadOnlyIdTrackerEnum::preopen(fs, segment_path)?;

        // Vector storages
        for (vector_name, vector_config) in &config.vector_data {
            let path = get_vector_storage_path(segment_path, vector_name);
            let storage_populate =
                load_profile.and_then(|profile| profile.vector_storage_placement(vector_name));
            VectorStorageReadEnum::<S>::preopen(fs, vector_config, &path, storage_populate)?;

            // Quantized vectors live in the vector storage directory; a no-op
            // when quantization isn't configured for this vector.
            let quantized_populate =
                load_profile.and_then(|profile| profile.quantized_vectors_placement(vector_name));
            ReadOnlyQuantizedVectors::<S>::preopen(fs, &path, vector_config, quantized_populate)?;

            // Vector index
            let index_path = get_vector_index_path(segment_path, vector_name);
            let index_populate =
                load_profile.and_then(|profile| profile.vector_index_placement(vector_name));
            VectorIndexReadEnum::<S>::preopen(fs, vector_config, &index_path, index_populate)?;
        }
        for (vector_name, sparse_vector_config) in &config.sparse_vector_data {
            let path = get_vector_storage_path(segment_path, vector_name);
            ReadOnlySparseVectorStorage::<S>::preopen(fs, &path)?;

            // Sparse vector index
            let index_path = get_vector_index_path(segment_path, vector_name);
            let index_populate =
                load_profile.and_then(|profile| profile.vector_index_placement(vector_name));
            VectorIndexReadEnum::<S>::preopen_sparse(
                fs,
                sparse_vector_config,
                &index_path,
                index_populate,
            )?;
        }

        // Payload indexes
        let payload_config = ReadOnlyStructPayloadIndex::preopen(
            fs,
            &get_payload_index_path(segment_path),
            load_profile,
        )?;

        Ok((config, payload_config))
    }

    /// Read-only mirror of `load_segment`: assembles every read-only component
    /// from `fs` (id tracker, payload storage+index, per-vector storage/index). No writes.
    ///
    /// `fs` is any filesystem producing `S`-typed handles — in production the
    /// per-segment [`CachedReadFs`], whose opens are served from its prefetch
    /// pool. `raw_fs` is the canonical backend, for the one component that
    /// stores a filesystem handle to re-open appended files later (the
    /// appendable id tracker): a caching wrapper's snapshot would go stale.
    ///
    /// `config` and `payload_config` are the ones
    /// [`first_preopen`](Self::first_preopen) already parsed off `fs`, and
    /// `load_profile` is the profile that preopen already applied — the opens
    /// here must make the same placement decisions the prefetches did.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_via(
        fs: &impl CachedReadFs<File = S>,
        raw_fs: &S::Fs,
        segment_path: &Path,
        config: SegmentConfig,
        payload_config: PayloadConfig,
        uuid: Uuid,
        deferred_internal_id: Option<PointOffsetType>,
        load_profile: Option<&LoadProfile>,
    ) -> OperationResult<Self> {
        if SegmentVersion::load_universal(fs, segment_path)?.is_none() {
            // `FileNotFound`, not a service error: the version file is written last, so
            // its absence means the segment vanished mid-open (or was never completed) —
            // a follower resolves that against the segment manifest.
            return Err(OperationError::FileNotFound {
                path: segment_path.join(VERSION_FILE),
            });
        }

        let is_appendable = config.is_appendable();
        let deferred_internal_id = deferred_internal_id.filter(|_| is_appendable);

        let payload_storage_populate = load_profile
            .and_then(|profile| profile.payload_storage_placement())
            .unwrap_or_else(|| payload_populate(&config));
        let payload_storage = Arc::new(AtomicRefCell::new(ReadOnlyPayloadStorage::open(
            fs,
            segment_path.to_path_buf(),
            payload_storage_populate,
        )?));

        // Detect the persisted format by attempting each format's open (no
        // per-file `exists` round-trips — important for object-storage backends).
        let id_tracker = Arc::new(AtomicRefCell::new(ReadOnlyIdTrackerEnum::detect_and_load(
            fs,
            raw_fs,
            segment_path,
            deferred_internal_id,
        )?));

        // Open all vector storages up front: the payload index needs them.
        let mut vector_storages: HashMap<
            VectorNameBuf,
            Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
        > = HashMap::new();
        for (vector_name, vector_config) in &config.vector_data {
            let path = get_vector_storage_path(segment_path, vector_name);
            let storage_populate =
                load_profile.and_then(|profile| profile.vector_storage_placement(vector_name));
            let storage = VectorStorageReadEnum::open(fs, vector_config, &path, storage_populate)?
                .ok_or_else(|| {
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
            payload_config,
            load_profile,
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
                load_profile,
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
    /// `load_profile` mirrors the preopen's per-vector placement decisions.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn open_dense(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
        vector_name: &VectorName,
        vector_config: &VectorDataConfig,
        segment_config: &SegmentConfig,
        id_tracker: Arc<AtomicRefCell<ReadOnlyIdTrackerEnum<S>>>,
        payload_index: Arc<AtomicRefCell<ReadOnlyStructPayloadIndex<S>>>,
        vector_storage: Arc<AtomicRefCell<VectorStorageReadEnum<S>>>,
        load_profile: Option<&LoadProfile>,
    ) -> OperationResult<Self> {
        let vector_storage_path = get_vector_storage_path(segment_path, vector_name);
        let vector_index_path = get_vector_index_path(segment_path, vector_name);

        let quantized_vectors = if segment_config.quantization_config(vector_name).is_some() {
            let (distance, datatype, on_disk) = {
                let storage = vector_storage.borrow();
                (storage.distance(), storage.datatype(), storage.is_on_disk())
            };
            let quantized_populate =
                load_profile.and_then(|profile| profile.quantized_vectors_placement(vector_name));
            ReadOnlyQuantizedVectors::open(
                fs,
                &vector_storage_path,
                distance,
                datatype,
                vector_config.multivector_config.as_ref(),
                on_disk,
                quantized_populate,
            )?
        } else {
            None
        };
        let quantized_vectors = Arc::new(AtomicRefCell::new(quantized_vectors));

        let index_populate =
            load_profile.and_then(|profile| profile.vector_index_placement(vector_name));
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
            index_populate,
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
        fs: &impl UniversalReadFs<File = S>,
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
