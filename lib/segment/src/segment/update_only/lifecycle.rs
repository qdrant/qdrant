use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::storage_version::{StorageVersion, VERSION_FILE};
use common::universal_io::{CachedReadFs, Populate, UniversalRead, UniversalReadFs, read_json_via};
use uuid::Uuid;

use super::{UpdateOnlySegment, UpdateOnlyVectorData};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::segment::{SEGMENT_STATE_FILE, SegmentVersion};
use crate::segment_constructor::get_vector_storage_path;
use crate::types::{SegmentConfig, SegmentState};
use crate::vector_storage::read_only::VectorStorageReadEnum;
use crate::vector_storage::sparse::read_only::ReadOnlySparseVectorStorage;

/// Every component of an update-only segment is opened cold.
///
/// A writer touches a handful of points scattered across a batch, so warming a
/// storage would fetch far more than it reads — the opposite of the search
/// path, where a warm storage pays for itself over many scored points.
const WRITER_POPULATE: Populate = Populate::No;

impl<S: UniversalRead + 'static> UpdateOnlySegment<S> {
    /// Open the segment's components: the id tracker, the payload storage and
    /// one storage per named vector. Nothing else — see the module docs for why
    /// the indexes and quantized vectors stay on the remote.
    ///
    /// `fs` opens the component files (in production a
    /// [`CachedFs`](common::universal_io::CachedFs) primed by
    /// [`preopen`](Self::preopen)); `raw_fs` is the canonical backend, kept by
    /// components that re-open files after this call and by the segment itself
    /// for its appends.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        raw_fs: &S::Fs,
        segment_path: &Path,
        uuid: Uuid,
    ) -> OperationResult<Self> {
        if SegmentVersion::load_universal(fs, segment_path)?.is_none() {
            // `FileNotFound`, not a service error: the version file is written
            // last, so its absence means the segment vanished mid-open (or was
            // never completed).
            return Err(OperationError::FileNotFound {
                path: segment_path.join(VERSION_FILE),
            });
        }

        let SegmentState {
            initial_version: _,
            version: _,
            config,
        } = read_json_via(fs, segment_path.join(SEGMENT_STATE_FILE))?;

        let payload_storage = Arc::new(AtomicRefCell::new(ReadOnlyPayloadStorage::open(
            fs,
            segment_path.to_path_buf(),
            WRITER_POPULATE,
        )?));

        // Detect the persisted format by attempting each format's open (no
        // per-file `exists` round-trips — important for object-storage
        // backends). No deferred threshold: the writer does not run
        // optimizations, so it has no deferred slot of its own to respect.
        let id_tracker = Arc::new(AtomicRefCell::new(ReadOnlyIdTrackerEnum::detect_and_load(
            fs,
            raw_fs,
            segment_path,
            None,
        )?));

        let mut vector_data = HashMap::new();
        for (vector_name, vector_config) in &config.vector_data {
            let path = get_vector_storage_path(segment_path, vector_name);
            let storage =
                VectorStorageReadEnum::open(fs, vector_config, &path, Some(WRITER_POPULATE))?
                    .ok_or_else(|| {
                        OperationError::service_error(format!(
                            "Dense vector storage '{vector_name}' was not found, or is corrupted.",
                        ))
                    })?;
            vector_data.insert(
                vector_name.clone(),
                UpdateOnlyVectorData {
                    vector_storage: Arc::new(AtomicRefCell::new(storage)),
                },
            );
        }
        for vector_name in config.sparse_vector_data.keys() {
            let path = get_vector_storage_path(segment_path, vector_name);
            let storage = VectorStorageReadEnum::Sparse(Box::new(
                ReadOnlySparseVectorStorage::open(fs, &path, WRITER_POPULATE)?,
            ));
            vector_data.insert(
                vector_name.clone(),
                UpdateOnlyVectorData {
                    vector_storage: Arc::new(AtomicRefCell::new(storage)),
                },
            );
        }

        let appendable = config.is_appendable();

        Ok(Self {
            uuid,
            segment_path: segment_path.to_path_buf(),
            fs: raw_fs.clone(),
            id_tracker,
            payload_storage,
            vector_data,
            segment_config: config,
            appendable,
        })
    }

    /// Schedule the prefetch of every file [`open`](Self::open) will read, so a
    /// caching filesystem can fetch them concurrently instead of one blocking
    /// round-trip at a time. Returns the segment config parsed along the way,
    /// so the open does not read it twice.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        segment_path: &Path,
    ) -> OperationResult<SegmentConfig> {
        let SegmentState {
            initial_version: _,
            version: _,
            config,
        } = read_json_via(fs, segment_path.join(SEGMENT_STATE_FILE))?;

        ReadOnlyPayloadStorage::preopen(fs, segment_path.to_path_buf(), WRITER_POPULATE)?;
        ReadOnlyIdTrackerEnum::preopen(fs, segment_path)?;

        for (vector_name, vector_config) in &config.vector_data {
            let path = get_vector_storage_path(segment_path, vector_name);
            VectorStorageReadEnum::<S>::preopen(fs, vector_config, &path, Some(WRITER_POPULATE))?;
        }
        for vector_name in config.sparse_vector_data.keys() {
            let path = get_vector_storage_path(segment_path, vector_name);
            ReadOnlySparseVectorStorage::<S>::preopen(fs, &path, WRITER_POPULATE)?;
        }

        Ok(config)
    }
}
