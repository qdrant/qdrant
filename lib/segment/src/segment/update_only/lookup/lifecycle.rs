use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::storage_version::{StorageVersion, VERSION_FILE};
use common::types::PointOffsetType;
use common::universal_io::{
    CachedFs, CachedReadFs, Populate, UniversalRead, UniversalReadFs, UniversalReadFsAsync,
    read_json_via,
};

use super::LookupSegment;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::payload_storage::read_only::ReadOnlyPayloadStorage;
use crate::segment::{SEGMENT_STATE_FILE, SegmentVersion};
use crate::segment_constructor::{get_vector_index_path, get_vector_storage_path};
use crate::types::{SegmentConfig, SegmentState};
use crate::vector_storage::read_only::VectorStorageReadEnum;
use crate::vector_storage::sparse::read_only::ReadOnlySparseVectorStorage;

/// Every component of an update-only segment is opened cold: a writer touches
/// a handful of scattered points, so warming a storage would fetch far more
/// than it reads.
///
/// On a remote backend this is also what keeps [`preopen`] cheap: a prefetch
/// is an open, and an open with `Populate::No` transfers no content — so the
/// data files (vectors, payload pages) are never downloaded, only the files
/// whose opens consume them whole (the configs, the id tracker, the deleted
/// flags).
///
/// [`preopen`]: LookupSegment::preopen
const WRITER_POPULATE: Populate = Populate::No;

/// Build the per-segment [`CachedFs`] an open runs over. Preloads statically
/// known files.
///
/// Mirror of the read-only segment's `build_cached_fs`, minus the payload index
/// config the writer never opens.
fn build_cached_fs<Fs: UniversalReadFsAsync>(
    fs: &Fs,
    segment_path: &Path,
) -> OperationResult<CachedFs<Fs>> {
    let mut cached_fs = CachedFs::new(fs.clone(), segment_path)?;

    cached_fs.cache_file_info()?;

    // Absence is tolerated here: the subsequent read reports it gracefully.
    for file_name in [VERSION_FILE, SEGMENT_STATE_FILE] {
        cached_fs.schedule_open(&segment_path.join(file_name), None, None);
    }

    Ok(cached_fs)
}

impl<S: UniversalRead + 'static> LookupSegment<S> {
    /// Open the segment over a per-segment [`CachedFs`]: every file the
    /// components will read is prefetched concurrently
    /// ([`preopen`](Self::preopen)) before the component opens consume it, so
    /// a remote backend pays for the depth of the longest dependent chain
    /// rather than one blocking round-trip per file.
    ///
    /// `fs` is the canonical backend: the caching wrapper lives only for this
    /// open, and it is `fs` that components keep for later re-opens.
    ///
    /// `deferred_internal_id` is the cutoff agreed with an external rebuilder
    /// working the same directory — see [`open_via`](Self::open_via).
    pub fn open(
        fs: &impl UniversalReadFsAsync<File = S>,
        segment_path: &Path,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OperationResult<Self> {
        let cached_fs = build_cached_fs(fs, segment_path)?;
        let config = Self::preopen(&cached_fs, segment_path)?;
        Self::open_via(&cached_fs, segment_path, config, deferred_internal_id)
    }

    /// Open the segment's components: the id tracker, the payload storage and
    /// one storage per named vector — nothing else.
    ///
    /// `fs` opens the component files (in production the [`CachedFs`] that
    /// [`open`](Self::open) primed). `config` is the one
    /// [`preopen`](Self::preopen) already parsed, so the state file is not
    /// read twice.
    ///
    /// `deferred_internal_id` is the cutoff agreed with an external rebuilder
    /// working the same directory: slots at or above it load into the id
    /// tracker's deferred track, marking them as outside the rebuild snapshot.
    /// It does not hide anything from the writer — resolution runs
    /// `WithDeferred`, so every point still locates at its latest slot. `None`
    /// (a writer running alone) keeps every mapping active. Only the
    /// appendable segment has a deferred track; on any other segment the
    /// cutoff is ignored.
    pub fn open_via(
        fs: &impl UniversalReadFs<File = S>,
        segment_path: &Path,
        config: SegmentConfig,
        deferred_internal_id: Option<PointOffsetType>,
    ) -> OperationResult<Self> {
        if SegmentVersion::load_universal(fs, segment_path)?.is_none() {
            // `FileNotFound`, not a service error: the version file is written
            // last, so its absence means the segment vanished mid-open (or was
            // never completed).
            return Err(OperationError::FileNotFound {
                path: segment_path.join(VERSION_FILE),
            });
        }

        let payload_storage = Arc::new(AtomicRefCell::new(ReadOnlyPayloadStorage::open(
            fs,
            segment_path.to_path_buf(),
            WRITER_POPULATE,
        )?));

        let appendable = config.is_appendable();

        // Detect the persisted format by attempting each format's open. The
        // deferred threshold applies to the appendable tracker only, mirroring
        // `ReadOnlySegment::open_via`.
        let id_tracker = Arc::new(AtomicRefCell::new(ReadOnlyIdTrackerEnum::detect_and_load(
            fs,
            segment_path,
            deferred_internal_id.filter(|_| appendable),
        )?));

        let mut vector_data = HashMap::new();
        for (vector_name, vector_config) in &config.vector_data {
            let path = get_vector_storage_path(segment_path, vector_name);
            let index_path = get_vector_index_path(segment_path, vector_name);
            let storage = VectorStorageReadEnum::open(
                fs,
                vector_config,
                &path,
                &index_path,
                Some(WRITER_POPULATE),
            )?
            .ok_or_else(|| {
                OperationError::service_error(format!(
                    "Dense vector storage '{vector_name}' was not found, or is corrupted.",
                ))
            })?;
            vector_data.insert(vector_name.clone(), Arc::new(AtomicRefCell::new(storage)));
        }
        for vector_name in config.sparse_vector_data.keys() {
            let path = get_vector_storage_path(segment_path, vector_name);
            let storage = VectorStorageReadEnum::Sparse(Box::new(
                ReadOnlySparseVectorStorage::open(fs, &path, WRITER_POPULATE)?,
            ));
            vector_data.insert(vector_name.clone(), Arc::new(AtomicRefCell::new(storage)));
        }

        Ok(Self {
            segment_path: segment_path.to_path_buf(),
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
            let index_path = get_vector_index_path(segment_path, vector_name);
            VectorStorageReadEnum::<S>::preopen(
                fs,
                vector_config,
                &path,
                &index_path,
                Some(WRITER_POPULATE),
            )?;
        }
        for vector_name in config.sparse_vector_data.keys() {
            let path = get_vector_storage_path(segment_path, vector_name);
            ReadOnlySparseVectorStorage::<S>::preopen(fs, &path, WRITER_POPULATE)?;
        }

        Ok(config)
    }
}
