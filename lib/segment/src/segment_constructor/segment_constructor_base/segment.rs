use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use common::defaults::log_load_timing;
use common::fs::{safe_delete_with_suffix, sync_parent_dir};
use common::storage_version::StorageVersion;
use common::types::PointOffsetType;
use common::universal_io::MmapFs;
use fs_err as fs;
use log::info;
use uuid::Uuid;

use super::create_segment::create_segment;
use super::legacy_state::{load_segment_state_v3, load_segment_state_v5};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::segment::{Segment, SegmentVersion};
use crate::types::SegmentConfig;

/// Proof that a segment was just built, obliging the builder to register it in the segment manifest.
///
/// Returned by [`build_segment`] (and the shard-level builders that wrap it). The `#[must_use]` lint
/// turns "built a segment but forgot to register it" into a compiler warning: discharge it by
/// registering the segment (its [`id`](Self::id) is the segment UUID), or drop it explicitly
/// (`let _ = token`) for segments that will not join a manifest-tracked holder (loading, tests,
/// transient segments).
///
/// Carries only the segment UUID; there is deliberately no `Drop` bomb — the lint is the enforcement.
#[must_use = "a newly built segment must be registered in the segment manifest, or explicitly dropped"]
pub struct NewSegmentToken(Uuid);

impl NewSegmentToken {
    /// Mint a token for a freshly built segment. Restricted to this crate so the obligation can
    /// only originate from a real segment build ([`build_segment`]).
    pub(crate) fn new(uuid: Uuid) -> Self {
        NewSegmentToken(uuid)
    }

    /// UUID of the newly built segment, to register in the manifest.
    pub fn id(&self) -> Uuid {
        self.0
    }
}

/// Normalize segment directory.
///
/// Might delete or rename the directory.
/// Returns `None` if the segment directory was deleted.
pub fn normalize_segment_dir(path: &Path) -> OperationResult<Option<(PathBuf, Uuid)>> {
    // 1. Delete dirs like `5345474d-454e-54f0-9f98-ba206e616d65.deleted`.
    // These are leftovers from rename-then-delete approach.
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext == "deleted")
        .unwrap_or(false)
    {
        log::warn!("Deleting leftover segment: {}", path.display());
        safe_delete_with_suffix(path).map_err(|err| {
            OperationError::service_error(format!("failed to delete leftover segment: {err}"))
        })?;
        return Ok(None);
    }

    // 2. Delete dirs without proper `version.info` file inside.
    // These segments are not properly saved.
    // Likely, the server crashed during saving.
    if SegmentVersion::load_universal(&MmapFs, path)?.is_none() {
        log::warn!("Deleting segment without version file: {}", path.display());
        safe_delete_with_suffix(path).map_err(|err| {
            OperationError::service_error(format!("failed to delete leftover segment: {err}"))
        })?;
        return Ok(None);
    }

    // 3. Force directory name to be a valid UUID.
    // Rename if necessary.
    let file_name = path
        .file_name()
        .and_then(|fname| fname.to_str())
        .ok_or_else(|| {
            OperationError::service_error(format!(
                "Failed to get segment folder name: {}",
                path.display()
            ))
        })?;
    match Uuid::try_parse(file_name) {
        Ok(uuid) => Ok(Some((path.to_path_buf(), uuid))),
        Err(_) => {
            let segment_uuid = Uuid::new_v4();
            let new_path = path.with_file_name(segment_uuid.to_string());
            log::warn!(
                "Segment name is not a valid UUID: {}. Renaming to {segment_uuid}",
                path.display(),
            );
            fs::rename(path, &new_path)?;
            sync_parent_dir(&new_path)?;
            Ok(Some((new_path, segment_uuid)))
        }
    }
}

/// Load segment from given `path`.
///
/// Preferably, the `uuid` should match the last component of `path`.
/// In production use [`normalize_segment_dir`] to obtain correct path and UUID.
/// In tests it is acceptable to pass an arbitrary UUID, e.g., [`Uuid::nil()`].
pub fn load_segment(
    path: &Path,
    uuid: Uuid,
    deferred_internal_id: Option<PointOffsetType>,
    stopped: &AtomicBool,
) -> OperationResult<Segment> {
    let total_started = Instant::now();

    let stored_version = SegmentVersion::load_universal(&MmapFs, path)?.ok_or_else(|| {
        OperationError::service_error(format!(
            "Segment version file not found in segment: {}",
            path.display()
        ))
    })?;

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

    let started = Instant::now();
    let segment_state = Segment::load_state(path)?;
    log_load_timing(path, "load_state", started);

    let segment = create_segment(
        segment_state.initial_version,
        segment_state.version,
        path,
        uuid,
        deferred_internal_id,
        &segment_state.config,
        stopped,
        false,
    )?;

    log_load_timing(path, "total", total_started);

    Ok(segment)
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
    deferred_internal_id: Option<PointOffsetType>,
    ready: bool,
) -> OperationResult<(Segment, NewSegmentToken)> {
    let uuid = Uuid::new_v4();
    let token = NewSegmentToken::new(uuid);

    let segment_path = segments_path.join(uuid.to_string());
    let stopped = AtomicBool::new(false);

    fs::create_dir_all(&segment_path)?;
    let segment = create_segment(
        None,
        None,
        &segment_path,
        uuid,
        deferred_internal_id,
        config,
        &stopped,
        true,
    )?;
    segment.save_current_state()?;

    // Version is the last file to save, as it will be used to check if segment was built correctly.
    // If it is not saved, segment will be skipped.
    if ready {
        SegmentVersion::save(&segment_path)?;
    }

    Ok((segment, token))
}
