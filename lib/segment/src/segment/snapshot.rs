use std::collections::{HashMap, HashSet};
use std::io::{Seek, Write};
use std::ops::Deref as _;
use std::path::{Path, PathBuf};
use std::{fmt, fs, thread};

use common::tar_ext;
use io::storage_version::VERSION_FILE;
use uuid::Uuid;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::segment_manifest::{FileVersion, SegmentManifest, SegmentManifests};
use crate::entry::SegmentEntry as _;
use crate::entry::snapshot_entry::SnapshotEntry;
use crate::index::{PayloadIndex, VectorIndex};
use crate::payload_storage::PayloadStorage;
use crate::segment::{
    DB_BACKUP_PATH, PAYLOAD_DB_BACKUP_PATH, SEGMENT_STATE_FILE, SNAPSHOT_FILES_PATH, SNAPSHOT_PATH,
    Segment,
};
use crate::types::SnapshotFormat;
use crate::utils::path::strip_prefix;
use crate::vector_storage::VectorStorage;

pub const ROCKS_DB_VIRT_FILE: &str = "::ROCKS_DB";
pub const PAYLOAD_INDEX_ROCKS_DB_VIRT_FILE: &str = "::PAYLOAD_INDEX_ROCKS_DB";

impl SnapshotEntry for Segment {
    fn take_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<&SegmentManifests>,
        snapshotted_segments: &mut HashSet<String>,
    ) -> OperationResult<()> {
        let segment_id = self.segment_id()?;

        log::debug!("Taking snapshot of segment {segment_id}");

        if !snapshotted_segments.insert(segment_id.to_string()) {
            // Already snapshotted.
            log::debug!("Segment {segment_id} is already snapshotted!");
            return Ok(());
        }

        // flush segment to capture latest state
        self.flush(true, false)?;

        let include_files = match manifest {
            None => HashSet::new(),

            Some(manifest) => {
                let updated_manifest = self.get_segment_manifest()?;

                let updated_manifest_json =
                    serde_json::to_vec(&updated_manifest).map_err(|err| {
                        OperationError::service_error(format!(
                            "failed to serialize segment manifest into JSON: {err}"
                        ))
                    })?;

                let tar = tar.descend(Path::new(&segment_id))?;
                tar.blocking_append_data(
                    &updated_manifest_json,
                    Path::new("files/segment_manifest.json"),
                )?;

                let mut empty_manifest = None;
                let request_manifest = manifest
                    .get(segment_id)
                    .unwrap_or_else(|| empty_manifest.insert(SegmentManifest::empty(segment_id)));

                updated_files(request_manifest, &updated_manifest)
            }
        };

        let include_if = |path: &Path| {
            if manifest.is_none() {
                true
            } else {
                include_files.contains(path)
            }
        };

        match format {
            SnapshotFormat::Ancient => {
                debug_assert!(false, "Unsupported snapshot format: {format:?}");
                return Err(OperationError::service_error(format!(
                    "Unsupported snapshot format: {format:?}"
                )));
            }
            SnapshotFormat::Regular => {
                tar.blocking_write_fn(Path::new(&format!("{segment_id}.tar")), |writer| {
                    let tar = tar_ext::BuilderExt::new_streaming_borrowed(writer);
                    let tar = tar.descend(Path::new(SNAPSHOT_PATH))?;
                    snapshot_files(self, temp_path, &tar, include_if)
                })??;
            }
            SnapshotFormat::Streamable => {
                let tar = tar.descend(Path::new(&segment_id))?;
                snapshot_files(self, temp_path, &tar, include_if)?;
            }
        }

        Ok(())
    }

    fn collect_segment_manifests(&self, manifests: &mut SegmentManifests) -> OperationResult<()> {
        manifests.add(self.get_segment_manifest()?);
        Ok(())
    }
}

impl Segment {
    fn segment_id(&self) -> OperationResult<&str> {
        let id = self
            .current_path
            .file_stem()
            .and_then(|segment_dir| segment_dir.to_str())
            .ok_or_else(|| {
                OperationError::service_error(format!(
                    "failed to extract segment ID from segment path {}",
                    self.current_path.display(),
                ))
            })?;

        debug_assert!(
            Uuid::try_parse(id).is_ok(),
            "segment ID {id} is not a valid UUID",
        );

        Ok(id)
    }

    fn get_segment_manifest(&self) -> OperationResult<SegmentManifest> {
        let segment_id = self.segment_id()?;
        let segment_version = self.version();

        let all_files = self.files();
        let versioned_files = self.versioned_files();

        // Note, that `all_files` should already contain all `versioned_files`, so each versioned file
        // would be iterated over *twice*:
        // - first as `FileVersion::Unversioned`
        // - and then with correct `FileVersion::Version`
        //
        // The loop below is structured to do safety-checks and work around this correctly.
        let all_files = all_files
            .into_iter()
            .map(|path| (path, FileVersion::Unversioned))
            .chain(
                versioned_files
                    .into_iter()
                    .map(|(path, version)| (path, FileVersion::from(version))),
            );

        let mut file_versions = HashMap::new();

        for (path, version) in all_files {
            // All segment files should be contained within segment directory
            debug_assert!(
                path.starts_with(&self.current_path),
                "segment file {} is not contained within segment directory {}",
                path.display(),
                self.current_path.display(),
            );

            let path = strip_prefix(&path, &self.current_path)?;
            let prev_version = file_versions.insert(path.to_path_buf(), version);

            // `all_files` should iterate over versioned files twice: first as unversioned,
            // and then once again with correct file version.
            //
            // These assertions check that each file in the manifest is unique:
            // - unversioned file can only be added once, it should *never* override existing entry
            // - versioned file can only be added once, after same file was added as unversioned,
            //   it should *always* override existing *unversioned* entry
            // - no file can *ever* override existing *versioned* entry
            if version.is_unversioned() {
                // Unversioned file should never override existing entry
                debug_assert_eq!(
                    prev_version,
                    None,
                    "unversioned segment file {} overrode versioned entry {:?}",
                    path.display(),
                    prev_version.unwrap(),
                );
            } else {
                // Versioned file should always override unversioned entry
                //
                // Split into two separate assertions to provide better error messages

                debug_assert_ne!(
                    prev_version,
                    None,
                    "segment file {} with version {:?} did not override existing entry",
                    path.display(),
                    version,
                );

                debug_assert_eq!(
                    prev_version,
                    Some(FileVersion::Unversioned),
                    "segment file {} with version {:?} overrode versioned entry {:?}",
                    path.display(),
                    version,
                    prev_version.unwrap(),
                );
            }
        }

        // TODO: Version RocksDB!? 🤯
        file_versions.insert(PathBuf::from(ROCKS_DB_VIRT_FILE), FileVersion::Unversioned);
        file_versions.insert(
            PathBuf::from(PAYLOAD_INDEX_ROCKS_DB_VIRT_FILE),
            FileVersion::Unversioned,
        );

        Ok(SegmentManifest {
            segment_id: segment_id.into(),
            segment_version,
            file_versions,
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = Vec::new();

        for vector_data in self.vector_data.values() {
            files.extend(vector_data.vector_index.borrow().files());
            files.extend(vector_data.vector_storage.borrow().files());

            if let Some(quantized_vectors) = vector_data.quantized_vectors.borrow().deref() {
                files.extend(quantized_vectors.files());
            }
        }

        files.extend(self.payload_index.borrow().files());
        files.extend(self.payload_storage.borrow().files());

        files.extend(self.id_tracker.borrow().files());

        files
    }

    fn versioned_files(&self) -> Vec<(PathBuf, u64)> {
        let mut files = Vec::new();

        for vector_data in self.vector_data.values() {
            files.extend(vector_data.vector_index.borrow().versioned_files());
            files.extend(vector_data.vector_storage.borrow().versioned_files());

            if let Some(quantized_vectors) = vector_data.quantized_vectors.borrow().deref() {
                files.extend(quantized_vectors.versioned_files());
            }
        }

        files.extend(self.payload_index.borrow().versioned_files());
        files.extend(self.payload_storage.borrow().versioned_files());

        files.extend(self.id_tracker.borrow().versioned_files());

        files
    }
}

pub fn snapshot_files(
    segment: &Segment,
    temp_path: &Path,
    tar: &tar_ext::BuilderExt<impl Write + Seek>,
    include_if: impl Fn(&Path) -> bool,
) -> OperationResult<()> {
    // use temp_path for intermediary files
    let temp_path = temp_path.join(format!("segment-{}", Uuid::new_v4()));

    // TODO: Version RocksDB!? 🤯

    if include_if(ROCKS_DB_VIRT_FILE.as_ref()) {
        let db_backup_path = temp_path.join(DB_BACKUP_PATH);

        let db = segment.database.read();
        crate::rocksdb_backup::create(&db, &db_backup_path).map_err(|err| {
            OperationError::service_error(format!(
                "failed to create RocksDB backup at {}: {err}",
                db_backup_path.display()
            ))
        })?;
    }

    if include_if(PAYLOAD_INDEX_ROCKS_DB_VIRT_FILE.as_ref()) {
        let payload_index_db_backup_path = temp_path.join(PAYLOAD_DB_BACKUP_PATH);

        segment
            .payload_index
            .borrow()
            .take_database_snapshot(&payload_index_db_backup_path)
            .map_err(|err| {
                OperationError::service_error(format!(
                    "failed to create payload index RocksDB backup at {}: {err}",
                    payload_index_db_backup_path.display()
                ))
            })?;
    }

    if temp_path.exists() {
        tar.blocking_append_dir_all(&temp_path, Path::new(""))
            .map_err(|err| {
                OperationError::service_error(format!(
                    "failed to add RockDB backup {} into snapshot: {err}",
                    temp_path.display()
                ))
            })?;

        // remove tmp directory in background
        let _ = thread::spawn(move || {
            let res = fs::remove_dir_all(&temp_path);
            if let Err(err) = res {
                log::error!(
                    "failed to remove temporary directory {}: {err}",
                    temp_path.display(),
                );
            }
        });
    }

    let tar = tar.descend(Path::new(SNAPSHOT_FILES_PATH))?;

    for vector_data in segment.vector_data.values() {
        for file in vector_data.vector_index.borrow().files() {
            let stripped_path = strip_prefix(&file, &segment.current_path)?;

            if include_if(stripped_path) {
                tar.blocking_append_file(&file, stripped_path)
                    .map_err(|err| failed_to_add("vector index file", &file, err))?;
            }
        }

        for file in vector_data.vector_storage.borrow().files() {
            let stripped_path = strip_prefix(&file, &segment.current_path)?;

            if include_if(stripped_path) {
                tar.blocking_append_file(&file, stripped_path)
                    .map_err(|err| failed_to_add("vector storage file", &file, err))?;
            }
        }

        if let Some(quantized_vectors) = vector_data.quantized_vectors.borrow().as_ref() {
            for file in quantized_vectors.files() {
                let stripped_path = strip_prefix(&file, &segment.current_path)?;

                if include_if(stripped_path) {
                    tar.blocking_append_file(&file, stripped_path)
                        .map_err(|err| failed_to_add("quantized vectors file", &file, err))?;
                }
            }
        }
    }

    for file in segment.payload_index.borrow().files() {
        let stripped_path = strip_prefix(&file, &segment.current_path)?;

        if include_if(stripped_path) {
            tar.blocking_append_file(&file, stripped_path)
                .map_err(|err| failed_to_add("payload index file", &file, err))?;
        }
    }

    for file in segment.payload_storage.borrow().files() {
        let stripped_path = strip_prefix(&file, &segment.current_path)?;

        if include_if(stripped_path) {
            tar.blocking_append_file(&file, stripped_path)
                .map_err(|err| failed_to_add("payload storage file", &file, err))?;
        }
    }

    for file in segment.id_tracker.borrow().files() {
        let stripped_path = strip_prefix(&file, &segment.current_path)?;

        if include_if(stripped_path) {
            tar.blocking_append_file(&file, stripped_path)
                .map_err(|err| failed_to_add("id tracker file", &file, err))?;
        }
    }

    let segment_state_path = segment.current_path.join(SEGMENT_STATE_FILE);
    tar.blocking_append_file(&segment_state_path, Path::new(SEGMENT_STATE_FILE))
        .map_err(|err| failed_to_add("segment state file", &segment_state_path, err))?;

    let version_file_path = segment.current_path.join(VERSION_FILE);
    tar.blocking_append_file(&version_file_path, Path::new(VERSION_FILE))
        .map_err(|err| failed_to_add("segment version file", &version_file_path, err))?;

    Ok(())
}

fn failed_to_add(what: &str, path: &Path, err: impl fmt::Display) -> OperationError {
    OperationError::service_error(format!(
        "failed to add {what} {} into snapshot: {err}",
        path.display(),
    ))
}

fn updated_files(old: &SegmentManifest, current: &SegmentManifest) -> HashSet<PathBuf> {
    // Compare two segment manifests, and return a list of files from `current` manifest, that
    // should be included into partial snapshot.

    let mut updated = HashSet::new();

    for (path, &current_version) in &current.file_versions {
        // Include file into partial snapshot if:
        //
        // 1. `old` manifest does not contain this file
        let Some(old_version) = old.file_versions.get(path).copied() else {
            updated.insert(path.clone());
            continue;
        };

        // 2. if `old` manifest contains this file and file/segment in `current` manifest is *newer*:
        //    - if file is `Unversioned` in both manifests, compare segment versions
        //    - if file is versioned in *one* of the manifests only, compare *file* version against
        //      other *segment* version
        //    - if file is versioned in both manifests, compare file versions
        let is_updated = old_version.or_segment_version(old.segment_version)
            < current_version.or_segment_version(current.segment_version);

        if is_updated {
            updated.insert(path.clone());
        }
    }

    updated
}
