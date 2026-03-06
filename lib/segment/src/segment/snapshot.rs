use std::collections::{HashMap, HashSet};
use std::io::{Seek, Write};
use std::ops::Deref as _;
use std::path::{Path, PathBuf};
use std::{fmt, thread};

use common::storage_version::VERSION_FILE;
use common::tar_ext;
use fs_err as fs;
use uuid::Uuid;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::manifest::{FileVersion, SegmentManifest};
use crate::entry::NonAppendableSegmentEntry as _;
use crate::entry::snapshot_entry::SnapshotEntry;
use crate::index::{PayloadIndex, VectorIndex};
use crate::payload_storage::PayloadStorage;
use crate::segment::{SEGMENT_STATE_FILE, SNAPSHOT_FILES_PATH, SNAPSHOT_PATH, Segment};
use crate::types::SnapshotFormat;
use crate::utils::path::strip_prefix;
use crate::vector_storage::VectorStorage;

/// File name, used to store segment manifest inside snapshots
pub const SEGMENT_MANIFEST_FILE_NAME: &str = "segment_manifest.json";

impl SnapshotEntry for Segment {
    fn segment_id(&self) -> OperationResult<String> {
        let id = self
            .segment_path
            .file_stem()
            .and_then(|segment_dir| segment_dir.to_str())
            .ok_or_else(|| {
                OperationError::service_error(format!(
                    "failed to extract segment ID from segment path {}",
                    self.segment_path.display(),
                ))
            })?
            .to_string();

        debug_assert!(
            Uuid::try_parse(&id).is_ok(),
            "segment ID {id} is not a valid UUID",
        );

        Ok(id)
    }

    fn take_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<&SegmentManifest>,
    ) -> OperationResult<()> {
        let segment_id = self.segment_uuid();

        log::debug!("Taking snapshot of segment {segment_id}");

        let include_files_opt = match manifest {
            None => None,

            Some(manifest) => {
                let updated_manifest = self._get_segment_manifest()?;

                let updated_manifest_json =
                    serde_json::to_vec(&updated_manifest).map_err(|err| {
                        OperationError::service_error(format!(
                            "failed to serialize segment manifest into JSON: {err}"
                        ))
                    })?;

                let tar = tar.descend(Path::new(&segment_id.to_string()))?;
                tar.blocking_append_data(
                    &updated_manifest_json,
                    &Path::new("files").join(SEGMENT_MANIFEST_FILE_NAME),
                )?;

                Some(updated_files(manifest, &updated_manifest))
            }
        };

        let include_if = |path: &Path| {
            if let Some(include_files) = &include_files_opt {
                include_files.contains(path)
            } else {
                true
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
                let tar = tar.descend(Path::new(&segment_id.to_string()))?;
                snapshot_files(self, temp_path, &tar, include_if)?;
            }
        }

        Ok(())
    }

    fn get_segment_manifest(&self) -> OperationResult<SegmentManifest> {
        self._get_segment_manifest()
    }
}

impl Segment {
    fn _get_segment_manifest(&self) -> OperationResult<SegmentManifest> {
        let segment_id = self.segment_id()?;
        let segment_version = self.version();

        let files = self
            .files()
            .into_iter()
            .map(|file| (file, FileVersion::Unversioned));

        let vector_storage_files =
            self.vector_data
                .iter()
                .flat_map(|(vector_name, vector_data)| {
                    let version = self
                        .version_tracker
                        .get_vector(vector_name)
                        .or(self.version);

                    vector_data
                        .vector_storage
                        .borrow()
                        .files()
                        .into_iter()
                        .map(move |file| (file, FileVersion::from(version)))
                });

        let payload_storage_files = {
            let version = self.version_tracker.get_payload().or(self.version);

            self.payload_storage
                .borrow()
                .files()
                .into_iter()
                .map(move |file| (file, FileVersion::from(version)))
        };

        let immutable_files = self.immutable_files().into_iter().map(|file| {
            let version = self.initial_version.unwrap_or(0);
            (file, FileVersion::from(version))
        });

        let payload_index_files = self
            .payload_index
            .borrow()
            .immutable_files()
            .into_iter()
            .map(|(field, file)| {
                let version = self
                    .version_tracker
                    .get_payload_index_schema(&field)
                    .or(self.initial_version)
                    .unwrap_or(0);

                (file, FileVersion::from(version))
            });

        let mut file_versions = HashMap::with_capacity(files.len());

        let files = files
            .chain(vector_storage_files)
            .chain(payload_storage_files)
            .chain(immutable_files)
            .chain(payload_index_files);

        for (path, version) in files {
            // All segment files should be contained within segment directory
            debug_assert!(
                path.starts_with(&self.segment_path),
                "segment file {} is not contained within segment directory {}",
                path.display(),
                self.segment_path.display(),
            );

            let path = strip_prefix(&path, &self.segment_path)?;
            let _ = file_versions.insert(path.to_path_buf(), version);
        }

        Ok(SegmentManifest {
            segment_id,
            segment_version,
            file_versions,
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = Vec::new();

        files.extend(self.id_tracker.borrow().files());

        for vector_data in self.vector_data.values() {
            files.extend(vector_data.vector_index.borrow().files());
            files.extend(vector_data.vector_storage.borrow().files());

            if let Some(quantized_vectors) = vector_data.quantized_vectors.borrow().deref() {
                files.extend(quantized_vectors.files());
            }
        }

        files.extend(self.payload_index.borrow().files());
        files.extend(self.payload_storage.borrow().files());

        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = Vec::new();

        files.extend(self.id_tracker.borrow().immutable_files());

        for vector_data in self.vector_data.values() {
            files.extend(vector_data.vector_index.borrow().immutable_files());
            files.extend(vector_data.vector_storage.borrow().immutable_files());

            if let Some(quantized_vectors) = vector_data.quantized_vectors.borrow().deref() {
                files.extend(quantized_vectors.immutable_files());
            }
        }

        files.extend(
            self.payload_index
                .borrow()
                .immutable_files()
                .into_iter()
                .map(|(_, path)| path),
        );

        files.extend(self.payload_storage.borrow().immutable_files());

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
            let stripped_path = strip_prefix(&file, &segment.segment_path)?;

            if include_if(stripped_path) {
                tar.blocking_append_file(&file, stripped_path)
                    .map_err(|err| failed_to_add("vector index file", &file, err))?;
            }
        }

        for file in vector_data.vector_storage.borrow().files() {
            let stripped_path = strip_prefix(&file, &segment.segment_path)?;

            if include_if(stripped_path) {
                tar.blocking_append_file(&file, stripped_path)
                    .map_err(|err| failed_to_add("vector storage file", &file, err))?;
            }
        }

        if let Some(quantized_vectors) = vector_data.quantized_vectors.borrow().as_ref() {
            for file in quantized_vectors.files() {
                let stripped_path = strip_prefix(&file, &segment.segment_path)?;

                if include_if(stripped_path) {
                    tar.blocking_append_file(&file, stripped_path)
                        .map_err(|err| failed_to_add("quantized vectors file", &file, err))?;
                }
            }
        }
    }

    for file in segment.payload_index.borrow().files() {
        let stripped_path = strip_prefix(&file, &segment.segment_path)?;

        if include_if(stripped_path) {
            tar.blocking_append_file(&file, stripped_path)
                .map_err(|err| failed_to_add("payload index file", &file, err))?;
        }
    }

    for file in segment.payload_storage.borrow().files() {
        let stripped_path = strip_prefix(&file, &segment.segment_path)?;

        if include_if(stripped_path) {
            tar.blocking_append_file(&file, stripped_path)
                .map_err(|err| failed_to_add("payload storage file", &file, err))?;
        }
    }

    for file in segment.id_tracker.borrow().files() {
        let stripped_path = strip_prefix(&file, &segment.segment_path)?;

        if include_if(stripped_path) {
            tar.blocking_append_file(&file, stripped_path)
                .map_err(|err| failed_to_add("id tracker file", &file, err))?;
        }
    }

    let segment_state_path = segment.segment_path.join(SEGMENT_STATE_FILE);
    tar.blocking_append_file(&segment_state_path, Path::new(SEGMENT_STATE_FILE))
        .map_err(|err| failed_to_add("segment state file", &segment_state_path, err))?;

    let version_file_path = segment.segment_path.join(VERSION_FILE);
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

    for (path, current_version) in current.file_versions() {
        // Include file into partial snapshot if:
        //
        // 1. `old` manifest does not contain this file
        let Some(old_version) = old.file_version(path) else {
            updated.insert(path.to_path_buf());
            continue;
        };

        // 2. if `old` manifest contains this file and file/segment in `current` manifest is *newer*:
        //    - if file is `Unversioned` in both manifests, compare segment versions
        //    - if file is versioned in *one* of the manifests only, compare *file* version against
        //      other *segment* version
        //    - if file is versioned in both manifests, compare file versions
        if old_version < current_version {
            updated.insert(path.to_path_buf());
            continue;
        }

        // 3. if `old` manifest contains this file and file/segment versions in both `old` and `current` manifests are 0
        //    - we can't distinguish between new empty (no operations applied yet) segment (version 0)
        //    - and segment with operation 0 applied (also version 0)
        //    - so if both files/segments are at version 0, we always include the file into snapshot
        if old_version == 0 && current_version == 0 {
            updated.insert(path.to_path_buf());
        }
    }

    updated
}
