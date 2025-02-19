use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use common::tar_ext;
use uuid::Uuid;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::segment_manifest::{FileVersion, SegmentManifest, SegmentManifests};
use crate::entry::entry_point::SegmentEntry;
use crate::entry::partial_snapshot_entry::PartialSnapshotEntry;
use crate::index::{PayloadIndex, VectorIndex};
use crate::payload_storage::PayloadStorage;
use crate::segment::snapshot::{
    snapshot_files, PAYLOAD_INDEX_ROCKS_DB_VIRT_FILE, ROCKS_DB_VIRT_FILE,
};
use crate::segment::Segment;
use crate::utils::path::strip_prefix;
use crate::vector_storage::VectorStorage;

impl PartialSnapshotEntry for Segment {
    fn take_partial_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        manifests: &SegmentManifests,
        snapshotted_segments: &mut HashSet<String>,
    ) -> OperationResult<()> {
        let segment_id = self.segment_id()?;

        if !snapshotted_segments.insert(segment_id.into()) {
            // Already snapshotted
            return Ok(());
        }

        let updated_manifest = self.get_segment_manifest()?;

        let updated_manifest_json = serde_json::to_vec(&updated_manifest).map_err(|err| {
            OperationError::service_error(format!(
                "failed to serialize segment manifest into JSON: {err}"
            ))
        })?;

        let tar = tar.descend(Path::new(&segment_id))?;
        tar.blocking_append_data(&updated_manifest_json, Path::new("segment_manifest.json"))?;

        match manifests.get(segment_id) {
            Some(manifest) => {
                let updated_files = updated_files(manifest, &updated_manifest);
                snapshot_files(self, temp_path, &tar, |path| updated_files.contains(path))?;
            }

            None => {
                snapshot_files(self, temp_path, &tar, |_| true)?;
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
