use std::collections::{HashMap, HashSet};
use std::path::Path;

use fs_err as fs;
use fs_err::File;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::manifest::SegmentManifest;
use segment::segment::snapshot::SEGMENT_MANIFEST_FILE_NAME;
use segment::types::SeqNumberType;

use crate::files::segments_path;

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct SnapshotManifest {
    segments: HashMap<String, SegmentManifest>,
}

impl SnapshotManifest {
    /// Creates a `SnapshotManifest` by looking at the files of the unpacked snapshot
    ///
    /// # Arguments
    /// * `snapshot_path` - Path to the unpacked snapshot directory
    pub fn load_from_snapshot(
        snapshot_path: &Path,
        recovery_type: Option<RecoveryType>,
    ) -> OperationResult<Self> {
        let segments_path = segments_path(snapshot_path);

        let mut snapshot_segments = HashSet::new();
        let mut snapshot_manifest = SnapshotManifest::default();

        for segment_entry in fs::read_dir(segments_path)? {
            let segment_path = segment_entry?.path();

            if !segment_path.is_dir() {
                log::warn!(
                    "segment path {} in extracted snapshot {} is not a directory",
                    segment_path.display(),
                    snapshot_path.display(),
                );

                continue;
            }

            let segment_id = segment_path
                .file_name()
                .and_then(|segment_id| segment_id.to_str())
                .expect("segment path ends with a valid segment id");

            let added = snapshot_segments.insert(segment_id.to_string());
            debug_assert!(added);

            let manifest_path = segment_path.join(SEGMENT_MANIFEST_FILE_NAME);

            let manifest_exists = manifest_path.exists();

            match recovery_type {
                None => {
                    if !manifest_exists {
                        continue; // Assume it should not exist
                    }
                }
                Some(RecoveryType::Full) => {
                    if manifest_exists {
                        return Err(OperationError::validation_error(format!(
                            "invalid shard snapshot: \
                         segment {segment_id} contains segment manifest; \
                         ensure you are not recovering partial snapshot on shard snapshot endpoint",
                        )));
                    } else {
                        continue;
                    }
                }
                Some(RecoveryType::Partial) => {
                    if !manifest_exists {
                        return Err(OperationError::validation_error(format!(
                            "invalid partial snapshot: \
                         segment {segment_id} does not contain segment manifest; \
                         ensure you are not recovering shard snapshot on partial snapshot endpoint",
                        )));
                    }
                }
            }

            let manifest = File::open(&manifest_path).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open segment {segment_id} manifest: {err}",
                ))
            })?;

            let manifest = std::io::BufReader::new(manifest);

            let manifest: SegmentManifest = serde_json::from_reader(manifest).map_err(|err| {
                OperationError::validation_error(format!(
                    "failed to deserialize segment {segment_id} manifest: {err}",
                ))
            })?;

            if segment_id != manifest.segment_id {
                return Err(OperationError::validation_error(format!(
                    "invalid partial snapshot: \
                     segment {segment_id} contains segment manifest with segment ID {}",
                    manifest.segment_id,
                )));
            }

            let added = snapshot_manifest.add(manifest);
            debug_assert!(added);
        }

        snapshot_manifest.validate().map_err(|err| {
            OperationError::validation_error(format!("invalid partial snapshot: {err}"))
        })?;

        Ok(snapshot_manifest)
    }

    pub fn validate(&self) -> OperationResult<()> {
        for (segment_id, manifest) in &self.segments {
            for (file, version) in &manifest.file_versions {
                if version.or_segment_version(manifest.segment_version) > manifest.segment_version {
                    return Err(OperationError::validation_error(format!(
                        "invalid snapshot manifest: \
                         file {segment_id}/{} is newer than segment {segment_id} ({version:?} > {})",
                        file.display(),
                        manifest.segment_version,
                    )));
                }
            }
        }

        Ok(())
    }

    pub fn version(&self, segment_id: &str) -> Option<SeqNumberType> {
        self.segments
            .get(segment_id)
            .map(|manifest| manifest.segment_version)
    }

    pub fn add(&mut self, new_manifest: SegmentManifest) -> bool {
        let Some(current_manifest) = self.segments.get_mut(&new_manifest.segment_id) else {
            self.segments
                .insert(new_manifest.segment_id.clone(), new_manifest);

            return true;
        };

        debug_assert_eq!(current_manifest.segment_id, new_manifest.segment_id);

        if current_manifest.segment_version < new_manifest.segment_version {
            *current_manifest = new_manifest;
            return true;
        }

        false
    }

    pub fn get(&self, segment_id: &str) -> Option<&SegmentManifest> {
        self.segments.get(segment_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &SegmentManifest)> {
        self.segments.iter()
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RecoveryType {
    Full,
    Partial,
}

impl RecoveryType {
    pub fn is_full(self) -> bool {
        matches!(self, Self::Full)
    }

    pub fn is_partial(self) -> bool {
        matches!(self, Self::Partial)
    }
}
