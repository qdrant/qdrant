use std::collections::HashMap;
use std::path::Path;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::manifest::SegmentManifest;
use segment::types::SeqNumberType;

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
    pub fn load_from_snapshot(snapshot_path: &Path) -> OperationResult<Self> {
        todo!()
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