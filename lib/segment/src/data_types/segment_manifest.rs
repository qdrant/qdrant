use std::collections::HashMap;
use std::path::PathBuf;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::SeqNumberType;

#[derive(Clone, Debug, Default)]
pub struct SegmentManifests {
    manifests: HashMap<String, SegmentManifest>,
}

impl SegmentManifests {
    pub fn validate(&self) -> OperationResult<()> {
        for (segment_id, manifest) in &self.manifests {
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
        self.manifests
            .get(segment_id)
            .map(|manifest| manifest.segment_version)
    }

    pub fn add(&mut self, new_manifest: SegmentManifest) -> bool {
        let Some(current_manifest) = self.manifests.get_mut(&new_manifest.segment_id) else {
            self.manifests
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
        self.manifests.get(segment_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &SegmentManifest)> {
        self.manifests.iter()
    }

    pub fn len(&self) -> usize {
        self.manifests.len()
    }

    pub fn is_empty(&self) -> bool {
        self.manifests.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SegmentManifest {
    pub segment_id: String,
    pub segment_version: SeqNumberType,
    pub file_versions: HashMap<PathBuf, FileVersion>,
}

impl SegmentManifest {
    pub fn empty(segment_id: impl Into<String>) -> Self {
        Self {
            segment_id: segment_id.into(),
            segment_version: 0,
            file_versions: HashMap::new(),
        }
    }
}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
)]
#[serde(untagged)]
pub enum FileVersion {
    Version(SeqNumberType),
    Unversioned,
}

impl FileVersion {
    pub fn is_unversioned(self) -> bool {
        self == Self::Unversioned
    }

    pub fn or_segment_version(self, segment_version: SeqNumberType) -> SeqNumberType {
        match self {
            FileVersion::Version(version) => version,
            FileVersion::Unversioned => segment_version,
        }
    }
}

impl From<SeqNumberType> for FileVersion {
    fn from(version: SeqNumberType) -> Self {
        Self::Version(version)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Tests that `FileVersion` variants are uniquely represented in JSON
    #[test]
    fn file_version_serde() {
        test_file_version_serde(FileVersion::Version(42), "42");
        test_file_version_serde(FileVersion::Unversioned, "null");
    }

    /// Tests that `FileVersion` serializes into/deserializes from provided JSON representation
    fn test_file_version_serde(version: FileVersion, json: &str) {
        let serialized =
            serde_json::to_string(&version).expect("failed to serialize FileVersion to JSON");

        assert_eq!(serialized, json);

        let deserialized: FileVersion =
            serde_json::from_str(json).expect("failed to deserialize FileVersion from JSON");

        assert_eq!(deserialized, version);
    }
}
