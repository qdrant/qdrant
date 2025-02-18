use std::collections::HashMap;
use std::path::PathBuf;

use crate::types::SeqNumberType;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SegmentManifest {
    pub segment_version: SeqNumberType,
    pub file_versions: HashMap<PathBuf, FileVersion>,
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
