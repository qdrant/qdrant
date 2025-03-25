use std::collections::HashSet;
use std::path::Path;

use common::tar_ext;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::segment_manifest::SegmentManifests;
use crate::entry::partial_snapshot_entry::PartialSnapshotEntry;
use crate::segment::Segment;
use crate::segment::snapshot::{snapshot_files, updated_files};

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
}
