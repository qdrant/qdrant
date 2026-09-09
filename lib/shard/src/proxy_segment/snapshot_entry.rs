use std::path::Path;

use common::tar_ext;
use segment::common::operation_error::OperationResult;
use segment::data_types::manifest::{FileVersion, SegmentManifest};
use segment::entry::StorageSegmentEntry;
use segment::entry::snapshot_entry::SnapshotEntry;
use segment::pending_changes::pending_changes_log_path;
use segment::types::*;

use super::ProxySegment;

impl SnapshotEntry for ProxySegment {
    fn segment_id(&self) -> OperationResult<String> {
        self.wrapped_segment.get().read().segment_id()
    }

    fn take_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<&SegmentManifest>,
    ) -> OperationResult<()> {
        log::info!("Taking a snapshot of a proxy segment");

        // Snapshot wrapped segment data into the temporary dir
        self.wrapped_segment
            .get()
            .read()
            .take_snapshot(temp_path, tar, format, manifest)?;

        Ok(())
    }

    fn get_segment_manifest(&self) -> OperationResult<SegmentManifest> {
        let mut manifest = self.wrapped_segment.get().read().get_segment_manifest()?;

        // Add persisted pending changes log file
        manifest.segment_version = self.version();
        manifest.file_versions.insert(
            pending_changes_log_path(Path::new(""), self.pending_changes.level()),
            FileVersion::Version(manifest.segment_version),
        );

        Ok(manifest)
    }
}
