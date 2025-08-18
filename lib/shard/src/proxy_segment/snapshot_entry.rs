use std::collections::HashSet;
use std::path::Path;

use common::tar_ext;
use segment::common::operation_error::OperationResult;
use segment::data_types::manifest::SnapshotManifest;
use segment::entry::snapshot_entry::SnapshotEntry;
use segment::types::*;

use super::ProxySegment;

impl SnapshotEntry for ProxySegment {
    fn take_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<&SnapshotManifest>,
        snapshotted_segments: &mut HashSet<String>,
    ) -> OperationResult<()> {
        log::info!("Taking a snapshot of a proxy segment");

        // Snapshot wrapped segment data into the temporary dir
        self.wrapped_segment.get().read().take_snapshot(
            temp_path,
            tar,
            format,
            manifest,
            snapshotted_segments,
        )?;

        // Snapshot write_segment
        self.write_segment.get().read().take_snapshot(
            temp_path,
            tar,
            format,
            manifest,
            snapshotted_segments,
        )?;

        Ok(())
    }

    fn collect_snapshot_manifest(&self, manifest: &mut SnapshotManifest) -> OperationResult<()> {
        self.wrapped_segment
            .get()
            .read()
            .collect_snapshot_manifest(manifest)?;

        self.write_segment
            .get()
            .read()
            .collect_snapshot_manifest(manifest)?;

        Ok(())
    }
}
