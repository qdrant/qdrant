use std::path::Path;

use common::tar_ext;
use segment::common::operation_error::OperationResult;
use segment::data_types::manifest::SegmentManifest;
use segment::entry::snapshot_entry::SnapshotEntry;
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
        self.wrapped_segment.get().read().get_segment_manifest()
    }
}
