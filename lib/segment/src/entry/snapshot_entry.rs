use std::path::Path;

use common::tar_ext;

use crate::common::operation_error::OperationResult;
use crate::data_types::manifest::SegmentManifest;
use crate::types::SnapshotFormat;

pub trait SnapshotEntry {
    /// Segment identifier in the snapshot.
    fn segment_id(&self) -> OperationResult<String>;

    /// Take a snapshot of the segment.
    ///
    /// Creates a tar archive of the segment directory into `snapshot_dir_path`.
    /// Uses `temp_path` to prepare files to archive.
    fn take_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<&SegmentManifest>,
    ) -> OperationResult<()>;

    fn get_segment_manifest(&self) -> OperationResult<SegmentManifest>;
}
