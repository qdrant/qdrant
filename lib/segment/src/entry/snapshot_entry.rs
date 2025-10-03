use std::path::Path;

use common::tar_ext;

use crate::common::operation_error::OperationResult;
use crate::data_types::manifest::SnapshotManifest;
use crate::types::SnapshotFormat;

pub trait SnapshotEntry {
    /// Take a snapshot of the segment.
    ///
    /// Creates a tar archive of the segment directory into `snapshot_dir_path`.
    /// Uses `temp_path` to prepare files to archive.
    fn take_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<&SnapshotManifest>,
    ) -> OperationResult<()>;

    fn collect_snapshot_manifest(&self, manifest: &mut SnapshotManifest) -> OperationResult<()>;
}
