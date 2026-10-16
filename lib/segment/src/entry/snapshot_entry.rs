use std::path::{Path, PathBuf};

use common::tar_ext;

use crate::common::operation_error::OperationResult;
use crate::data_types::manifest::SegmentManifest;
use crate::types::SnapshotFormat;

pub trait SnapshotEntry {
    /// Segment identifier in the snapshot.
    fn segment_id(&self) -> OperationResult<String>;

    /// Take a snapshot of the segment.
    ///
    /// Packs the segment files and [`Self::visible_pending_changes_log_files`] into `tar`, not
    /// every file in the segment directory. Uses `temp_path` to prepare files to archive.
    fn take_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<&SegmentManifest>,
    ) -> OperationResult<()> {
        self.take_snapshot_with_pending_changes_logs(
            temp_path,
            tar,
            format,
            manifest,
            &self.visible_pending_changes_log_files(),
        )
    }

    /// Take a snapshot of the segment, packing `pending_changes_logs` as its pending changes logs.
    ///
    /// Lets a proxy segment add its own log to the snapshot of the segment it wraps.
    fn take_snapshot_with_pending_changes_logs(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        format: SnapshotFormat,
        manifest: Option<&SegmentManifest>,
        pending_changes_logs: &[PathBuf],
    ) -> OperationResult<()>;

    fn get_segment_manifest(&self) -> OperationResult<SegmentManifest>;

    /// Pending changes log files a snapshot of this segment must pack: the logs the underlying
    /// segment found on load, plus the log of each live proxy layer from this one down.
    ///
    /// Logs of proxies wrapping this segment are never included: a snapshot proxy's log only
    /// holds changes made after the freeze. Neither are logs of unwrapped proxies awaiting
    /// removal: their changes are in the wrapped segment, which is flushed before snapshotting.
    fn visible_pending_changes_log_files(&self) -> Vec<PathBuf>;
}
