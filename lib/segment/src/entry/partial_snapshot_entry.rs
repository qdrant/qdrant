use std::path::Path;

use common::tar_ext;

use crate::common::operation_error::OperationResult;
use crate::data_types::segment_manifest::SegmentManifest;

pub trait PartialSnapshotEntry {
    fn take_partial_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        manifest: &SegmentManifest,
    ) -> OperationResult<()>;

    fn get_segment_manifest(&self) -> OperationResult<SegmentManifest>;
}
