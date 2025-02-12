use std::collections::HashSet;
use std::path::Path;

use common::tar_ext;

use crate::common::operation_error::OperationResult;
use crate::data_types::segment_manifest::SegmentManifests;

pub trait PartialSnapshotEntry {
    fn take_partial_snapshot(
        &self,
        temp_path: &Path,
        tar: &tar_ext::BuilderExt,
        manifest: &SegmentManifests,
        snapshotted_segments: &mut HashSet<String>,
    ) -> OperationResult<()>;

    fn collect_segment_manifests(&self, manifests: &mut SegmentManifests) -> OperationResult<()>;
}
