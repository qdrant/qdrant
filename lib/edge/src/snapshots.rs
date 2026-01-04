use std::path::Path;

use segment::common::operation_error::OperationResult;
use shard::snapshots::snapshot_manifest::SnapshotManifest;
use shard::snapshots::snapshot_utils::SnapshotUtils;

use crate::Shard;

impl Shard {
    pub fn unpack_snapshot(snapshot_path: &Path, target_path: &Path) -> OperationResult<()> {
        SnapshotUtils::unpack_snapshot(snapshot_path, target_path)
    }

    pub fn snapshot_manifest(&self) -> OperationResult<SnapshotManifest> {
        self.segments.read().snapshot_manifest()
    }
}
