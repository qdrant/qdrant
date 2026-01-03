use std::path::Path;

use segment::common::operation_error::OperationResult;
use shard::segment_holder::SegmentHolder;

use crate::Shard;

impl Shard {
    pub fn unpack_snapshot(snapshot_path: &Path, target_path: &Path) -> OperationResult<()> {
        SegmentHolder::unpack_snapshot(snapshot_path, target_path)
    }
}
