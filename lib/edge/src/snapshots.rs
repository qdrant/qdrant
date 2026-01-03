use crate::Shard;
use segment::common::operation_error::OperationResult;
use shard::segment_holder::SegmentHolder;
use std::path::Path;

impl Shard {
    pub fn unpack_snapshot(snapshot_path: &Path, target_path: &Path) -> OperationResult<()> {
        SegmentHolder::unpack_snapshot(snapshot_path, target_path)
    }
}
