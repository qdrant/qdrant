use std::path::Path;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::PointOffsetType;
use common::universal_io::MmapFs;

use super::sp;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::disk_id_tracker::DiskIdTracker;
use crate::id_tracker::immutable_id_tracker::ImmutableIdTracker;
use crate::id_tracker::mutable_id_tracker::MutableIdTracker;
use crate::id_tracker::{IdTrackerEnum, IdTrackerFormat};

pub(crate) fn create_mutable_id_tracker(
    segment_path: &Path,
    deferred_internal_id: Option<PointOffsetType>,
) -> OperationResult<MutableIdTracker> {
    MutableIdTracker::open(segment_path, deferred_internal_id)
}

pub(crate) fn create_segment_id_tracker(
    format: IdTrackerFormat,
    segment_path: &Path,
    deferred_internal_id: Option<PointOffsetType>,
) -> OperationResult<Arc<AtomicRefCell<IdTrackerEnum>>> {
    let id_tracker = match format {
        IdTrackerFormat::Mutable => {
            IdTrackerEnum::MutableIdTracker(create_mutable_id_tracker(
                segment_path,
                deferred_internal_id,
            )?)
        }
        IdTrackerFormat::Immutable => {
            IdTrackerEnum::ImmutableIdTracker(ImmutableIdTracker::open(&MmapFs, segment_path)?)
        }
        IdTrackerFormat::Disk => {
            IdTrackerEnum::DiskIdTracker(DiskIdTracker::open(&MmapFs, segment_path)?)
        }
    };
    Ok(sp(id_tracker))
}
