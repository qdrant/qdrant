//! The write phase for an immutable segment: retiring points, and nothing
//! else.

use std::path::Path;

use common::types::PointOffsetType;
use common::universal_io::{UniversalRead, UniversalWriteFileOps};

use super::DeleteOnlyIdTrackerState;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::delete_only_tracker_enum::DeleteOnlyIdTrackerEnum;
use crate::id_tracker::disk_id_tracker::update_only::UpdateOnlyDiskIdTracker;
use crate::id_tracker::immutable_id_tracker::update_only::UpdateOnlyImmutableIdTracker;
use crate::types::PointIdType;

/// A segment open for deletes: nothing in it can grow, so the only thing a
/// batch can do here is retire points that are already there. Where their
/// tombstones go is the id tracker's decision.
pub struct DeleteOnlySegment<S: UniversalRead<Fs: UniversalWriteFileOps> + 'static> {
    id_tracker: DeleteOnlyIdTrackerEnum<S>,
}

impl<S: UniversalRead<Fs: UniversalWriteFileOps> + 'static> DeleteOnlySegment<S> {
    /// Open the segment directory at `segment_path` for deletes, resuming the
    /// tracker kind its [`DeleteOnlyIdTrackerState`] variant names; nothing is
    /// read.
    pub fn open(
        fs: S::Fs,
        segment_path: &Path,
        id_tracker_state: DeleteOnlyIdTrackerState,
    ) -> Self {
        let id_tracker = match id_tracker_state {
            DeleteOnlyIdTrackerState::Immutable(deleted) => DeleteOnlyIdTrackerEnum::Immutable(
                UpdateOnlyImmutableIdTracker::new(fs, segment_path, deleted),
            ),
            DeleteOnlyIdTrackerState::DiskResident(deleted) => {
                DeleteOnlyIdTrackerEnum::DiskResident(UpdateOnlyDiskIdTracker::new(
                    fs,
                    segment_path,
                    deleted,
                ))
            }
        };
        Self { id_tracker }
    }

    /// Retire the given points by marking the slots they occupy in the id
    /// tracker's stored deleted mask — the only thing written, the data on
    /// those slots stays.
    pub fn tombstone_points(
        &mut self,
        points: &[(PointIdType, PointOffsetType)],
    ) -> OperationResult<()> {
        self.id_tracker.tombstone_points(points)
    }
}
