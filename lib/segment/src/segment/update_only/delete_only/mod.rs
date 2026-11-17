//! The write phase for an immutable segment: retiring points, and nothing
//! else.

use std::path::Path;

use common::types::PointOffsetType;
use common::universal_io::UniversalAppendFs;

use super::DeleteOnlyIdTrackerState;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::delete_only_tracker_enum::DeleteOnlyIdTrackerEnum;
use crate::id_tracker::delete_versions::DeleteVersioning;
use crate::id_tracker::disk_id_tracker::update_only::UpdateOnlyDiskIdTracker;
use crate::id_tracker::immutable_id_tracker::update_only::UpdateOnlyImmutableIdTracker;
use crate::types::{PointIdType, SeqNumberType};

/// A segment open for deletes: nothing in it can grow, so the only thing a
/// batch can do here is retire points that are already there. Where their
/// tombstones go is the id tracker's decision.
pub struct DeleteOnlySegment<Fs: UniversalAppendFs> {
    fs: Fs,
    id_tracker: DeleteOnlyIdTrackerEnum,
}

impl<Fs: UniversalAppendFs> DeleteOnlySegment<Fs> {
    /// Open the segment directory at `segment_path` for deletes, resuming the
    /// tracker kind its [`DeleteOnlyIdTrackerState`] variant names; nothing is
    /// read.
    pub fn open(
        fs: Fs,
        segment_path: &Path,
        id_tracker_state: DeleteOnlyIdTrackerState,
    ) -> OperationResult<Self> {
        let id_tracker = match id_tracker_state {
            DeleteOnlyIdTrackerState::Immutable(deleted) => DeleteOnlyIdTrackerEnum::Immutable(
                UpdateOnlyImmutableIdTracker::new(segment_path, deleted)?,
            ),
            DeleteOnlyIdTrackerState::DiskResident(deleted) => {
                DeleteOnlyIdTrackerEnum::DiskResident(UpdateOnlyDiskIdTracker::new(
                    segment_path,
                    deleted,
                )?)
            }
        };

        Ok(Self { fs, id_tracker })
    }

    /// Retire the given points — the data on their slots stays — in the delete
    /// log with `version`, or in the stored deleted mask without delete
    /// versions, see [`delete_versions`](crate::id_tracker::delete_versions).
    pub fn tombstone_points(
        &mut self,
        points: &[(PointIdType, PointOffsetType)],
        version: SeqNumberType,
        versioning: DeleteVersioning,
    ) -> OperationResult<()> {
        self.id_tracker
            .tombstone_points(&self.fs, points, version, versioning)
    }
}
