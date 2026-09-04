use common::types::PointOffsetType;
use common::universal_io::UniversalAppendFs;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::disk_id_tracker::update_only::UpdateOnlyDiskIdTracker;
use crate::id_tracker::immutable_id_tracker::update_only::UpdateOnlyImmutableIdTracker;
use crate::types::PointIdType;

/// The update-only tracker of whichever immutable id-tracker format a segment
/// holds. Each variant decides where its tombstones go.
pub enum DeleteOnlyIdTrackerEnum<Fs: UniversalAppendFs> {
    Immutable(UpdateOnlyImmutableIdTracker<Fs>),
    DiskResident(UpdateOnlyDiskIdTracker<Fs>),
}

impl<Fs: UniversalAppendFs> DeleteOnlyIdTrackerEnum<Fs> {
    /// Retire the given points by marking the slots they occupy in the stored
    /// deleted mask — the only thing written, the data on those slots stays.
    pub fn tombstone_points(
        &mut self,
        points: &[(PointIdType, PointOffsetType)],
    ) -> OperationResult<()> {
        match self {
            Self::Immutable(id_tracker) => id_tracker.tombstone_points(points),
            Self::DiskResident(id_tracker) => id_tracker.tombstone_points(points),
        }
    }
}
