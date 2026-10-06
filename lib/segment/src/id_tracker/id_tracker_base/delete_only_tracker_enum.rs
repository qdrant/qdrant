use common::types::PointOffsetType;
use common::universal_io::{UniversalAppendFs, UniversalWriteFs};

use crate::common::operation_error::OperationResult;
use crate::id_tracker::disk_id_tracker::update_only::UpdateOnlyDiskIdTracker;
use crate::id_tracker::immutable_id_tracker::update_only::UpdateOnlyImmutableIdTracker;
use crate::id_tracker::point_moves::Retirement;
use crate::types::PointIdType;

/// The update-only tracker of whichever immutable id-tracker format a segment
/// holds. Each variant decides where its tombstones go.
pub enum DeleteOnlyIdTrackerEnum {
    Immutable(UpdateOnlyImmutableIdTracker),
    DiskResident(UpdateOnlyDiskIdTracker),
}

impl DeleteOnlyIdTrackerEnum {
    /// Retire the given points by marking the slots they occupy in the stored
    /// deleted mask — the only thing written, the data on those slots stays.
    pub fn tombstone_points<Fs>(
        &mut self,
        fs: &Fs,
        points: &[(PointIdType, PointOffsetType)],
    ) -> OperationResult<()>
    where
        Fs: UniversalWriteFs,
    {
        match self {
            Self::Immutable(id_tracker) => id_tracker.tombstone_points(fs, points),
            Self::DiskResident(id_tracker) => id_tracker.tombstone_points(fs, points),
        }
    }

    /// Retire the points of `retirements`: the moved-out records of the moves among them first,
    /// then the tombstones in the stored deleted mask.
    pub fn retire_points<Fs>(&mut self, fs: &Fs, retirements: &[Retirement]) -> OperationResult<()>
    where
        Fs: UniversalAppendFs,
    {
        match self {
            Self::Immutable(id_tracker) => id_tracker.retire_points(fs, retirements),
            Self::DiskResident(id_tracker) => id_tracker.retire_points(fs, retirements),
        }
    }
}
