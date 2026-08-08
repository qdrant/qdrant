//! The write phase for the appendable segment: the one segment of a shard a
//! batch appends its points to.

use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalAppend;

use super::AppendableIdTrackerState;
use crate::common::operation_error::OperationResult;
use crate::data_types::fully_qualified_point::FullyQualifiedPoint;
use crate::id_tracker::mutable_id_tracker::update_only::UpdateOnlyAppendableIdTracker;
use crate::types::PointIdType;

/// A segment open for appends: the write target. Every point a batch stores
/// lands here, in a fresh slot — nothing is ever rewritten in place.
pub struct AppendableSegment<S: UniversalAppend + 'static> {
    id_tracker: UpdateOnlyAppendableIdTracker<S>,
}

impl<S: UniversalAppend + 'static> AppendableSegment<S> {
    /// Resume the segment directory at `segment_path` from the mappings-log
    /// state the read phase observed.
    ///
    /// Opening is not free of side effects: points left on slots whose
    /// versions were never committed are retired here, since which components
    /// got to write their data is unknowable.
    pub fn open(
        fs: S::Fs,
        segment_path: &Path,
        state: AppendableIdTrackerState,
    ) -> OperationResult<Self> {
        let AppendableIdTrackerState {
            max_claimed_internal_id,
            pending_inserts,
            mappings_end,
        } = state;

        let id_tracker = UpdateOnlyAppendableIdTracker::new(
            fs,
            segment_path,
            max_claimed_internal_id,
            pending_inserts,
            mappings_end,
        )?;

        Ok(Self { id_tracker })
    }

    /// Append `points` to this segment, each into a fresh slot, and repoint
    /// the id tracker at those slots. A point that already exists here is
    /// never rewritten in place: it is written anew, and the mappings log
    /// retires its previous slot on its own.
    pub fn store_points(
        &mut self,
        points: &[FullyQualifiedPoint],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let _ = (points, hw_counter);
        // The id tracker half is ready (`insert_operations` +
        // `set_internal_versions`); what is missing is everywhere the point's
        // data goes.
        //
        // Keep the ids `insert_operations` returns when writing this: with
        // them, `tombstone_points` can refuse to retire a point this batch
        // stored, and the caller stops having to know not to ask.
        todo!("needs the append-only vector storages, payload storage and field indexes")
    }

    /// Retire the given points, addressed by their external ids — the slots
    /// they occupy play no part here, since a retired mapping is what makes a
    /// point unreachable. The data on those slots is left where it is.
    ///
    /// Only call this for points the batch *deletes*. A point the batch stores
    /// again needs no retirement: its new mapping supersedes the old slot, and
    /// a delete recorded afterwards would retire the new slot along with it.
    pub fn tombstone_points(
        &mut self,
        points: &[(PointIdType, PointOffsetType)],
    ) -> OperationResult<()> {
        self.id_tracker
            .delete_points(points.iter().map(|(point_id, _internal_id)| *point_id))
    }
}
