use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;

use super::ReadOnlyBoolIndex;
use crate::common::operation_error::OperationResult;
use crate::index::UniversalReadExt;
use crate::index::field_index::LiveReload;

impl<S: UniversalReadExt> LiveReload for ReadOnlyBoolIndex<S> {
    type Fs = S::Fs;

    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // Reload each flag set's bitmap from the changed points only.
        self.storage
            .trues_flags
            .live_reload(fs, deleted_points, new_points, hw_counter)?;
        self.storage
            .falses_flags
            .live_reload(fs, deleted_points, new_points, hw_counter)?;

        // Re-derive the counts from the just-reloaded bitmaps, but only if they
        // were computed before: an untouched index must not pay for the bitmap
        // scan that deriving them would force.
        //
        // possible opt: update this using deleted_points and new_points separately,
        //               so we only process the delta
        self.refresh_counts()?;

        Ok(())
    }
}
