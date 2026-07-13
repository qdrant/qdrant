use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::ReadOnlyNullIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;

impl<S: UniversalRead> LiveReload for ReadOnlyNullIndex<S> {
    type Fs = S::Fs;

    fn live_reload(
        &mut self,
        fs: &S::Fs,
        _deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // Resync each flag set from its on-disk state; the point deltas are
        // irrelevant, the flag files are the source of truth.
        self.storage.has_values_flags.live_reload(fs)?;
        self.storage.is_null_flags.live_reload(fs)?;

        // total_point_count only grows, to cover appended offsets.
        self.total_point_count = new_points
            .last()
            .map(|&id| id as usize + 1)
            .unwrap_or(self.total_point_count);

        Ok(())
    }
}
