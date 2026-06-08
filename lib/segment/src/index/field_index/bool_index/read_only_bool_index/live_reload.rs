use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::ReadOnlyBoolIndex;
use crate::common::flags::roaring_flags::RoaringFlagsRead;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;

impl<S: UniversalRead> LiveReload for ReadOnlyBoolIndex<S> {
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

        // Refresh the derived counts from the reloaded bitmaps, as `open` does.
        //
        // possible opt: update this using deleted_points and new_points separately,
        //               so we only process the delta
        self.indexed_count =
            self.storage
                .trues_flags
                .get_bitmap()
                .union_len(self.storage.falses_flags.get_bitmap()) as usize;

        // possible opt: track num_trues within `RoaringFlags`.
        self.trues_count = self.storage.trues_flags.count_trues();
        self.falses_count = self.storage.falses_flags.count_trues();

        Ok(())
    }
}
