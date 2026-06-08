use common::counter::hardware_counter::HardwareCounterCell;
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
        deleted_points: &[PointOffsetType],
        new_points: &[PointOffsetType],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // Reload each flag set's bitmap from the changed points only.
        self.storage
            .has_values_flags
            .live_reload(fs, deleted_points, new_points, hw_counter)?;
        self.storage
            .is_null_flags
            .live_reload(fs, deleted_points, new_points, hw_counter)?;

        // total_point_count only grows, to cover appended offsets.
        self.total_point_count = new_points
            .iter()
            .fold(self.total_point_count, |max, &id| max.max(id as usize + 1));

        Ok(())
    }
}
