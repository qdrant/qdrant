use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::OnDiskFullTextIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;

impl<S: UniversalRead> LiveReload for OnDiskFullTextIndex<S> {
    type Fs = S::Fs;

    fn live_reload(
        &mut self,
        _fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        _new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // Immutable on-disk state: only the in-memory deletion bitmap is
        // patched (mirrors the other immutable leaves). `fs` / `new_points`
        // are unused because nothing is appended after build.
        for deleted_point in deleted_points {
            self.remove_point(*deleted_point);
        }

        Ok(())
    }
}
