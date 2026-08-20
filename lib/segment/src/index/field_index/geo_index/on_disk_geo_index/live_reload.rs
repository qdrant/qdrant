use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};

use super::OnDiskGeoIndex;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;

impl<S: UniversalRead> LiveReload for OnDiskGeoIndex<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(&self, _fs: &Fs) -> OperationResult<()> {
        Ok(())
    }

    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        _fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        _new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // No on-disk state changes on reload: this index is immutable, so only
        // the in-memory deletion bitvec is patched. `fs` / `new_points` are
        // unused because nothing is appended after build.
        for deleted_point in deleted_points {
            self.remove_point(*deleted_point);
        }

        Ok(())
    }
}
