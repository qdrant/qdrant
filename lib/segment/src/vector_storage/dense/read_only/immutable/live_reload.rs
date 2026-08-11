use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};

use super::ReadOnlyImmutableDenseVectorStorage;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;

impl<T: PrimitiveVectorElement, S: UniversalRead> LiveReload
    for ReadOnlyImmutableDenseVectorStorage<T, S>
{
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(&self, _fs: &Fs) -> OperationResult<()> {
        Ok(())
    }

    /// Vector data is immutable, so only the in-memory deletion flags are patched
    /// from the authoritative `deleted_points`; `fs` and `new_points` are unused.
    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        _fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        _new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.deleted.insert_all(deleted_points);

        Ok(())
    }
}
