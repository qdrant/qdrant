use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::ReadOnlyChunkedMultiDenseVectorStorage;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;

impl<T: PrimitiveVectorElement, S: UniversalRead> LiveReload
    for ReadOnlyChunkedMultiDenseVectorStorage<T, S>
{
    type Fs = S::Fs;

    /// Reload the vectors and offsets, apply `deleted_points`, and fold in the
    /// persisted deletion of each appended offset — a live point may have a
    /// deleted vector slot recorded only on disk.
    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.vectors
            .live_reload(fs, deleted_points, new_points, hw_counter)?;
        self.offsets
            .live_reload(fs, deleted_points, new_points, hw_counter)?;
        self.deleted.insert_all(deleted_points);
        self.deleted.reload_appended::<S>(fs, new_points)?;

        Ok(())
    }
}
