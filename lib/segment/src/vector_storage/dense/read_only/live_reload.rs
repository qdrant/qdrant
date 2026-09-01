use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};
use futures::future::BoxFuture;

use super::ReadOnlyChunkedDenseVectorStorage;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;

impl<T: PrimitiveVectorElement, S: UniversalRead> LiveReload
    for ReadOnlyChunkedDenseVectorStorage<T, S>
{
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        let futs = self.vectors.live_preload(fs)?;
        self.deleted.live_preload(fs)?;
        Ok(futs)
    }

    /// Reload the chunked vectors, apply `deleted_points`, and fold in the
    /// persisted deletion of each appended offset — a live point may have a
    /// deleted vector slot recorded only on disk.
    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.vectors
            .live_reload(fs, deleted_points, new_points, hw_counter)?;
        self.deleted.insert_all(deleted_points);
        self.deleted.reload_appended::<S>(fs, new_points)?;

        Ok(())
    }
}
