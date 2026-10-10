use common::generic_consts::Random;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};
use futures::future::BoxFuture;

use super::ReadOnlyChunkedMultiTurboVectorStorage;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::{OperationError, OperationResult};

impl<S: UniversalRead> LiveReload for ReadOnlyChunkedMultiTurboVectorStorage<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        let mut futs = self.storage.live_preload(fs)?;
        futs.extend(self.offsets.live_preload(fs)?);
        self.deleted.live_preload(fs)?;
        Ok(futs)
    }

    /// Reload the vectors and offsets, apply `deleted_points`, and fold in the
    /// persisted deletion of each appended offset — a live point may have a
    /// deleted vector slot recorded only on disk.
    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        // Offsets first: they say how many inner records the new points use.
        self.offsets.live_reload(fs, deleted_points, new_points)?;
        // Records are appended in point order; the last range determines the end.
        if let Some(&last_point) = new_points.last() {
            let offset = self
                .offsets
                .get::<Random>(last_point as usize)
                .and_then(|offsets| offsets.first().copied())
                .ok_or_else(|| {
                    OperationError::service_error(format!(
                        "Offset of published point {last_point} is missing",
                    ))
                })?;
            self.storage
                .live_reload_to(fs, (offset.offset + offset.count) as usize)?;
        }
        self.deleted.insert_all(deleted_points);
        self.deleted.reload_appended::<S>(fs, new_points)?;

        Ok(())
    }
}
