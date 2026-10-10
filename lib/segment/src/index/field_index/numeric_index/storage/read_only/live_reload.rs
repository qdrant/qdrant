use blobstore::Blob;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};
use futures::future::BoxFuture;

use super::ReadOnlyNumericIndexInner;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;
use crate::index::field_index::numeric_index::Encodable;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::on_disk_point_to_values::StoredValue;

impl<T: Encodable + Numericable + StoredValue + Send + Sync + Default + 'static, S: UniversalRead>
    LiveReload for ReadOnlyNumericIndexInner<T, S>
where
    Vec<T>: Blob,
{
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        match self {
            Self::Appendable(index) => index.live_preload(fs),
            Self::Immutable(index) => index.live_preload(fs),
            Self::OnDisk(index) => index.live_preload(fs),
        }
    }

    fn apply_deletions(
        &mut self,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyNumericIndexInner::Appendable(index) => index.apply_deletions(deleted_points),
            ReadOnlyNumericIndexInner::Immutable(index) => index.apply_deletions(deleted_points),
            ReadOnlyNumericIndexInner::OnDisk(index) => index.apply_deletions(deleted_points),
        }
    }

    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        match self {
            ReadOnlyNumericIndexInner::Appendable(index) => {
                index.live_reload(fs, deleted_points, new_points)
            }
            ReadOnlyNumericIndexInner::Immutable(index) => {
                index.live_reload(fs, deleted_points, new_points)
            }
            ReadOnlyNumericIndexInner::OnDisk(index) => {
                index.live_reload(fs, deleted_points, new_points)
            }
        }
    }
}
