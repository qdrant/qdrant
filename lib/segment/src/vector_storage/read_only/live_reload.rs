use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::{CachedReadFs, UniversalRead, UniversalReadFs};
use futures::future::BoxFuture;

use super::VectorStorageReadEnum;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

impl<S: UniversalRead> LiveReload for VectorStorageReadEnum<S> {
    type File = S;

    fn live_preload<Fs: CachedReadFs<File = S>>(
        &self,
        fs: &Fs,
    ) -> OperationResult<Vec<BoxFuture<'static, ()>>> {
        match self {
            VectorStorageReadEnum::Dense(s) => s.live_preload(fs),
            VectorStorageReadEnum::DenseByte(s) => s.live_preload(fs),
            VectorStorageReadEnum::DenseHalf(s) => s.live_preload(fs),
            VectorStorageReadEnum::DenseGraphInline(s) => s.live_preload(fs),
            VectorStorageReadEnum::DenseGraphInlineByte(s) => s.live_preload(fs),
            VectorStorageReadEnum::DenseGraphInlineHalf(s) => s.live_preload(fs),
            VectorStorageReadEnum::DenseChunked(s) => s.live_preload(fs),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.live_preload(fs),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.live_preload(fs),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.live_preload(fs),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.live_preload(fs),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.live_preload(fs),
            VectorStorageReadEnum::DenseTurbo(s) => s.live_preload(fs),
            VectorStorageReadEnum::DenseTurboGraphInline(s) => s.live_preload(fs),
            VectorStorageReadEnum::DenseTurboChunked(s) => s.live_preload(fs),
            VectorStorageReadEnum::MultiDenseTurbo(s) => s.live_preload(fs),
            VectorStorageReadEnum::Sparse(s) => s.live_preload(fs),
        }
    }

    fn live_reload<Fs: UniversalReadFs<File = S>>(
        &mut self,
        fs: &Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
    ) -> OperationResult<()> {
        match self {
            VectorStorageReadEnum::Dense(s) => s.live_reload(fs, deleted_points, new_points),
            VectorStorageReadEnum::DenseByte(s) => s.live_reload(fs, deleted_points, new_points),
            VectorStorageReadEnum::DenseHalf(s) => s.live_reload(fs, deleted_points, new_points),
            VectorStorageReadEnum::DenseGraphInline(s) => {
                s.live_reload(fs, deleted_points, new_points)
            }
            VectorStorageReadEnum::DenseGraphInlineByte(s) => {
                s.live_reload(fs, deleted_points, new_points)
            }
            VectorStorageReadEnum::DenseGraphInlineHalf(s) => {
                s.live_reload(fs, deleted_points, new_points)
            }
            VectorStorageReadEnum::DenseChunked(s) => s.live_reload(fs, deleted_points, new_points),
            VectorStorageReadEnum::DenseChunkedByte(s) => {
                s.live_reload(fs, deleted_points, new_points)
            }
            VectorStorageReadEnum::DenseChunkedHalf(s) => {
                s.live_reload(fs, deleted_points, new_points)
            }
            VectorStorageReadEnum::MultiDenseChunked(s) => {
                s.live_reload(fs, deleted_points, new_points)
            }
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => {
                s.live_reload(fs, deleted_points, new_points)
            }
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => {
                s.live_reload(fs, deleted_points, new_points)
            }
            VectorStorageReadEnum::DenseTurbo(s) => s.live_reload(fs, deleted_points, new_points),
            VectorStorageReadEnum::DenseTurboGraphInline(s) => {
                s.live_reload(fs, deleted_points, new_points)
            }
            VectorStorageReadEnum::DenseTurboChunked(s) => {
                s.live_reload(fs, deleted_points, new_points)
            }
            VectorStorageReadEnum::MultiDenseTurbo(s) => {
                s.live_reload(fs, deleted_points, new_points)
            }
            VectorStorageReadEnum::Sparse(s) => s.live_reload(fs, deleted_points, new_points),
        }
    }
}
