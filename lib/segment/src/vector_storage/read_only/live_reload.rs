use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::VectorStorageReadEnum;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

impl<S: UniversalRead> LiveReload for VectorStorageReadEnum<S> {
    type Fs = S::Fs;

    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            VectorStorageReadEnum::Dense(s) => {
                s.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            VectorStorageReadEnum::DenseByte(s) => {
                s.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            VectorStorageReadEnum::DenseHalf(s) => {
                s.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            VectorStorageReadEnum::DenseChunked(s) => {
                s.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            VectorStorageReadEnum::DenseChunkedByte(s) => {
                s.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            VectorStorageReadEnum::DenseChunkedHalf(s) => {
                s.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            VectorStorageReadEnum::MultiDenseChunked(s) => {
                s.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => {
                s.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => {
                s.live_reload(fs, deleted_points, new_points, hw_counter)
            }
            VectorStorageReadEnum::Sparse(s) => {
                s.live_reload(fs, deleted_points, new_points, hw_counter)
            }
        }
    }
}
