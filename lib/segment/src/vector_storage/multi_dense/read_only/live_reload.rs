use common::counter::hardware_counter::HardwareCounterCell;
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

    /// Reload the vectors and offsets and apply `deleted_points`; appended points
    /// are served from the refreshed chunks, so `new_points` is unused.
    fn live_reload(
        &mut self,
        fs: &S::Fs,
        deleted_points: &[PointOffsetType],
        _new_points: &[PointOffsetType],
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.vectors.live_reload(fs, self.advice, self.populate)?;
        self.offsets.live_reload(fs, self.advice, self.populate)?;
        self.deleted.insert_all(deleted_points);

        Ok(())
    }
}
