use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use super::QuantizedVectorsRead;
use crate::common::live_reload::LiveReload;
use crate::common::operation_error::OperationResult;

impl<S: UniversalRead> LiveReload for QuantizedVectorsRead<S> {
    type Fs = S::Fs;

    /// Reload appended quantized vectors from disk (chunked layouts only).
    /// Quantized tracks no deletions, so `deleted_points` is unused; appended
    /// vectors come from disk, so `new_points` is unused too.
    fn live_reload(
        &mut self,
        fs: &S::Fs,
        _deleted_points: &SortedSlice<'_, PointOffsetType>,
        _new_points: &SortedSlice<'_, PointOffsetType>,
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage_impl.live_reload(fs)
    }
}
