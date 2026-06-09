use common::counter::hardware_counter::HardwareCounterCell;
use common::sorted_slice::SortedSlice;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageRead;

impl<S: UniversalRead> LiveReload for QuantizedChunkedStorageRead<S> {
    type Fs = <S as UniversalRead>::Fs;

    /// Pick up quantized vectors a writer appended to the chunked backing.
    fn live_reload(
        &mut self,
        fs: &Self::Fs,
        deleted_points: &SortedSlice<'_, PointOffsetType>,
        new_points: &SortedSlice<'_, PointOffsetType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.data
            .live_reload(fs, deleted_points, new_points, hw_counter)
    }
}
